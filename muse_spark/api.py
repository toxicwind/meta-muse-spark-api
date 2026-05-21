from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable, Optional, Union

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

from .client import CHAT_TEMPLATE_NAME, DEFAULT_STATE_PATH, HOME_TEMPLATE_NAME
from .config import ApiSettings
from .errors import MissingAuthError, ProviderProtocolError, ProviderTransportError, ReauthRequiredError
from .idempotency import request_hash, resolve_idempotency_key, sha256_text
from .ledger import RequestLedger, default_ledger_path
from .locks import ConversationLockRegistry
from .logging_utils import get_logger
from .openai_compat import (
    build_chat_completion_chunk,
    build_chat_completion_response,
    build_error_response,
    build_models_response,
    encode_sse_data,
    encode_sse_done,
)
from .prompt_compiler import build_stateful_turn_plan
from .provider import (
    MuseProviderRequest,
    generate_from_state_async,
    load_provider_auth,
    resolve_api_conversation,
    stream_from_state_async,
)
from .schemas import ChatCompletionsRequest

ProviderGenerateFn = Callable[..., Awaitable[Any]]
ProviderStreamFn = Callable[..., AsyncIterator[str]]
CompilerFn = Callable[..., Any]
LoadAuthFn = Callable[..., dict[str, str]]

UNSAFE_RETRY_PHASES = {"bootstrap_turn_sent", "user_turn_sent", "streaming", "failed_after_send"}


def _normalize_stop(stop: Optional[Union[str, list[str]]]) -> Optional[list[str]]:
    if isinstance(stop, list):
        return stop
    if isinstance(stop, str):
        return [stop]
    return None


def _chunk_text(text: str, chunk_size: int) -> list[str]:
    if chunk_size <= 0:
        chunk_size = 120
    return [text[index : index + chunk_size] for index in range(0, len(text), chunk_size)] or [""]


def _body_payload(body: ChatCompletionsRequest) -> dict[str, Any]:
    return body.model_dump(mode="json")


def _json_response_with_meta(payload: dict[str, Any], *, idempotency_key: str, cache: str) -> JSONResponse:
    return JSONResponse(
        content=payload,
        headers={
            "X-Muse-Spark-Idempotency-Key": idempotency_key,
            "X-Muse-Spark-Cache": cache,
        },
    )


def _cached_response(record: dict[str, Any], idempotency_key: str) -> Optional[JSONResponse]:
    if record.get("phase") != "completed" or not record.get("response_json"):
        return None
    return _json_response_with_meta(json.loads(record["response_json"]), idempotency_key=idempotency_key, cache="HIT")


def _cached_streaming_response(
    record: dict[str, Any],
    *,
    idempotency_key: str,
    model: str,
    chunk_size: int,
) -> Optional[StreamingResponse]:
    if record.get("phase") != "completed" or not record.get("response_json"):
        return None
    response_payload = json.loads(record["response_json"])
    response_text = record.get("response_text") or response_payload.get("choices", [{}])[0].get("message", {}).get("content", "")
    response_id = response_payload.get("id") or f"chatcmpl-{uuid.uuid4()}"
    conversation_id = response_payload.get("conversation_id") or record.get("client_conversation_id") or ""
    bootstrap_response = response_payload.get("bootstrap_response")

    async def replay() -> AsyncIterator[bytes]:
        yield encode_sse_data(
            build_chat_completion_chunk(
                model=model,
                response_id=response_id,
                delta={"role": "assistant"},
                conversation_id=conversation_id,
                bootstrap_response=bootstrap_response,
            )
        )
        for chunk in _chunk_text(response_text, chunk_size):
            if chunk:
                yield encode_sse_data(
                    build_chat_completion_chunk(
                        model=model,
                        response_id=response_id,
                        delta={"content": chunk},
                        conversation_id=conversation_id,
                        bootstrap_response=bootstrap_response,
                    )
                )
        yield encode_sse_data(
            build_chat_completion_chunk(
                model=model,
                response_id=response_id,
                delta={},
                finish_reason="stop",
                conversation_id=conversation_id,
                bootstrap_response=bootstrap_response,
            )
        )
        yield encode_sse_done()

    return StreamingResponse(
        replay(),
        media_type="text/event-stream",
        headers={
            "X-Muse-Spark-Idempotency-Key": idempotency_key,
            "X-Muse-Spark-Cache": "HIT",
        },
    )


def run_api_server(*, host: str = "127.0.0.1", port: int = 8000, state_path: Union[Path, str] = DEFAULT_STATE_PATH) -> None:
    import uvicorn

    print("Muse Spark API")
    print(f"  URL: http://{host}:{port}")
    print(f"  Playground: http://{host}:{port}/test")
    print(f"  State: {state_path}")
    print(f"  Ledger: {default_ledger_path(state_path)}")
    print("  Endpoints: /healthz /readyz /test /v1/models /v1/chat/completions")
    app = create_app(state_path=state_path)
    uvicorn.run(app, host=host, port=port)


async def _stream_chat_completion(
    *,
    chunk_iter: AsyncIterator[str],
    model: str,
    response_id: str,
    conversation_id: str,
    chunk_size: int,
    bootstrap_response: Optional[str] = None,
    on_complete: Optional[Callable[[str], Awaitable[None]]] = None,
    on_error: Optional[Callable[[BaseException], Awaitable[None]]] = None,
) -> AsyncIterator[bytes]:
    accumulated: list[str] = []
    completed = False
    error_reported = False
    try:
        yield encode_sse_data(
            build_chat_completion_chunk(
                model=model,
                response_id=response_id,
                delta={"role": "assistant"},
                conversation_id=conversation_id,
                bootstrap_response=bootstrap_response,
            )
        )
        async for provider_chunk in chunk_iter:
            accumulated.append(provider_chunk)
            for chunk in _chunk_text(provider_chunk, chunk_size):
                if chunk:
                    yield encode_sse_data(
                        build_chat_completion_chunk(
                            model=model,
                            response_id=response_id,
                            delta={"content": chunk},
                            conversation_id=conversation_id,
                            bootstrap_response=bootstrap_response,
                        )
                    )
        yield encode_sse_data(
            build_chat_completion_chunk(
                model=model,
                response_id=response_id,
                delta={},
                finish_reason="stop",
                conversation_id=conversation_id,
                bootstrap_response=bootstrap_response,
            )
        )
        yield encode_sse_done()
        if on_complete:
            await on_complete("".join(accumulated))
        completed = True
    except BaseException as exc:
        if on_error:
            error_reported = True
            await on_error(exc)
        raise
    finally:
        if not completed and not error_reported and on_error:
            await on_error(asyncio.CancelledError("stream did not complete"))


def _playground_html() -> str:
    return """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Muse Spark API Playground</title>
  <style>
    :root { color-scheme: dark; --bg:#07080b; --panel:#11131a; --soft:#1a1d27; --line:#2a2f3b; --text:#f5f7fb; --muted:#99a2b3; --accent:#9ad7ff; --good:#8ff0b0; --bad:#ff8f9f; }
    * { box-sizing: border-box; }
    body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, \"Segoe UI\", sans-serif; background: radial-gradient(circle at top left, #172033, var(--bg) 42%); color:var(--text); }
    main { max-width: 1180px; margin: 0 auto; padding: 28px; }
    h1 { margin: 0 0 6px; letter-spacing: -0.04em; }
    p { color: var(--muted); margin-top: 0; }
    .grid { display:grid; grid-template-columns: minmax(320px, 0.9fr) minmax(360px, 1.1fr); gap:18px; align-items:start; }
    .card { background: color-mix(in srgb, var(--panel) 92%, transparent); border:1px solid var(--line); border-radius:22px; padding:18px; box-shadow: 0 20px 80px rgba(0,0,0,.28); }
    label { display:block; color:var(--muted); font-size:12px; font-weight:700; letter-spacing:.08em; text-transform:uppercase; margin:12px 0 7px; }
    input, textarea, select { width:100%; border:1px solid var(--line); background:var(--soft); color:var(--text); border-radius:14px; padding:11px 12px; font:inherit; outline:none; }
    textarea { min-height: 140px; resize: vertical; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; line-height: 1.45; }
    input:focus, textarea:focus, select:focus { border-color: color-mix(in srgb, var(--accent), white 18%); box-shadow: 0 0 0 3px rgba(154,215,255,.13); }
    .row { display:grid; grid-template-columns: 1fr 1fr; gap:10px; }
    .actions { display:flex; flex-wrap:wrap; gap:10px; margin-top:14px; }
    button { border:0; border-radius:14px; padding:11px 14px; font-weight:800; color:#051018; background:var(--accent); cursor:pointer; transition: transform .16s ease-out, filter .16s ease-out; }
    button:hover { transform: translateY(-1px); filter: brightness(1.06); }
    button.secondary { background:#2a3040; color:var(--text); }
    button.danger { background:#3b1f28; color:#ffd8df; }
    .status { display:flex; gap:10px; flex-wrap:wrap; margin:12px 0; color:var(--muted); font-size:13px; }
    .pill { border:1px solid var(--line); background:var(--soft); border-radius:999px; padding:5px 9px; }
    pre { white-space: pre-wrap; word-break: break-word; background:#050609; border:1px solid var(--line); border-radius:16px; padding:14px; min-height:180px; max-height:520px; overflow:auto; }
    .ok { color: var(--good); } .err { color: var(--bad); }
    @media (max-width: 860px) { main { padding: 18px; } .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
<main>
  <h1>Muse Spark API Playground</h1>
  <p>Fast local loop for prompts, streaming, conversation IDs, Idempotency-Key retries, and raw response inspection.</p>
  <div class=\"grid\">
    <section class=\"card\">
      <div class=\"row\">
        <div><label>Endpoint</label><input id=\"endpoint\" value=\"/v1/chat/completions\" /></div>
        <div><label>Model</label><input id=\"model\" value=\"meta/muse-spark\" /></div>
      </div>
      <label>Conversation ID</label><input id=\"conversation\" placeholder=\"blank = new conversation\" />
      <label>Idempotency-Key</label><input id=\"idem\" />
      <div class=\"row\">
        <div><label>Mode</label><select id=\"stream\"><option value=\"false\">non-streaming</option><option value=\"true\">streaming</option></select></div>
        <div><label>Bootstrap debug</label><select id=\"bootstrap\"><option value=\"false\">hide</option><option value=\"true\">include</option></select></div>
      </div>
      <label>Prompt</label><textarea id=\"prompt\">Reply with exactly: pong</textarea>
      <div class=\"actions\">
        <button id=\"send\">Send</button>
        <button class=\"secondary\" id=\"repeat\">Repeat same key</button>
        <button class=\"secondary\" id=\"newkey\">New key</button>
        <button class=\"danger\" id=\"clear\">Clear</button>
      </div>
    </section>
    <section class=\"card\">
      <div class=\"status\">
        <span class=\"pill\">status: <b id=\"status\">idle</b></span>
        <span class=\"pill\">time: <b id=\"timing\">-</b></span>
        <span class=\"pill\">cache: <b id=\"cache\">-</b></span>
      </div>
      <label>Assistant text</label><pre id=\"text\"></pre>
      <label>Raw response / SSE</label><pre id=\"raw\"></pre>
    </section>
  </div>
</main>
<script>
const $ = (id) => document.getElementById(id);
const newKey = () => `muse-play-${Date.now()}-${Math.random().toString(16).slice(2)}`;
$('idem').value = newKey();
$('newkey').onclick = () => $('idem').value = newKey();
$('clear').onclick = () => { $('text').textContent=''; $('raw').textContent=''; $('status').textContent='idle'; $('timing').textContent='-'; $('cache').textContent='-'; };
$('repeat').onclick = () => send();
$('send').onclick = () => send();
function payload() {
  const body = { model: $('model').value, stream: $('stream').value === 'true', include_bootstrap_response: $('bootstrap').value === 'true', messages: [{ role: 'user', content: $('prompt').value }] };
  if ($('conversation').value.trim()) body.conversation_id = $('conversation').value.trim();
  return body;
}
async function send() {
  $('status').textContent = 'running'; $('status').className = ''; $('text').textContent=''; $('raw').textContent=''; $('cache').textContent='-';
  const started = performance.now();
  const res = await fetch($('endpoint').value, { method:'POST', headers:{ 'content-type':'application/json', 'Idempotency-Key': $('idem').value }, body: JSON.stringify(payload()) });
  $('status').textContent = `${res.status} ${res.statusText}`;
  $('cache').textContent = res.headers.get('x-muse-spark-cache') || '-';
  const ctype = res.headers.get('content-type') || '';
  if (ctype.includes('text/event-stream')) {
    const reader = res.body.getReader(); const decoder = new TextDecoder(); let raw = ''; let text = '';
    while (true) {
      const {done, value} = await reader.read(); if (done) break;
      const chunk = decoder.decode(value, {stream:true}); raw += chunk; $('raw').textContent = raw;
      for (const line of chunk.split('\n')) {
        if (!line.startsWith('data: ') || line.includes('[DONE]')) continue;
        try { const evt = JSON.parse(line.slice(6)); const delta = evt.choices?.[0]?.delta?.content || ''; text += delta; $('text').textContent = text; if (evt.conversation_id) $('conversation').value = evt.conversation_id; } catch {}
      }
    }
  } else {
    const data = await res.json(); $('raw').textContent = JSON.stringify(data, null, 2);
    const message = data.choices?.[0]?.message?.content || data.error?.message || '';
    $('text').textContent = message; if (data.conversation_id) $('conversation').value = data.conversation_id;
  }
  $('timing').textContent = `${Math.round(performance.now() - started)}ms`;
}
</script>
</body>
</html>"""


def create_app(
    *,
    provider_generate_fn: ProviderGenerateFn = generate_from_state_async,
    provider_stream_fn: ProviderStreamFn = stream_from_state_async,
    compiler_fn: CompilerFn = build_stateful_turn_plan,
    load_auth_fn: LoadAuthFn = load_provider_auth,
    state_path: Union[Path, str] = DEFAULT_STATE_PATH,
    settings: Optional[ApiSettings] = None,
) -> FastAPI:
    settings = settings or ApiSettings.from_env()
    logger = get_logger("muse_spark.api", settings.log_level)
    app = FastAPI(title="Muse Spark OpenAI-Compatible API")
    app.state.settings = settings
    app.state.ledger = RequestLedger(default_ledger_path(state_path))
    app.state.locks = ConversationLockRegistry()

    def error_json(status_code: int, code: str, message: str, error_type: str = "invalid_request_error") -> JSONResponse:
        return JSONResponse(
            status_code=status_code,
            content=build_error_response(code=code, message=message, error_type=error_type),
        )

    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        try:
            response = await call_next(request)
        except Exception:
            logger.exception("request_failed method=%s path=%s", request.method, request.url.path)
            raise
        logger.info("request method=%s path=%s status=%s", request.method, request.url.path, response.status_code)
        return response

    @app.exception_handler(MissingAuthError)
    async def handle_missing_auth(request, exc: MissingAuthError):
        return error_json(503, "missing_auth", str(exc), error_type="auth_error")

    @app.exception_handler(ReauthRequiredError)
    async def handle_reauth_required(request, exc: ReauthRequiredError):
        return error_json(401, "reauth_required", str(exc), error_type="auth_error")

    @app.exception_handler(ProviderTransportError)
    async def handle_transport_error(request, exc: ProviderTransportError):
        return error_json(502, "provider_transport_error", str(exc), error_type="provider_error")

    @app.exception_handler(ProviderProtocolError)
    async def handle_protocol_error(request, exc: ProviderProtocolError):
        return error_json(502, "provider_protocol_error", str(exc), error_type="provider_error")

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"ok": True}

    @app.get("/readyz")
    async def readyz() -> dict[str, Any]:
        load_auth_fn(state_path)
        return {"ok": True, "model": settings.model_name}

    @app.get("/test", response_class=HTMLResponse)
    async def playground() -> HTMLResponse:
        return HTMLResponse(_playground_html())

    @app.get("/v1/models")
    async def list_models() -> dict[str, Any]:
        return build_models_response([settings.model_name])

    @app.get("/v1/muse-spark/requests/{idempotency_key}")
    async def inspect_request(idempotency_key: str):
        record = app.state.ledger.get_by_idempotency_key(idempotency_key)
        if not record:
            return error_json(404, "request_not_found", "No request exists for that idempotency key.")
        safe_keys = {
            "id",
            "idempotency_key",
            "client_conversation_id",
            "turn_index",
            "phase",
            "response_hash",
            "error_type",
            "created_at",
            "updated_at",
        }
        return {key: record.get(key) for key in safe_keys}

    @app.post("/v1/chat/completions")
    async def chat_completions(body: ChatCompletionsRequest, request: Request):
        if body.model != settings.model_name:
            return error_json(400, "invalid_model", f"Unsupported model: {body.model}")

        body_payload = _body_payload(body)
        header_idempotency_key = request.headers.get("idempotency-key")
        # Explicit keys are stable. Fallback keys are deterministic when the client
        # supplies a conversation_id; no-conversation requests intentionally get a
        # fresh local key so normal "new chat" calls do not collapse into a cache hit.
        if header_idempotency_key or body.conversation_id:
            idempotency_key = resolve_idempotency_key(header_idempotency_key, body_payload)
        else:
            idempotency_key = f"muse-auto-new-{uuid.uuid4()}"
        body_hash = request_hash(body_payload)
        lock_key = "explicit-idempotency" if header_idempotency_key else body.conversation_id or idempotency_key
        ledger: RequestLedger = app.state.ledger
        locks: ConversationLockRegistry = app.state.locks

        async with locks.hold(lock_key):
            existing = ledger.get_by_idempotency_key(idempotency_key)
            if existing:
                if existing["request_hash"] != body_hash:
                    return error_json(
                        409,
                        "idempotency_key_conflict",
                        "This Idempotency-Key was already used with a different request body.",
                    )
                if body.stream:
                    cached_stream = _cached_streaming_response(
                        existing,
                        idempotency_key=idempotency_key,
                        model=settings.model_name,
                        chunk_size=settings.stream_chunk_size,
                    )
                    if cached_stream:
                        return cached_stream
                cached = _cached_response(existing, idempotency_key)
                if cached:
                    return cached
                if existing.get("phase") in UNSAFE_RETRY_PHASES:
                    return error_json(
                        409,
                        "request_already_sent",
                        "This request may already have been sent upstream; refusing to resend it.",
                    )

            compiled = compiler_fn([message.model_dump() for message in body.messages])
            resolved = resolve_api_conversation(state_path=state_path, client_conversation_id=body.conversation_id)
            if not existing:
                active = ledger.get_active_for_conversation(
                    resolved.client_conversation_id,
                    exclude_idempotency_key=idempotency_key,
                )
                if active:
                    return error_json(
                        409,
                        "conversation_busy",
                        "This conversation already has an in-flight upstream request; wait for it to finish before sending another turn.",
                    )
            if not existing:
                ledger.create_pending(
                    request_id=f"muse-req-{uuid.uuid4()}",
                    idempotency_key=idempotency_key,
                    request_hash=body_hash,
                    client_conversation_id=resolved.client_conversation_id,
                    meta_conversation_id=resolved.meta_conversation_id,
                    turn_index=ledger.next_turn_index(resolved.client_conversation_id),
                )
            ledger.mark_phase(
                idempotency_key,
                "conversation_resolved",
                client_conversation_id=resolved.client_conversation_id,
                meta_conversation_id=resolved.meta_conversation_id,
            )

            provider_request = MuseProviderRequest(
                prompt=compiled.user_prompt,
                conversation_id=resolved.meta_conversation_id,
                template_name=CHAT_TEMPLATE_NAME,
                user_prompt=compiled.user_prompt,
            )
            bootstrap_response_text: Optional[str] = None
            try:
                if not body.conversation_id and compiled.bootstrap_prompt:
                    ledger.mark_phase(idempotency_key, "bootstrap_turn_sent")
                    bootstrap_response = await provider_generate_fn(
                        MuseProviderRequest(
                            prompt=compiled.bootstrap_prompt,
                            conversation_id=resolved.meta_conversation_id,
                            template_name=HOME_TEMPLATE_NAME,
                        ),
                        state_path=state_path,
                    )
                    ledger.mark_phase(idempotency_key, "bootstrap_completed")
                    if body.include_bootstrap_response:
                        bootstrap_response_text = bootstrap_response.text

                if body.stream:
                    response_id = f"chatcmpl-{uuid.uuid4()}"
                    ledger.mark_phase(idempotency_key, "user_turn_sent")
                    ledger.mark_phase(idempotency_key, "streaming")
                    chunk_iter = provider_stream_fn(provider_request, state_path=state_path)

                    async def on_complete(text: str) -> None:
                        response_payload = build_chat_completion_response(
                            content=text,
                            model=settings.model_name,
                            response_id=response_id,
                            conversation_id=resolved.client_conversation_id,
                            bootstrap_response=bootstrap_response_text,
                        )
                        ledger.complete(
                            idempotency_key,
                            response_json=response_payload,
                            response_text=text,
                            response_hash=sha256_text(text),
                        )

                    async def on_error(exc: BaseException) -> None:
                        current = ledger.get_by_idempotency_key(idempotency_key)
                        phase = "failed_after_send" if current and current.get("phase") in UNSAFE_RETRY_PHASES else "failed"
                        ledger.fail(idempotency_key, error_type=exc.__class__.__name__, error_message=str(exc), phase=phase)

                    return StreamingResponse(
                        _stream_chat_completion(
                            chunk_iter=chunk_iter,
                            model=settings.model_name,
                            response_id=response_id,
                            conversation_id=resolved.client_conversation_id,
                            chunk_size=settings.stream_chunk_size,
                            bootstrap_response=bootstrap_response_text,
                            on_complete=on_complete,
                            on_error=on_error,
                        ),
                        media_type="text/event-stream",
                        headers={
                            "X-Muse-Spark-Idempotency-Key": idempotency_key,
                            "X-Muse-Spark-Cache": "MISS",
                        },
                    )

                ledger.mark_phase(idempotency_key, "user_turn_sent")
                provider_response = await provider_generate_fn(provider_request, state_path=state_path)
                response_payload = build_chat_completion_response(
                    content=provider_response.text,
                    model=settings.model_name,
                    conversation_id=resolved.client_conversation_id,
                    bootstrap_response=bootstrap_response_text,
                )
                ledger.complete(
                    idempotency_key,
                    response_json=response_payload,
                    response_text=provider_response.text,
                    response_hash=sha256_text(provider_response.text),
                )
                return _json_response_with_meta(response_payload, idempotency_key=idempotency_key, cache="MISS")
            except Exception as exc:
                current = ledger.get_by_idempotency_key(idempotency_key)
                phase = "failed_after_send" if current and current.get("phase") in UNSAFE_RETRY_PHASES else "failed"
                ledger.fail(idempotency_key, error_type=exc.__class__.__name__, error_message=str(exc), phase=phase)
                raise

    return app
