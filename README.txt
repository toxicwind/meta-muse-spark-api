Muse Spark CLI + Local OpenAI-Compatible API

What this is
- A local CLI and FastAPI wrapper for Meta AI Muse Spark.
- It stores auth locally so day to day usage is just `new`, `chat`, `use`, `current`, and `list`.
- It opens the captured `gateway.meta.ai` WebSocket.
- It uses the home prompt template for new conversations.
- It uses the chat prompt template for follow ups and resumed conversations.
- It exposes a local OpenAI-compatible `/v1/chat/completions` endpoint for your own agents and tools.
- It includes a local browser playground at `/test` for fast prompt/API iteration.
- It supports `Idempotency-Key`, a SQLite request ledger, and per-conversation/request locking to avoid accidental duplicate upstream sends.

What it is not
- Not a public API client.
- Not a real login flow yet.
- Still reverse engineered, so Meta can break it whenever they feel like being annoying.
- Not a multi-user hosted service. This is local-first tooling.

Project layout
- `muse_spark/client.py`: protobuf patcher, session store, HTTP helpers, CLI, protocol transport
- `muse_spark/provider.py`: provider adapter over the transport layer
- `muse_spark/prompt_compiler.py`: OpenAI-style messages -> Muse prompt compiler
- `muse_spark/api.py`: FastAPI app factory, HTTP routes, playground, idempotent request handling
- `muse_spark/idempotency.py`: request hashes and idempotency key resolution
- `muse_spark/ledger.py`: SQLite request ledger
- `muse_spark/locks.py`: local lock registry for duplicate/conversation safety
- `muse_spark/openai_compat.py`: OpenAI-style response and SSE helpers
- `muse_spark/schemas.py`: request schemas
- `muse_spark/config.py`: env-driven API settings
- `muse_spark/logging_utils.py`: logger setup
- `tests/`: regression and API tests
- `pyproject.toml`: project metadata and runtime dependencies
- `requirements.txt`: minimal compatibility fallback dependency list
- `.env.example`: optional environment variable examples

Setup with uv (recommended)
1. `cd /Users/kamell/Documents/Projects/labs/muse-spark`
2. `uv sync`
3. Run commands with `uv run ...`

Examples:
- `uv run muse-spark --help`
- `uv run python -m unittest discover -s tests -v`

Fallback setup without uv
1. `cd /Users/kamell/Documents/Projects/labs/muse-spark`
2. `python3 -m venv .venv`
3. `source .venv/bin/activate`
4. `pip3 install -r requirements.txt`

One time auth setup
You still need both values from Charles for now:
- Cookie header from Meta requests
- `ecto1:...` authorization token from the WebSocket query string

Store them once:
`uv run muse-spark auth set --cookie 'datr=...; ecto_1_sess=...; ...' --authorization 'ecto1:...'`

CLI usage
Start a new conversation:
`uv run muse-spark new "new convo probe 1"`

Send to the current conversation:
`uv run muse-spark chat "follow up probe 2"`

Switch current conversation:
`uv run muse-spark use b08385a6-5a53-4f14-966e-347f28088454`

Show current conversation:
`uv run muse-spark current`

List known conversations:
`uv run muse-spark list`

Debug a generated frame:
`uv run muse-spark debug-frame "hello world" --conversation-id 0408aded-55f9-4748-bcdf-dfe5f13b337b --template home`

Run the local API
Start the server:
`uv run muse-spark serve --host 127.0.0.1 --port 8000`

Open the interactive testing environment:
`http://127.0.0.1:8000/test`

Health check:
`curl http://127.0.0.1:8000/healthz`

Readiness/auth check:
`curl http://127.0.0.1:8000/readyz`

List models:
`curl http://127.0.0.1:8000/v1/models`

Non-streaming completion with idempotency:
```bash
curl http://127.0.0.1:8000/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: muse-demo-001' \
  -d '{
    "model":"meta/muse-spark",
    "conversation_id":"demo-thread-1",
    "messages":[{"role":"user","content":"Reply with exactly: pong"}]
  }'
```

Repeat the exact same command to verify a cached safe retry. The response includes:
- `X-Muse-Spark-Idempotency-Key`
- `X-Muse-Spark-Cache: MISS` on first completion
- `X-Muse-Spark-Cache: HIT` on duplicate completed requests

Conflict test with same key, changed body:
```bash
curl http://127.0.0.1:8000/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: muse-demo-001' \
  -d '{
    "model":"meta/muse-spark",
    "conversation_id":"demo-thread-1",
    "messages":[{"role":"user","content":"This body is different"}]
  }'
```
Expected result: HTTP 409 with `idempotency_key_conflict`.

Streaming completion:
```bash
curl -N http://127.0.0.1:8000/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: muse-stream-001' \
  -d '{
    "model":"meta/muse-spark",
    "conversation_id":"demo-thread-1",
    "stream":true,
    "messages":[{"role":"user","content":"Reply with exactly: pong"}]
  }'
```

Inspect request ledger state. This returns only safe metadata, not provider response text, request hashes, Meta conversation IDs, auth, or raw payloads:
```bash
curl http://127.0.0.1:8000/v1/muse-spark/requests/muse-demo-001
```

Environment variable fallback
The CLI/API prefer values stored in `~/.muse_spark/state.json`, but they can also fall back to environment variables:
- `MUSE_SPARK_COOKIE_HEADER`
- `MUSE_SPARK_COOKIE`
- `MUSE_SPARK_AUTHORIZATION`
- `MUSE_SPARK_MODE`

API settings from env
- `MUSE_SPARK_MODEL_NAME` default: `meta/muse-spark`
- `MUSE_SPARK_LOG_LEVEL` default: `INFO`
- `MUSE_SPARK_STREAM_CHUNK_SIZE` default: `120`
- `MUSE_SPARK_DEBUG_FRAME_DUMPS` default: `0`

Local state and ledger
- Auth and conversation mappings live in `~/.muse_spark/state.json`.
- The request ledger lives beside it at `~/.muse_spark/ledger.sqlite3` by default.
- Automated tests use temp state/ledger paths and do not make real Meta calls.

Retry/idempotency behavior
- Client-supplied `Idempotency-Key` is preferred.
- If absent and `conversation_id` is supplied, the API derives a deterministic fallback key from conversation id, latest user turn, and canonical request body hash.
- If absent and no `conversation_id` is supplied, the API uses a fresh local key so normal new-chat calls do not accidentally collapse into a cache hit.
- Same key + same body returns the cached completed response after success.
- Streaming safe retries replay cached text as OpenAI-style SSE with `X-Muse-Spark-Cache: HIT`.
- Same key + different body returns HTTP 409.
- A new request for a conversation with an active upstream-send phase returns HTTP 409 `conversation_busy` instead of racing another turn into Meta.
- Once a request reaches an upstream-send phase, the API refuses unsafe duplicate resend instead of gambling with duplicate Meta turns.

Notes
- The API is stateful: each OpenAI conversation_id maps to a persistent Meta conversation under the hood.
- New conversations use a hidden bootstrap turn, then the real user turn.
- Follow ups reuse the same conversation_id and send only the latest user message.
- Streaming uses incremental Meta response deltas wrapped as OpenAI-style SSE chunks.
- The stateless transcript compiler is removed; stateful XML turn planning is the only chat path.
- The CLI stores known conversations locally in `~/.muse_spark/state.json`.
- If auth expires, rerun `auth set` with fresh Charles values.
- `response_format={"type":"json_object"}` is best-effort prompting, not hard schema enforcement.
- `max_tokens` and `stop` are advisory prompt guidance for now, not provider-native controls.
- A proper login command comes later.
