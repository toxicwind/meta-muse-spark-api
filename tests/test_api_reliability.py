import asyncio
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fastapi.testclient import TestClient

from muse_spark.api import create_app, _stream_chat_completion
from muse_spark.ledger import RequestLedger, default_ledger_path
from muse_spark.provider import MuseProviderResponse


class ApiReliabilityTests(unittest.TestCase):
    def test_duplicate_idempotency_key_returns_cached_response_without_provider_second_call(self):
        calls = []

        async def fake_provider(request, state_path=None):
            calls.append(request.prompt)
            return MuseProviderResponse(
                text=f"reply #{len(calls)}",
                conversation_id=request.conversation_id,
                template_name=request.template_name,
            )

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            app = create_app(provider_generate_fn=fake_provider, state_path=state_path)
            client = TestClient(app)
            body = {
                "model": "meta/muse-spark",
                "conversation_id": "thread-1",
                "messages": [{"role": "user", "content": "Reply with exactly pong."}],
            }

            first = client.post("/v1/chat/completions", headers={"Idempotency-Key": "idem-1"}, json=body)
            second = client.post("/v1/chat/completions", headers={"Idempotency-Key": "idem-1"}, json=body)

            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)
            self.assertEqual(len(calls), 1)
            self.assertEqual(second.headers["x-muse-spark-cache"], "HIT")
            self.assertEqual(second.json(), first.json())

    def test_same_idempotency_key_with_different_body_returns_conflict(self):
        async def fake_provider(request, state_path=None):
            return MuseProviderResponse(
                text="first reply",
                conversation_id=request.conversation_id,
                template_name=request.template_name,
            )

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            app = create_app(provider_generate_fn=fake_provider, state_path=state_path)
            client = TestClient(app)

            first = client.post(
                "/v1/chat/completions",
                headers={"Idempotency-Key": "idem-conflict"},
                json={
                    "model": "meta/muse-spark",
                    "conversation_id": "thread-1",
                    "messages": [{"role": "user", "content": "First body"}],
                },
            )
            second = client.post(
                "/v1/chat/completions",
                headers={"Idempotency-Key": "idem-conflict"},
                json={
                    "model": "meta/muse-spark",
                    "conversation_id": "thread-1",
                    "messages": [{"role": "user", "content": "Changed body"}],
                },
            )

            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 409)
            self.assertEqual(second.json()["error"]["code"], "idempotency_key_conflict")

    def test_request_ledger_records_completed_phase_and_cached_response(self):
        async def fake_provider(request, state_path=None):
            return MuseProviderResponse(
                text="ledger reply",
                conversation_id=request.conversation_id,
                template_name=request.template_name,
            )

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            app = create_app(provider_generate_fn=fake_provider, state_path=state_path)
            client = TestClient(app)

            response = client.post(
                "/v1/chat/completions",
                headers={"Idempotency-Key": "idem-ledger"},
                json={
                    "model": "meta/muse-spark",
                    "conversation_id": "thread-1",
                    "messages": [{"role": "user", "content": "Track this"}],
                },
            )

            self.assertEqual(response.status_code, 200)
            record = RequestLedger(default_ledger_path(state_path)).get_by_idempotency_key("idem-ledger")
            self.assertIsNotNone(record)
            self.assertEqual(record["phase"], "completed")
            self.assertEqual(record["client_conversation_id"], "thread-1")
            self.assertIn("ledger reply", record["response_json"])

    def test_concurrent_duplicate_idempotency_key_only_invokes_provider_once(self):
        calls = []

        async def fake_provider(request, state_path=None):
            calls.append(request.prompt)
            await asyncio.sleep(0.05)
            return MuseProviderResponse(
                text="single upstream result",
                conversation_id=request.conversation_id,
                template_name=request.template_name,
            )

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            app = create_app(provider_generate_fn=fake_provider, state_path=state_path)
            client = TestClient(app)
            body = {
                "model": "meta/muse-spark",
                "conversation_id": "thread-1",
                "messages": [{"role": "user", "content": "Concurrent duplicate"}],
            }

            def post_once():
                return client.post("/v1/chat/completions", headers={"Idempotency-Key": "idem-race"}, json=body)

            with ThreadPoolExecutor(max_workers=2) as pool:
                first, second = list(pool.map(lambda _: post_once(), range(2)))

            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)
            self.assertEqual(len(calls), 1)
            self.assertEqual(first.json(), second.json())

    def test_concurrent_same_idempotency_key_different_body_returns_conflict_not_500(self):
        calls = []

        async def fake_provider(request, state_path=None):
            calls.append(request.prompt)
            await asyncio.sleep(0.05)
            return MuseProviderResponse(
                text="first wins",
                conversation_id=request.conversation_id,
                template_name=request.template_name,
            )

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            app = create_app(provider_generate_fn=fake_provider, state_path=state_path)
            client = TestClient(app, raise_server_exceptions=False)
            bodies = [
                {
                    "model": "meta/muse-spark",
                    "conversation_id": "thread-a",
                    "messages": [{"role": "user", "content": "Body A"}],
                },
                {
                    "model": "meta/muse-spark",
                    "conversation_id": "thread-b",
                    "messages": [{"role": "user", "content": "Body B"}],
                },
            ]

            def post_body(body):
                return client.post("/v1/chat/completions", headers={"Idempotency-Key": "same-key-race"}, json=body)

            with ThreadPoolExecutor(max_workers=2) as pool:
                responses = list(pool.map(post_body, bodies))

            statuses = sorted(response.status_code for response in responses)
            self.assertEqual(statuses, [200, 409])
            conflict = next(response for response in responses if response.status_code == 409)
            self.assertEqual(conflict.json()["error"]["code"], "idempotency_key_conflict")
            self.assertEqual(len(calls), 1)

    def test_conversation_busy_blocks_new_request_before_resending_upstream(self):
        calls = []

        async def fake_provider(request, state_path=None):
            calls.append(request.prompt)
            return MuseProviderResponse(
                text="should not run",
                conversation_id=request.conversation_id,
                template_name=request.template_name,
            )

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            app = create_app(provider_generate_fn=fake_provider, state_path=state_path)
            ledger = RequestLedger(default_ledger_path(state_path))
            ledger.create_pending(
                request_id="muse-req-active",
                idempotency_key="active-request",
                request_hash="hash",
                client_conversation_id="thread-busy",
                meta_conversation_id="meta-thread-busy",
                turn_index=1,
            )
            ledger.mark_phase("active-request", "streaming")
            client = TestClient(app)

            response = client.post(
                "/v1/chat/completions",
                headers={"Idempotency-Key": "new-request"},
                json={
                    "model": "meta/muse-spark",
                    "conversation_id": "thread-busy",
                    "messages": [{"role": "user", "content": "New turn while old turn streams"}],
                },
            )

            self.assertEqual(response.status_code, 409)
            self.assertEqual(response.json()["error"]["code"], "conversation_busy")
            self.assertEqual(calls, [])

    def test_failed_after_user_turn_sent_is_not_retried(self):
        calls = []

        async def fake_provider(request, state_path=None):
            calls.append(request.prompt)
            raise RuntimeError("upstream died after send")

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            app = create_app(provider_generate_fn=fake_provider, state_path=state_path)
            client = TestClient(app, raise_server_exceptions=False)
            body = {
                "model": "meta/muse-spark",
                "conversation_id": "thread-1",
                "messages": [{"role": "user", "content": "Do not double-send this"}],
            }

            first = client.post("/v1/chat/completions", headers={"Idempotency-Key": "idem-after-send"}, json=body)
            second = client.post("/v1/chat/completions", headers={"Idempotency-Key": "idem-after-send"}, json=body)

            self.assertEqual(first.status_code, 500)
            self.assertEqual(second.status_code, 409)
            self.assertEqual(second.json()["error"]["code"], "request_already_sent")
            self.assertEqual(len(calls), 1)

    def test_streaming_request_finalizes_ledger_with_accumulated_text(self):
        async def fake_provider(request, state_path=None):
            return MuseProviderResponse(
                text="unused",
                conversation_id=request.conversation_id,
                template_name=request.template_name,
            )

        async def fake_stream(request, state_path=None):
            for chunk in ["hello", " ", "stream"]:
                yield chunk

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            app = create_app(provider_generate_fn=fake_provider, provider_stream_fn=fake_stream, state_path=state_path)
            client = TestClient(app)

            with client.stream(
                "POST",
                "/v1/chat/completions",
                headers={"Idempotency-Key": "idem-stream"},
                json={
                    "model": "meta/muse-spark",
                    "conversation_id": "thread-1",
                    "stream": True,
                    "messages": [{"role": "user", "content": "Stream this"}],
                },
            ) as response:
                body = b"".join(response.iter_bytes()).decode("utf-8")

            self.assertEqual(response.status_code, 200)
            self.assertIn('"content":"hello"', body)
            record = RequestLedger(default_ledger_path(state_path)).get_by_idempotency_key("idem-stream")
            self.assertEqual(record["phase"], "completed")
            self.assertEqual(record["response_text"], "hello stream")

    def test_stream_generator_marks_error_when_closed_before_completion(self):
        errors = []

        async def chunks():
            yield "partial"
            await asyncio.sleep(10)

        async def on_error(exc):
            errors.append(exc.__class__.__name__)

        async def run_probe():
            stream = _stream_chat_completion(
                chunk_iter=chunks(),
                model="meta/muse-spark",
                response_id="chatcmpl-test",
                conversation_id="thread-cancel",
                chunk_size=120,
                on_error=on_error,
            )
            await stream.__anext__()
            await stream.__anext__()
            await stream.aclose()

        asyncio.run(run_probe())
        self.assertIn("GeneratorExit", errors)

    def test_duplicate_streaming_request_replays_cached_sse(self):
        calls = []

        async def fake_provider(request, state_path=None):
            return MuseProviderResponse(
                text="unused",
                conversation_id=request.conversation_id,
                template_name=request.template_name,
            )

        async def fake_stream(request, state_path=None):
            calls.append(request.prompt)
            for chunk in ["cached", " stream"]:
                yield chunk

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            app = create_app(provider_generate_fn=fake_provider, provider_stream_fn=fake_stream, state_path=state_path)
            client = TestClient(app)
            body = {
                "model": "meta/muse-spark",
                "conversation_id": "thread-1",
                "stream": True,
                "messages": [{"role": "user", "content": "Stream then cache"}],
            }

            with client.stream("POST", "/v1/chat/completions", headers={"Idempotency-Key": "idem-stream-cache"}, json=body) as first:
                first_body = b"".join(first.iter_bytes()).decode("utf-8")
            with client.stream("POST", "/v1/chat/completions", headers={"Idempotency-Key": "idem-stream-cache"}, json=body) as second:
                second_body = b"".join(second.iter_bytes()).decode("utf-8")

            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)
            self.assertIn("text/event-stream", second.headers["content-type"])
            self.assertEqual(second.headers["x-muse-spark-cache"], "HIT")
            self.assertIn('"content":"cached stream"', second_body)
            self.assertIn("data: [DONE]", second_body)
            self.assertEqual(len(calls), 1)
            self.assertNotEqual(first_body, "")

    def test_request_inspection_redacts_sensitive_provider_details(self):
        async def fake_provider(request, state_path=None):
            return MuseProviderResponse(
                text="redacted reply",
                conversation_id=request.conversation_id,
                template_name=request.template_name,
            )

        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            app = create_app(provider_generate_fn=fake_provider, state_path=state_path)
            client = TestClient(app)
            response = client.post(
                "/v1/chat/completions",
                headers={"Idempotency-Key": "inspect-safe"},
                json={
                    "model": "meta/muse-spark",
                    "conversation_id": "thread-1",
                    "messages": [{"role": "user", "content": "Inspect this"}],
                },
            )
            self.assertEqual(response.status_code, 200)

            inspection = client.get("/v1/muse-spark/requests/inspect-safe")
            self.assertEqual(inspection.status_code, 200)
            data = inspection.json()
            self.assertEqual(data["phase"], "completed")
            self.assertNotIn("response_json", data)
            self.assertNotIn("response_text", data)
            self.assertNotIn("meta_conversation_id", data)
            self.assertNotIn("request_hash", data)
            self.assertNotIn("error_message", data)

    def test_playground_route_loads_interactive_testing_environment(self):
        app = create_app()
        client = TestClient(app)

        response = client.get("/test")

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("Muse Spark API Playground", response.text)
        self.assertIn("Idempotency-Key", response.text)
        self.assertIn("/v1/chat/completions", response.text)


if __name__ == "__main__":
    unittest.main()
