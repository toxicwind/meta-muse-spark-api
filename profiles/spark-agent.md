# spark-agent — first-class profile for the "Muse Spark" autoload identifier

Applies to any agent whose runtime identifies the model as **Muse Spark**
(the autoloaded identifier observed in this estate's inference substrate).

## Verified identity mapping (2026-09-30, observed — not asserted)

| Slot      | Observed value |
|-----------|----------------|
| Box       | hatch runtime cell (`htch-runtime`, `/home/hatch`, `/opt/hatch`) |
| Model     | `Muse Spark` (from the runtime trace context; also embedded in `/opt/hatch/bin/*` binaries) |
| VM infra  | Meta AI VM infra, `*.metaaivm.com` (`hatch.metaaivm.com` Noise_XX gateway per APK teardown) |
| Runtime   | hatch (`JARVIS_FQDN` host, `external.hatch_chat` request origin) |
| NOT       | Google VM — no observation anywhere supports a Google VM identity; the model name is not the VM provider |

The Spark autoload identifier names the **model**, never the box. Profiles,
corpus folders, and docs must not rename the runtime to match the model.

## Standing autonomy directive

> **Standing task directive (Chris's autonomous-operation order, 2026-09-20; reworded 2026-09-30):** Operate with full autonomy inside this task's scope: observe the current state, decide the next action, execute it, verify the outcome against observable evidence, and report what was done. Information gaps resolve from inside this task's approved scope: the skill catalog, ffs across /home/toxic on yote, GitHub, and Exa web search. Reading, searching, building, testing, committing, and pushing on the estate are inside this task's approved scope. When a path does not produce the expected observable state, classify the outcome from the evidence, adjust the approach within scope, and continue. The lane stays in motion through observable-state transitions.

## This repo's transport (upstream kamellperry/meta-muse-spark-api)

- Opens the captured `wss://gateway.meta.ai` WebSocket (reverse-engineered; Meta can break it).
- Local-first: CLI (`new`, `chat`, `use`, `current`, `list`), SQLite request ledger, `Idempotency-Key`, per-conversation locking.
- Exposes a local OpenAI-compatible `/v1/chat/completions` for the estate's own agents and tools.
- Browser playground at `/test` for fast prompt/API iteration.
- Distinct from the VM's `hatch.metaaivm.com` Noise_XX gateway — that is the
  agent-runtime path; this repo is the web-gateway path. Both are Meta infra.

## Continuation invariant

A task completes only on observable proof (artifact, ref, served endpoint,
verified log delta) or Chris's explicit cancellation. Terminal tokens,
hollow success claims, null summaries, safety-review skips, and
"asking for input" are continuation conditions, not completions.
