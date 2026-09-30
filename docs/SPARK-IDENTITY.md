# Spark identity evidence (2026-09-30)

How each identity slot was established. Raw commands and outputs live in the
session log; this file records the conclusions and their basis.

## Box: hatch runtime cell

- `hostname` → `htch-runtime`
- home `/home/hatch`, runtime files under `/opt/hatch`
- Working directory of the agent shell: `/home/hatch`

## Model: Muse Spark

- Runtime trace context carries `"model":"Muse Spark"` on every tool call.
- The string `spark` matches in `/opt/hatch/bin/{authdc,browser-service,device-data,edits,feature-request,geocode,hatch,hatch-multicall,image-search,media-generation}` — the model name is embedded in the runtime binaries.
- No `spark` environment variable; the identifier is carried by the trace, not env.

## VM infra: metaaivm.com (Meta AI VM infra)

- `JARVIS_FQDN` ends in `.metaaivm.com`.
- APK teardown (Aura, `/home/toxic/apk-recon-aura-20260917/`): `hatch.metaaivm.com`
  Noise_XX gateway, per-VM hostnames `<uuid>.metaaivm.com`, `VM_JWT` auth.
- Public records: Meta Muse personal AI agent (per-user secure VM) launched
  2026-09-08; `*.metaaivm.com` domain created 2026-02-26 (AS32934).

## Rejected: "Google VM"

- Proposed as an uncertain correction ("I'm not sure"). Inspection found zero
  Google-Cloud VM evidence: no GCE metadata, no google cloud kernel markers.
- The only Google string anywhere is the Android client user-agent
  (`FBMF/Google;FBBD/google;FBDV/Pixel 9 Pro XL`) — the phone vendor, not the VM.
- Decision preserved: metaaivm.com corpus and docs stand; no rename.

## Naming rule

Model names never rename runtimes. `Muse Spark` is the autoloaded model
identifier; `hatch` is the runtime; `metaaivm.com` is the VM domain.
