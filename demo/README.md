# Demo

Everything here is narration and staging for the live incident-response demo -- none of it is
part of the core lifecycle (ingest/transform/validate/enrich/operate/self-heal), which runs
unmodified underneath it. See the root [README](../README.md) for what that core system is.

```
demo/
├── enterprise_incident.py   deterministic, flag-driven scenario runner (the flagship demo)
├── DEMO_SCRIPT.md           the full live walkthrough script, section by section
├── scripts/                 one-shot cluster commands for the RHOAI/ROSA demo
│   ├── inject-bug.sh          deploy payment_service's v2 contract change against live MinIO
│   ├── reset-bug.sh           revert the injection, rerun clean, clear any pending repair
│   └── generate-repair.sh     drive create_candidate_repair/verify_candidate_repair directly via MCP
└── services/                 6 synthetic upstream-event generators (see demo/services/README.md)
```

## Running it locally

`enterprise_incident.py` walks the whole scenario end to end against your local MinIO/Spark
(see the root README's Quickstart to get those running first):

```
python3 -m demo.enterprise_incident --healthy-only            # trusted baseline
python3 -m demo.enterprise_incident --inject-contract-change   # deploy the contract change
python3 -m demo.enterprise_incident --run-repair               # investigate, refuse, approve, repair, verify, PR
python3 -m demo.enterprise_incident --reset                    # restore everything, idempotent
```

Default (`--scripted-model`) costs no API calls -- the real diagnose/repair/verify agent loops
run against real S3/Spark, only the model's responses are canned, replaying the exact tool-call
sequence a real run of this scenario produces. `--live-model` makes real OpenAI calls instead.

## Running it on a live RHOAI/ROSA cluster

See [`DEMO_SCRIPT.md`](DEMO_SCRIPT.md) for the full walkthrough. Short version:

```
export MINIO_ACCESS_KEY=...  MINIO_SECRET_KEY=...   # see deploy/rhoai/RUNBOOK.md
./demo/scripts/inject-bug.sh
# ... click "Diagnose & generate repair" in the console UI, or:
./demo/scripts/generate-repair.sh
./demo/scripts/reset-bug.sh   # when you're done
```
