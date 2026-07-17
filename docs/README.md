# Parser (fpp) — Documentation

Deep documentation for the file-parsing pipeline, written 2026-07-17. Start here.

## Contents

| Doc | What it covers |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | The 9 Cloud Run services, end-to-end job lifecycle + state machine, the `fpp-job-events` bus, dual Pub/Sub-or-SQS queues, GCS storage, the Postgres schema, and each subsystem (ingest/SSRF/archives, detect/probing/templates, stream parser/classifier/parquet, load/report/retry, DLQ). |
| [DESIGN.md](DESIGN.md) | The authoritative design contract — the six rules, the ordered line classifier and the keep-and-check asymmetry, per-service contracts, the data model, the 14 key decisions, tuning defaults, open questions, and out-of-scope. The intent the code should converge to. |
| [DESIGN-VS-IMPLEMENTATION.md](DESIGN-VS-IMPLEMENTATION.md) | Where the code diverges from DESIGN.md and how the 2026-07-17 classifier refactor closed the biggest gaps; what's still deferred (in-loop AI, match-rate monitor). |
| [INGEST-FINDINGS.md](INGEST-FINDINGS.md) | The 32 adversarially-verified ingest defects (7 critical), grouped A–E with file:line anchors, severity, and failure scenarios. Read-only review; re-verify anchors before acting. |
| [INGEST-REMEDIATION.md](INGEST-REMEDIATION.md) | The implementation design for the 32 findings — shared components, 24 change-units by phase, dependency-ordered sequence, hazards, and the 3 decisions needed before deploy. Not yet implemented. |
| [KNOWN-BUGS.md](KNOWN-BUGS.md) | Live bugs, the dead-code map, fragile areas, and the 2026-07-17 parquet-flush/DLQ firefight (root causes + invariants). Marks which items the recent fixes resolved. |
| [DEPLOYMENT.md](DEPLOYMENT.md) | The authoritative deploy path (Cloud Build trigger → `cloudbuild.yaml`), the service topology + images, the current fast-test 4-service subset and how to restore the rest, the broken GitHub Actions pipeline, and the current stale-deploy blocker + fix. |
| [CHANGELOG-2026-07-17.md](CHANGELOG-2026-07-17.md) | Everything shipped in the 2026-07-17 session — encoding fix, classifier flow refactor, `field_spec` JSONB fix, line-splitting recovery + CSV output, Cloud Build trim, CI/CD fix — with what/why/verification and the open blocker. |

## TL;DR of current state (2026-07-17)

The pipeline turns arbitrary files (upload/link/S3/archives) into structured Parquet + DB rows, classifying every line against learned templates and extracting only the client's `field_spec` fields.

Recently fixed and merged to `main`: encoding detection, the designed ordered classifier (field-spec-only extraction, junk declined, header mapping), the `field_spec` JSONB insert, stray-quote line-splitting, and a per-job CSV output mirror. Verified by `tsc` + an 87-case offline suite (`src/scripts/local_test.ts`).

**Open blocker:** the deployed `stream-parser` was still running pre-fix code (a build succeeded but the new code wasn't in the running service). The fix is deploy-side — force `gcloud builds submit --config cloudbuild.yaml` and/or repoint the Cloud Build trigger at the repo's `cloudbuild.yaml`. See DEPLOYMENT.md.

The large outstanding work item is the **ingest remediation** (INGEST-FINDINGS.md + INGEST-REMEDIATION.md): 32 verified defects, designed but not implemented.
