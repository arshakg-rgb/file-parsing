# Deployment

Authoritative deploy reference for the parser (file-parsing) pipeline.

- **GCP project:** `data-etl-499916`
- **Region:** `us-central1`
- **Artifact Registry repo:** `us-central1-docker.pkg.dev/data-etl-499916/file-parsing/*`
- **Runtime:** Cloud Run (managed), one service per pipeline stage.

---

## 1. The authoritative deploy path

**The one true deploy is a Cloud Build trigger on this repo that runs `cloudbuild.yaml` on push to `main`.** Everything else is either a fallback or a stale path to ignore.

What `cloudbuild.yaml` does, in order:

1. **Create `.env` from Secret Manager** (`gcr.io/cloud-builders/gcloud`). This step is **vestigial** — no Dockerfile copies `.env` into an image. Real runtime config is injected by the per-service Cloud Run `--set-env-vars` / `--update-secrets` flags, not this file. It is harmless but do not rely on it.
2. **`npm install && npm run build`** in a `node:20` step. The Dockerfiles only `COPY dist/` — they do **not** build inside the image, so this step is what actually produces the shipped code. If this step builds stale code, every image ships stale code (see §7).
3. **Build + push Docker images** to `us-central1-docker.pkg.dev/data-etl-499916/file-parsing/<service>:latest`.
4. **Deploy each service to Cloud Run** with a 3-phase pattern per service:
   - `gcloud run deploy … --remove-env-vars=… || true` (strip any conflicting plain env vars)
   - `gcloud run deploy … --remove-secrets=… || true` (strip any conflicting secret-backed vars)
   - `gcloud run deploy …` with the full flag set and `--update-secrets=…`.

   The strip-then-set dance exists so future secret rotations never fail with an env-var-vs-secret type conflict.

**cloudbuild.yaml is the source of truth for scaling/CPU flags.** Every deploy re-asserts `--min-instances`, `--max-instances`, and `--no-cpu-throttling`. Cloud Run does **not** inherit settings between revisions, so **manual console changes are reverted on the next push.** If you change scaling in the console, also change it here.

Common flags on every service: `--allow-unauthenticated` (note: the job API is publicly reachable), `--service-account=datalead-osint@data-etl-499916.iam.gserviceaccount.com`, `--add-cloudsql-instances=data-etl-499916:us-central1:datalead-osint`, `--timeout 3600s`, `--min-instances 1`, `--max-instances 5`, `--no-cpu-throttling`.

---

## 2. The 9-service topology and images

| Service (Cloud Run name) | Image (`…/file-parsing/<name>:latest`) | Dockerfile | Entry point (`CMD`) | Memory | Notes |
|---|---|---|---|---|---|
| `job-service` | `job-service` | `Dockerfile.job` | `dist/services/job_service/main.js` | 512Mi | Public job API |
| `ingest` | `ingest` | `Dockerfile.ingest` | `dist/services/ingest/IngestServiceHandler.js` | 4Gi | gen2, session affinity, GCS bucket `datalead-osint` mounted at `/mnt/scratch` for RAR; bundles `unrar` static binary |
| `detect-bootstrap` | `detect-bootstrap` | `Dockerfile.detect` | `dist/services/detect_bootstrap/DetectBootstrapServiceHandler.js` | 512Mi | Runs AI in-process |
| `stream-parser` | `stream-parser` | `Dockerfile.stream` | `dist/services/stream_parser/StreamParserServiceHandler.js` | 1Gi | Runs AI in-process; writes Parquet + per-job CSV output |
| `ai-classifier` | `ai-classifier` | `Dockerfile.ai` | `dist/services/ai_classifier/main.js` | 512Mi | Standalone `/classify` HTTP service — **no callers** (see §4) |
| `load` | `load` | `Dockerfile.load` | `dist/services/load/LoadServiceHandler.js` | 512Mi | Loads parsed output |
| `report` | `report` | `Dockerfile.report` | `dist/services/report/ReportServiceHandler.js` | 512Mi | |
| `retry` | `retry` | `Dockerfile.retry` | `dist/services/retry/RetryServiceHandler.js` | 512Mi | |
| `archive-entry-consumer` | `archive-entry-consumer` | `Dockerfile.archive-entry` | `dist/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.js` | 2Gi | gen2, session affinity, same `/mnt/scratch` GCS mount; bundles `unrar` |

Image variable names in `cloudbuild.yaml`: `_JOB_IMAGE`, `_INGEST_IMAGE`, `_DETECT_IMAGE`, `_STREAM_IMAGE`, `_AI_IMAGE`, `_LOAD_IMAGE`, `_REPORT_IMAGE`, `_RETRY_IMAGE`, `_ARCHIVE_ENTRY_IMAGE`.

---

## 3. Current FAST-TEST subset (4 of 9 services)

`cloudbuild.yaml` is currently trimmed to **4 services** to keep test builds fast. Only these build and deploy:

- `job-service`
- `ingest`
- `detect-bootstrap`
- `stream-parser`

The other **5 are disabled** via commented-out blocks tagged `>>> DISABLED FOR FAST TEST BUILDS … <<<`:

- `ai-classifier` — build/push and deploy blocks
- `load` + `report` — build/push and deploy blocks
- `retry` + `archive-entry-consumer` — build/push and deploy blocks

Behavioral consequences of the subset:
- Without `load`/`report`, a job parks at status `loading`, **but the output Parquet/CSV files are still written during parsing** (stream-parser writes `gs://$DATA_BUCKET/output/<jobId>.csv` and Parquet directly).
- Without `ai-classifier`, nothing breaks — AI classification runs in-process (see §4).

### How to re-enable the disabled 5

Each disabled service has **three** things to uncomment. Do all three per service or the deploy will be inconsistent:

1. **Build + push step** — the `# - name: 'gcr.io/cloud-builders/docker'` block(s) under `# >>> DISABLED FOR FAST TEST BUILDS …` in the `steps:` build section (near the top of `steps:`).
2. **Deploy step** — the corresponding commented `# - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'` deploy block further down.
3. **Images list** — the matching `# - $_…_IMAGE` line in the `images:` block at the bottom of the file.

The `DISABLED FOR FAST TEST BUILDS` comments are grouped exactly as: (a) `ai-classifier`, (b) `load` + `report`, (c) `retry` + `archive-entry-consumer`. To restore the full 9-service topology, uncomment all three groups in all three places. Verify with `grep -n "DISABLED FOR FAST TEST BUILDS" cloudbuild.yaml` returning nothing after editing.

---

## 4. Why `ai-classifier` is safe to disable

The standalone `ai-classifier` Cloud Run service exposes a `/classify` HTTP endpoint that **has no callers**. AI classification happens **in-process** inside `detect-bootstrap` and `stream-parser` via dynamic import of the same handler code:

- `src/services/detect_bootstrap/DetectBootstrapServiceHandler.ts` → `await import("../ai_classifier/AiClassifierServiceHandler.js")` (and `../ai_classifier/mock.js`)
- `src/services/stream_parser/classifier.ts` → `await import("../ai_classifier/AiClassifierServiceHandler.js")`

So the classifier library ships **inside** the detect/stream images and runs in the same process — no network hop to a separate service.

`AI_CLASSIFIER_URL` is defined in `src/shared/config.ts` (as an optional string) but is **never read anywhere as a URL to call**. No code does an HTTP request to the standalone classifier. Disabling the `ai-classifier` service therefore removes zero functionality.

---

## 5. Why GitHub Actions (`.github/workflows/ci-cd.yml`) is manual-only

The old workflow ran its own parallel `docker build` + `gcloud run deploy` and was broken:

- **Invalid Cloud Run service names** — it used underscore names (`stream_parser`, `detect_bootstrap`). Cloud Run requires hyphens, so it never updated the live `stream-parser` / `detect-bootstrap` services. The code "appeared to never deploy."
- **Wrong registry** — it pushed to `gcr.io`, not the Artifact Registry `us-central1-docker.pkg.dev/data-etl-499916/file-parsing/*` used by the real services.
- **Missing WIF setup** — it lacked the `id-token: write` permission / Workload Identity secrets, so authentication would fail anyway.

It ran on every push, producing a **red "failed" run on every push** while never actually deploying.

**Now:** it is `on: workflow_dispatch` only (manual). When run manually, it simply delegates to the authoritative config — a single step: `gcloud builds submit --config cloudbuild.yaml --project "$GCP_PROJECT_ID" .`. It still needs the `GCP_WORKLOAD_IDENTITY_PROVIDER` / `GCP_SERVICE_ACCOUNT_EMAIL` secrets configured before it can submit. It no longer runs on push and no longer produces false red runs. It is a **fallback only** — the real deploy is the Cloud Build trigger.

### Other stale paths to ignore

- `.github/workflows/ci-cd.yml` — fallback only (above).
- `scripts/deploy-gcp.sh` — creates Pub/Sub topics **without** the required `fpp-` prefix. Do not use for topic provisioning.

---

## 6. Deploying manually

If you cannot rely on the push trigger (or want to force a build from your working tree), run the authoritative config directly:

```bash
gcloud builds submit --config cloudbuild.yaml --project data-etl-499916 .
```

This runs the exact same steps the trigger runs (build `dist/`, build/push images, deploy the current service subset). Requires `gcloud` installed and authenticated with rights to Cloud Build, Artifact Registry, Cloud Run, Secret Manager, and the `datalead-osint` service account.

To confirm what actually deployed:

```bash
# What revision/image is live for a service:
gcloud run services describe stream-parser --region us-central1 --project data-etl-499916 \
  --format='value(status.latestReadyRevisionName, spec.template.spec.containers[0].image)'

# Recent builds:
gcloud builds list --project data-etl-499916 --limit 5
```

---

## 7. CURRENT stale-deploy blocker

**Symptom:** After a **successful** build, the deployed `stream-parser` still runs **OLD code**. Proven by output that is impossible under the current code:

- Parquet output contains `email="1368866"` — a bare numeric ID sitting in the email field (the new classifier extracts only declared `field_spec` fields and would never do this).
- A single row-0 "blob" of ~50 merged lines (the new line-splitting / quoted-newline recovery would have split these).

Both artifacts are only producible by the pre-`5e5299b`/pre-`0d11495` classifier and line-splitter, so the running instance is executing code from before this session's merges to `main`.

**Diagnosis — this is a deploy-side problem, not a code problem.** The code on `main` is correct (verified by the 87-case offline suite in `src/scripts/local_test.ts`, sections 16 encoding / 17 classifier ordered flow / 18 line-splitting+CSV). The two leading root-cause hypotheses:

1. **The Cloud Build trigger uses an INLINE build config, not the repo `cloudbuild.yaml`.** A trigger configured with an inline/embedded YAML will not pick up repo changes to `cloudbuild.yaml` or run the current build steps — it can build an old image or skip services. This would explain a "successful" build that ships stale code.
2. **The new revision did not replace the running Pub/Sub consumer.** `stream-parser` is a min-1 consumer; if a new revision was created but traffic/the active consuming instance was not actually cut over (or an old min-instance kept consuming), the live message-processing instance stays on old code.

**Fix (in progress):**

1. Install `gcloud` locally.
2. Force a build from the working tree with the authoritative config:
   ```bash
   gcloud builds submit --config cloudbuild.yaml --project data-etl-499916 .
   ```
3. **Repoint the Cloud Build trigger to the repo `cloudbuild.yaml`** (not an inline config) so future push-triggered builds are reproducible from source.
4. After deploy, verify the live image digest / revision changed (commands in §6) and re-run a known-bad input; confirm the `email="1368866"` bare-ID and merged row-0 blob no longer appear.

### This session's code changes (all merged to `main`, 2026-07-17)

Context for what the running instance *should* be doing once the stale deploy is cleared:

- `0d11495` — Fix encoding detection (`ERR_UNKNOWN_ENCODING` for latin-1/iso-8859-1/cp1252/etc.); decode via `TextDecoder`; prefer UTF-8 over low-confidence jschardet guesses. New `src/shared/encoding.ts`.
- `de5299b` — stream_parser now follows the designed ordered classifier flow (length/binary gate → header capture → record templates → structural JSON/kv recognizers → rubbish → validated CSV → uncertain). Extracts only `field_spec` fields; declines junk; removed the `formatDetector` bypass and greedy positional csv-auto. Hardened via a 26-agent adversarial review (20 bugs fixed).
- `79d5cca` — Fix `field_spec` JSONB insert (`invalid input syntax for type json`): `JSON.stringify` the array before binding (`router.ts` + `stateMachine.ts`).
- `4463d80` — Line-splitting recovery for stray/unbalanced quotes (`gcsUtils` scanLines; `MAX_QUOTED_NEWLINES` default 0, `MAX_LINE_BYTES` cap) + per-job CSV writer (`src/shared/csvOutputWriter.ts` → `gs://$DATA_BUCKET/output/<jobId>.csv`).
- `4091092` + `c7d7388` — Trimmed `cloudbuild.yaml` to the 4 test services; disabled the other 5 (see §3).
- `8b8f91d` + `81ab663` — Switched `ci-cd.yml` to `workflow_dispatch`-only (see §5).
