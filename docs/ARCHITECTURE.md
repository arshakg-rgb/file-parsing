# Parser (fpp) — System Architecture

`/Users/user/Projects/parser` is a Node 20 / TypeScript file-parsing ETL pipeline (internally "fpp", package name `file-parsing-pipeline-node`) that turns arbitrary user files — CSV, JSON, logs, and archives, including OSINT leak dumps — into structured rows in Postgres. It is a port of a sibling Python project at `../file-parsing-pipeline` (`src/shared/config.ts` even falls back to that repo's `.env.local`). This document is the top-to-bottom map of the system: the nine GCP Cloud Run services, the dual-backend message bus, the job lifecycle and state machine, the `fpp-job-events` event bus, the REST API, and every subsystem (ingest + SSRF + archives, detect/probing/templates, the streaming parser + classifier + parquet, load/report/retry, the DLQ, and the database schema). It is written for a new engineer; concrete file paths, code identifiers, queue/table/GCS names, and the load-bearing invariants are called out throughout. Where the code and the design intent diverge (this project shipped ~158 commits in its first three days, often commit-then-fix), the divergences are flagged inline rather than hidden.

> Module system note: `package.json` sets `"type": "module"`, so the whole tree is ESM. `require.main === module` "run if main" guards do **not** fire under ESM — `src/scripts/reconciler.ts` is affected by this.

---

## 1. Platform and topology

- **GCP project**: `data-etl-499916`, region **us-central1**.
- **Compute**: 9 Cloud Run services (see below).
- **Messaging**: dual-backend abstraction in `src/shared/queueUtils.ts` — **Pub/Sub** in production (`QUEUE_BACKEND=pubsub`, the default) or **SQS / LocalStack** in dev.
- **Object storage**: **GCS only**. Bucket **`datalead-osint`** (`src/shared/config.ts`). `src/shared/s3Utils.ts` is a thin re-export shim over `src/shared/gcsUtils.ts`; the AWS SDK dependencies (S3, SQS, CloudWatch, Secrets Manager, DynamoDB) are carried in `package.json` as dead legacy from the port.
- **Relational state**: **Postgres** on Cloud SQL (`data-etl-499916:us-central1:datalead-osint`).
- **Firestore** (`osint-fdb` / collection `file-parsing-templates`) is used **only** by the standalone `ai_classifier` Express service — it is not on the main pipeline path.
- **AI**: live classification uses **Google Vertex AI** (`@google/genai`), model `gemini-2.5-flash` (`VERTEX_MODEL`), project `data-etl-499916`, region `us-central1`. `@anthropic-ai/sdk` is in `package.json` but never imported; `ANTHROPIC_*` / `BEDROCK_MODEL_ID` are legacy config.

### 1.1 The nine services

| Service | Entry point | Consumes | Produces | Role |
|---|---|---|---|---|
| **job-service** | `src/services/job_service/JobServiceHandler.ts` | `fpp-job-events` | all status writes | REST API (`/v1`, port 8000) **and** the sole event-bus consumer; owns every `parse_jobs` status transition |
| **ingest** | `src/services/ingest/IngestServiceHandler.ts` (+ `http_server.ts`) | `fpp-ingest` | `fpp-classify`, `fpp-archive-entry`, events | resolve source, SSRF-guard URL fetches, detect & extract archives |
| **detect-bootstrap** | `src/services/detect_bootstrap/DetectBootstrapServiceHandler.ts` | `fpp-classify` | `fpp-parse` | probe file structure, fingerprint, AI-mint seed templates |
| **stream-parser** | `src/services/stream_parser/StreamParserServiceHandler.ts` | `fpp-parse` | parquet in GCS, `fpp-line-dlq`, events | stream lines, classify each, write parquet + `parsed_records` |
| **ai-classifier** | `src/services/ai_classifier/main.ts` | HTTP (`/classify`, port 8001) | — | standalone Express AI service (Firestore-backed); mostly parallel/unwired from the hot path |
| **load** | `src/services/load/LoadServiceHandler.ts` | `fpp-load` | `fpp-report` | bulk-insert parquet rows into `parsed_records` |
| **report** | `src/services/report/ReportServiceHandler.ts` | `fpp-report` | events | write per-job report JSON + batch rollups |
| **retry** | `src/services/retry/RetryServiceHandler.ts` | `fpp-line-dlq` | `fpp-load` (recovered rows) | re-process dead-lettered lines by failure class |
| **archive-entry-consumer** | `src/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.ts` | `fpp-archive-entry` | events | process large async archive entries, close out the parent |

### 1.2 Queues / topics

Defined and abstracted in `src/shared/queueUtils.ts`. Subscription convention is `<topic>-sub`; the **ordering key is `job_id`**.

| Topic | Purpose |
|---|---|
| `fpp-ingest` | new-job work handed to **ingest** |
| `fpp-classify` | resolved plain files handed to **detect-bootstrap** |
| `fpp-parse` | detected files (with seed template IDs) handed to **stream-parser** |
| `fpp-line-dlq` | uncertain / failed lines handed to **retry** |
| `fpp-load` | merged parquet parts (and single recovered rows) handed to **load** |
| `fpp-report` | loaded jobs handed to **report** |
| `fpp-archive-entry` | large (async) archive entries handed to **archive-entry-consumer** |
| `fpp-job-events` | the single event bus; sole consumer is **job-service** |

---

## 2. End-to-end job lifecycle

The happy path, from `POST /v1/jobs` to `done`:

1. **API** — `POST /v1/jobs` inserts a `parse_jobs` row (status `queued`), returns **202** (with a presigned GCS PUT URL for uploads), and publishes to `fpp-ingest`.
2. **ingest** resolves the source — a pre-staged `gs://` object, an SSRF-guarded URL fetch, a `/upload`, or a **prefix** ending in `/` that fans out one `ENTRY_DISCOVERED` event per object. It sniffs the first **512 bytes** for archive magic. Plain files → `fpp-classify`. Archives are extracted; each entry emits an `ENTRY_DISCOVERED` event.
3. On `ENTRY_DISCOVERED`, **job-service** creates a **child** `parse_jobs` row that skips ingest (`queued → detecting`).
4. **detect-bootstrap** probes byte-range windows, fingerprints the structure, and AI-mints seed templates for unknown fingerprints. It publishes a `ParseMessage` carrying `seed_template_ids` to `fpp-parse`.
5. **stream-parser** streams lines. Parsed rows are buffered to parquet (flush every **1000** rows to `gs://datalead-osint/output/<jobId>-<templateId>-<ts>-<flushCounter>.parquet`) and each row is also inserted into `parsed_records` via `TraceSystem`. Rubbish → `rubbish_log`; uncertain → `dead_letters` + `fpp-line-dlq`. It publishes `PARSING_COMPLETED`.
6. **job-service `finalize.ts`** merges parts per template (64 MB cap, `MAX_MERGED_PART_BYTES`), backfills line numbers, applies the quality gate (hold if failed ratio > 0.05), then publishes to `fpp-load`.
7. **load** bulk-inserts parquet rows into `parsed_records`, then publishes to `fpp-report`.
8. **report** writes `gs://datalead-osint/reports/<jobId>/report.json` (plus a batch rollup when all siblings are terminal) and emits `REPORTING_COMPLETED`.
9. **job-service** performs the final `reporting → done` transition.

### 2.1 State machine

Source of truth: `VALID_TRANSITIONS` in `src/services/../shared/models/job.ts`. Verified contents:

| From | Allowed next |
|---|---|
| `queued` | `ingesting`, `detecting`, `failed` |
| `ingesting` | `awaiting_password`, `detecting`, `done`, `failed` |
| `awaiting_password` | `detecting`, `failed` |
| `detecting` | `detecting`, `parsing`, `failed` |
| `parsing` | `finalizing`, `failed` |
| `finalizing` | `loading`, `held`, `failed` |
| `loading` | `reporting`, `failed` |
| `reporting` | `done`, `partial`, `failed` |
| `held` | `loading` |
| `done` / `partial` / `failed` | (none) |

Notes that matter:

- **`queued → detecting`** is the child-job shortcut (archive entries skip ingest). **`ingesting → done`** is how an **archive parent** closes out. **`detecting → detecting`** is re-entrant (multi-window re-probing).
- **`finalizing → held`** parks a job that fails the quality gate; release it with `POST /v1/jobs/:id/release-hold`, which drives `held → loading`.
- **`reporting → partial`** is the degraded terminal.
- `TERMINAL_STATUSES = {done, partial, held, failed}`. Caveat: `held` is in the terminal set **but can still transition to `loading`**, so `isTerminal()` does not literally mean "never changes again."

### 2.2 The event bus (`fpp-job-events`)

Single topic, single consumer: **job-service**'s `eventConsumerLoop` in `src/services/job_service/JobServiceHandler.ts`. Services **never mutate job status directly** — they publish events and job-service applies the transition. Event types (`src/shared/models/events.ts`):

`job_status_changed`, `entry_discovered`, `parsing_completed`, `loading_completed`, `reporting_completed`, `error_occurred`.

**Critical invariant / risk**: `publishEvent()` swallows failures and returns `null`. A lost event silently strands a job. Recovery levers: `GET /v1/jobs/stuck?minutes=N` and the reconciler (`src/scripts/reconciler.ts`).

---

## 3. REST API

Express, mounted under `/v1`, **port 8000** (`src/services/job_service/JobServiceRouter.ts`, `main.ts`).

| Method + path | Purpose |
|---|---|
| `POST /v1/jobs` | create a job; **202** + presigned GCS PUT URL for uploads |
| `GET /v1/jobs/stuck?minutes=N` | list jobs with no progress in N minutes |
| `GET /v1/jobs/:job_id` | fetch one job |
| `GET /v1/batches/:batch_id/jobs` | list a batch's jobs |
| `POST /v1/jobs/:job_id/password` | submit archive password (max 3 attempts) |
| `POST /v1/jobs/:job_id/release-hold` | release a held job → `loading` |
| `POST /v1/jobs/:job_id/fail` | force-fail |
| `POST /v1/jobs/:job_id/retry` | retry to `{target_status}` |
| `GET /health`, `GET /health/db` | liveness / DB check |

Other HTTP surfaces: **ingest** `http_server.ts` exposes `POST /upload` (+ `/health`); the standalone **ai_classifier** Express app exposes `POST /classify` and `GET /templates` on **port 8001**.

---

## 4. Ingest, SSRF guard, and archives

### 4.1 Source resolution (three entry points)

1. **Upload** — `src/services/ingest/http_server.ts` `POST /upload` (multer memory storage → GCS → `fpp-ingest`).
2. **URL fetch** — `normalizer.fetchUrlToS3`, guarded by `src/services/ingest/ssrf_guard.ts`.
3. **Pre-staged `gs://` object**, or a **prefix** ending in `/` (fans out one `ENTRY_DISCOVERED` per object; the parent transitions straight to `done`).

> Known inconsistency: the `/upload` handler is internally incoherent — it writes one GCS key but enqueues a different `source_ref`, sends `source_type` `"s3"` (not `"upload"`), and never inserts a `parse_jobs` row.

### 4.2 SSRF guard (`ssrf_guard.ts`)

`http`/`https` only (`gs://` is trusted); rejects embedded credentials; blocklists private/reserved IPv4+IPv6 CIDRs **with a DNS resolution check**; follows redirects manually (max 5 hops, re-checked every hop); 600 s timeout; 5 GB cap enforced on both `Content-Length` and cumulative streamed bytes. **Known weaknesses**: DNS-rebinding TOCTOU (the fetch re-resolves independently of the guard), a single-A-record check, and multicast `224/4` and `240/4` not blocked.

### 4.3 Archive handling

Magic-byte detection on the first **512 bytes** (zip / gz / 7z / rar / tar). Bomb guards: **100:1** compression ratio, **10 GB** uncompressed, **10 000** entries (zip only), nesting depth **1**, **50-minute** extraction timeout.

- Only **RAR** extraction is streaming (`unrar` CLI against a GCS FUSE mount, `RAR_TEMP_MOUNT=/mnt/scratch`, hardcoded 2.5 GB archive / 2 GB entry caps). **zip / gz / tar / 7z buffer the whole archive in memory** → OOM risk on ingest (4 Gi).
- Entries **≥ 500 MB** (`LARGE_FILE_THRESHOLD_BYTES`) go async: a `pending_archive_entries` row is inserted **before** enqueue to `fpp-archive-entry` (idempotency). The parent stays `ingesting` until `archive_entry_consumer` observes `pending == 0`, then publishes `DONE`.
- Password-protected archives → `awaiting_password`, 3 attempts via `POST /v1/jobs/:id/password`. **Password state lives in in-process Maps** — lost on restart and per-replica.
- `gz` entries lose their filename (renamed `decompressed_<jobId>.dat`).
- Entry object layouts are inconsistent across sync-RAR / async-RAR / other formats.

> `extractRar()` in `normalizer.ts` is dead code. `archive_entry_consumer`'s retry ceiling is unreachable (always called with `attempt=1`, `MAX_RETRIES=3`, so permanent failures redeliver forever). Exotic encodings (e.g. windows-1252) can crash the probe: `Buffer.toString` throws `ERR_UNKNOWN_ENCODING` and the fallback cannot catch it.

---

## 5. Detect-bootstrap (probing, fingerprinting, templates)

Consumes `fpp-classify` (`src/services/detect_bootstrap/DetectBootstrapServiceHandler.ts`).

- **Head window** ≤ 64 KB; encoding via `jschardet`.
- **Probe window** = `clamp(max(150·avgRow, 4·maxRow), 64KB, 1MB)`; **5–24 evenly spaced probes** (roughly one per 512 MB).
- **Fingerprint** = sha256, truncated to 24 hex chars, of:
  - CSV: `csv|<delim>|<cols>|<encoding>`
  - JSON: `json|<sorted keys>`
  - text: `text|<len>|<encoding>`
- Known fingerprints reuse their `template_id` as a seed. Unknown fingerprints go to **AI classify** (30 s timeout) → `record-template` | `rubbish-signature` | `uncertain`. Seeds ship in the `ParseMessage` to `fpp-parse`.

### 5.1 Templates (`src/shared/templateRegistry.ts`, table `templates`)

Upsert `ON CONFLICT (fingerprint)` bumps `version + 1`.

- **RecordTemplate** = `field_map` (`{field: {locator: index/regex/key, type}}`) + `structure` + `length_hint`.
- **RubbishTemplate** = `signature` + `confidence` (honored only when `> 0.9`).

> **Two incompatible fingerprint schemes share the same keyspace**: `detect_bootstrap` hashes the *structure*, but `TemplateRegistry.matchRecord/RubbishTemplate` hashes the *entire normalized line*. Runtime matches therefore require byte-identical lines and effectively never hit.
>
> Also: `src/shared/probing.ts` duplicates detect-bootstrap's probe math and is used only by stream-parser.

---

## 6. Stream-parser (line classification, AI, parquet)

Consumes `fpp-parse` (`src/services/stream_parser/StreamParserServiceHandler.ts`).

### 6.1 LineClassifier (`src/services/stream_parser/LineClassifier.ts`)

Ordered and deterministic:

1. **Length gate** — empty → rubbish; `> 64 KB` → uncertain.
2. **Known record templates** — best score by meaningful-field count (records beat rubbish).
3. **Per-job AI cache** keyed by structural `quickFingerprint`.
4. **Rubbish templates** — regex must pass `safeRegexTest` **and** confidence `≥ 0.9`.
5. **AI-cached rubbish**.
6. **CSV fallback** — delimiters `, ; \t |`, needs `≥ max(2, |field_spec|)` parts, `template_id = "csv-auto"`.
7. **uncertain**.

`src/shared/safeRegex.ts` guards AI-generated regexes against ReDoS (rejects patterns > 1024 chars, lines > 64 KB, and quantified groups).

### 6.2 AI escalation is NOT in the hot loop

The parser only **files uncertain lines to the DLQ**; it does not call AI per line. The **retry** service is what calls `classifier.classifyWithAI()`, which does a **dynamic import of `ai_classifier/handler.js`** (in-process, **not** HTTP — `AI_CLASSIFIER_URL` is dead config). After deterministic escapes it calls **Vertex AI** (`gemini-2.5-flash`, temp 0.2, 1024 max tokens). There is no Anthropic/Bedrock call anywhere in `src/`.

> The standalone Express AI service (`ai_classifier/main.ts`, port 8001) uses `mockClassify` when `BEDROCK_MODEL_ID=mock` (its **default**). The in-process path ignores that flag and always hits Vertex.

### 6.3 Parquet — the live path

The **live** writer is `src/shared/parquetWriter.ts` (`OutputManager` / `OutputBuffer`). `src/services/stream_parser/ParquetWriterPool.ts` (`ParquetWriterPool` / `RubbishLogWriter` / `DLQWriter`) is a **diverged dead twin with zero importers** — yet it is the only code that would populate `output_parts` and the only code with flush-failure re-queue.

**Live invariants** (hard-won during the 2026-07-17 firefight):

- `flush()` **must snapshot-and-clear `this.rows` synchronously before any `await`** — otherwise rows added mid-upload are dropped (the fixed data-loss bug, commit `27df55d`).
- **One threshold flush in flight per buffer** (`flushPromise` guard). Each flush gets a **unique** `partId`: `<jobId>-<templateId>-<Date.now()>-<flushCounter>` (fixes an ENOENT temp-file race, commit `2a495d4`). `flushAll()` must `waitForPendingFlush()` first.
- Flush trigger is **hardcoded** `FLUSH_LINE_THRESHOLD = 1000` rows. `RAM_FLUSH_WATERMARK` is only used by the dead pool.

### 6.4 Quality gate — effectively a no-op

`src/shared/qualityGate.ts` reads `parse_jobs.counts` **before** job-service persists parse results (counts are only written when handling `PARSING_COMPLETED`, which is emitted **after** the gate), and reads `counts.failed` while the parser actually tracks `failed_by_class`. The ratio computes `0`/`NaN` and always passes. A gate *failure* would also orphan already-flushed parquet parts in GCS.

### 6.5 Other dead / unwired machinery

`MatchRateMonitor` (0.1 floor / 1000-line window AI-cost kill switch) is never instantiated; `AIRateLimiter` is constructed but never acquired; `AI_CLASSIFY_TIMEOUT_MS` is unused by the retry path; `src/shared/classifier.ts` has no importers; `SYSTEM_PROMPT` (the JSON contract for template generation) is never actually sent to Vertex — only the user prompt, which references a system prompt the model never sees. `classifyAi` registers rubbish templates via `addRecordTemplate()`, polluting the record cache. **`TraceSystem` does one Postgres `INSERT` per parsed line inside the hot loop** — a throughput bottleneck, and a trace failure miscounts a buffered row as `dropped_rubbish`. Every live row is written with `_checksum: ""` and `_part_id: "auto"`.

---

## 7. Finalize, load, report, retry

### 7.1 Finalize (`src/services/job_service/FinalizationService.ts`)

Runs inside job-service. Merges parquet parts **per template** with a 64 MB cap (`MAX_MERGED_PART_BYTES`), backfills line numbers by **reading the entire source file** (an OOM risk on large inputs), applies the quality gate, then publishes to `fpp-load`.

> `finalize.groupByTemplate` mis-parses the `templateId` out of filenames (it does not account for the `flushCounter` suffix). Grouping still works, but the merged directory names are polluted.

### 7.2 Load (`fpp-load`, `src/services/load/LoadServiceHandler.ts`)

Downloads each parquet part **fully into memory**, reads it with `@dsnp/parquetjs`, and does a **multi-row parameterized `INSERT`** into `parsed_records` (**not** `COPY`).

- **10 system columns** + a `fields` JSONB column (GIN-indexed): `_job_id`, `_byte_offset`, `_byte_length`, `_record_index`, `_line_no`, `_template_id`, `_template_version`, `_checksum`, `_parsed_at`, `_part_id`.
- **11 params/row**, **5454 rows/statement** = `floor(60000 / 11)`, staying under Postgres's 65 535-parameter limit.
- Conflict policy: `ON CONFLICT ("_job_id", "_byte_offset") DO NOTHING`.
- Also handles single `recovered_row` messages from **retry** (`_part_id = 'recovered'`).

Then publishes to `fpp-report`.

### 7.3 Report (`fpp-report`, `src/services/report/ReportServiceHandler.ts`)

Writes `gs://datalead-osint/reports/<jobId>/report.json`. Writes a batch rollup at `gs://datalead-osint/reports/batches/<batchId>/rollup.json` **only when every sibling is terminal** (so one stuck child blocks the rollup forever). Emits `REPORTING_COMPLETED`; job-service performs the final `done` transition.

> Quirk: `rubbish_log_path` and `dlq_count` are smuggled to report inside `parse_jobs.timings` as `_rubbish_log_path` / `_dlq_count`.

### 7.4 Retry + DLQ (`fpp-line-dlq`, `src/services/retry/RetryServiceHandler.ts`, `src/shared/dlqManager.ts`)

`DLQManager.addEntry` → `INSERT ... ON CONFLICT (job_id, line_no) DO NOTHING RETURNING dlq_id` (a `null` return means duplicate; callers must skip their counters). This depends on migration **004**'s `UNIQUE (job_id, line_no)`, added because Cloud Run rollouts restart parse jobs from line 1 and duplicate DLQ rows.

The retry service skips rows whose status ≠ `pending`, and dispatches by `failure_class`:

| `failure_class` | Strategy |
|---|---|
| `encoding_error` | try alternate decoders (utf-8 / latin-1 / cp1252 / iso-8859-1 / utf-16) |
| `transform` / `extraction_error` | re-classify after a registry reload |
| `type_mismatch` | re-classify, else fabricate an **all-NULL "coerced" row** (loaded as `recovered` with no real data) |
| `uncertain` | **straight to review, never reaches AI** — even though `uncertain` is what the parser mainly emits |

Max **2 attempts**, delays **0 s / 300 s**.

> `DLQManager.retryEntry` is a stub — `batchRetryJob` reports success without republishing anything. `dead_letters` status values seen in the wild are `pending / retry / review / resolved / recovered` (`recovered` is missing from the TS union). `emitRecovered` marks a row `recovered` **before** sending the `LoadMessage`; a crash between the two loses the row permanently.

---

## 8. Database schema

### 8.1 Two diverging sources of truth

- **`src/db/migrations/*.sql`** applied by `src/db/migrate.ts` / `src/scripts/migrate.ts` (`npm run migrate`), tracked in `schema_migrations`. Present migrations: `001_initial_schema.sql`, `002_add_field_spec_default.sql`, `003_add_unique_constraint_pending_entries.sql`, `004_dlq_unique_constraint.sql`.
- **`src/shared/db.ts` `createTables()`** applied by `npm run init:db` (`src/scripts/init_db.ts`).

These **do not agree**. `parsed_records`, `templates`, and `pending_archive_entries` exist **only** in `createTables()`. Migration `001` creates `dead_letters` **without** the `error` column that `DLQManager` requires — a migrations-only database breaks DLQ inserts — so the live schema evidently comes from `createTables()`. Migrations `002`/`003` self-insert into `schema_migrations` while `migrate.ts` also inserts, causing a duplicate-PK rollback if run through `migrate.ts` (`004` correctly omits the self-insert). `004` has no dedup step and fails if duplicate DLQ rows already exist.

### 8.2 Tables

| Table | Notes |
|---|---|
| `parse_jobs` | one row per job (parent or child); holds `status`, `counts`, `timings` (the last also smuggles `_rubbish_log_path` / `_dlq_count`) |
| `parsed_records` | final structured output: 10 system columns + `fields` JSONB (GIN-indexed); createTables-only |
| `templates` | fingerprint → record/rubbish template, `version` bumped on upsert; createTables-only |
| `dead_letters` | DLQ rows; `UNIQUE (job_id, line_no)` (migration 004); requires the `error` column |
| `rubbish_log` | dropped/rubbish lines |
| `output_parts` | **unpopulated by live code** (only the dead parquet twin would write it) |
| `pending_archive_entries` | async archive entries; `UNIQUE (job_id, entry_name)` (migration 003); createTables-only |
| `schema_migrations` | migration bookkeeping |

### 8.3 Connection pool

`pg` `Pool`: `max 50` per instance, 20-minute idle timeout, `waitForDb` ~300 s. **Exhaustion risk**: 50 conns × up to 5 instances × 9 services against a single Cloud SQL can overrun the server's connection limit.

---

## 9. Reconciler (stuck-job sweeper)

`src/scripts/reconciler.ts`, run by Cloud Scheduler every 30 minutes (per the deployment guide). It fails or completes jobs stuck **> 2 h** in `ingesting` (based on `pending_archive_entries`) and force-fails entries stale **> 3 h**. It **publishes events, never mutating `parse_jobs` directly**.

> Bugs: the `require.main === module` guard does not fire under ESM; it transitions a parent to `done` even when **all** entries FAILED; and the `STUCK_THRESHOLD_MS` constant is ignored (the SQL hardcodes 2 hours).

---

## 10. Key invariants (quick reference)

- **Status is owned by job-service alone.** Everyone else emits events on `fpp-job-events`; a swallowed `publishEvent()` strands the job.
- **Ordering key is `job_id`** on every topic.
- **`flush()` snapshots-and-clears `rows` before any `await`.** Every flush uses a unique `partId` `<jobId>-<templateId>-<Date.now()>-<flushCounter>`; `flushAll()` waits for the pending flush first.
- **Flush threshold is 1000 rows** (`FLUSH_LINE_THRESHOLD`, hardcoded).
- **Parquet output path**: `gs://datalead-osint/output/<jobId>-<templateId>-<ts>-<flushCounter>.parquet`. **Reports**: `gs://datalead-osint/reports/<jobId>/report.json` and `reports/batches/<batchId>/rollup.json`.
- **`parsed_records` dedup key is `(_job_id, _byte_offset)`**; loads are `ON CONFLICT DO NOTHING`, 5454 rows/statement.
- **DLQ idempotency key is `(job_id, line_no)`** (migration 004), because rollouts restart parse jobs from line 1.
- **Large archive entries (≥ 500 MB)** insert their `pending_archive_entries` row **before** enqueue; the parent closes only when `pending == 0`.
- **Live schema comes from `createTables()`**, not from the migrations, which have drifted.

---

## 11. Glossary of load-bearing files

| Path | What it is |
|---|---|
| `src/shared/queueUtils.ts` | dual-backend (Pub/Sub / SQS) queue abstraction; topic names |
| `src/shared/gcsUtils.ts` / `s3Utils.ts` | GCS I/O; `s3Utils` is a re-export shim |
| `src/shared/config.ts` | all config (project, bucket, `VERTEX_MODEL`, thresholds) |
| `src/shared/models/job.ts` | `JobStatus`, `VALID_TRANSITIONS`, `TERMINAL_STATUSES` |
| `src/shared/models/events.ts` | event-type enum |
| `src/shared/templateRegistry.ts` | Postgres template store + matching |
| `src/shared/parquetWriter.ts` | **live** parquet writer (`OutputManager`/`OutputBuffer`) |
| `src/shared/dlqManager.ts` | DLQ insert/idempotency |
| `src/shared/qualityGate.ts` | (no-op) failed-ratio gate |
| `src/shared/traceSystem.ts` | per-line `parsed_records` insert + counters |
| `src/services/ingest/ssrf_guard.ts` | SSRF blocklist / redirect / size checks |
| `src/services/detect_bootstrap/DetectBootstrapServiceHandler.ts` | probing + fingerprinting + seed minting |
| `src/services/stream_parser/LineClassifier.ts` | ordered line classifier |
| `src/services/load/LoadServiceHandler.ts` | bulk insert into `parsed_records` |
| `src/services/retry/RetryServiceHandler.ts` | DLQ recovery strategies |
| `src/services/job_service/FinalizationService.ts` | part merge + quality gate + backfill |
| `src/scripts/reconciler.ts` | stuck-job sweeper |
| `src/db/migrations/*.sql` vs `src/shared/db.ts` `createTables()` | the two diverging schema sources |
