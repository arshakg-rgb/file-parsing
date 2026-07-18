# Parser — Design & Architecture Contract

This document is the authoritative design contract for the parser pipeline: the intent the code should converge to. It is derived from the project knowledge base (`~/.claude/projects/-Users-user-Projects-parser/memory/parser-design-spec.md`) and spot-verified against the current implementation under `src/`. Where the code and the intent differ, the intent is stated first and the current implementation reality is called out inline so a new engineer can tell "what we mean to do" from "what the code does today". The system takes client files (upload, link, cloud-object reference, archives), streams them, classifies every line against learned templates, extracts **only** the fields a client asked for, and emits structured Parquet plus database rows — with every dropped and every failed line logged and addressable. A historical note on vocabulary: the original design doc says "S3" throughout; production runs on **Google Cloud Platform** (GCS buckets, Pub/Sub, Firestore, Cloud SQL Postgres). The code keeps `s3_url` / `s3://` naming for compatibility even though the bytes live in GCS — treat "S3" and "object store" as synonyms for GCS below.

---

## 1. The Six Rules

Every design decision traces back to one of these six rules. They are non-negotiable.

| # | Rule | Why |
|---|------|-----|
| 1 | **AI teaches templates; the engine runs them.** AI is asked only when no known template matches; its answer is cached as a template and reused. Never per-line by default. | Per-line AI is unaffordable at file scale. |
| 2 | **Two template stores, one classifier.** Record templates (parseable data) *and* rubbish templates (junk to skip). | So alternating good/rubbish/good blocks don't re-hit AI on every rubbish block. |
| 3 | **Extract only what the client wants** (the field spec, e.g. `email`, `name`, `surname`, `address`). | Shrinks output *and* shrinks every AI request. |
| 4 | **Everything becomes an object-store object first** (uploads, links, archive contents normalized at intake). | One range-addressable source; remote links lack `Range` and expire. |
| 5 | **A bad row never stops a file; a bad file never stops a batch.** Failures → dead-letter (retried). Rubbish → rubbish-log (dropped, counted, **never** retried). Separate stores, never mixed. | Isolation of failure; rubbish is not an error. |
| 6 | **Every line is addressed by `(job_id, byte_offset)` plus a true source `line_no`.** | The key for retry, idempotent loads, tracing, and human review. The global line number is unknowable mid-parse, so it is backfilled at finalize. |

---

## 2. The Ordered Line Classifier

The classifier is the core of the system. It runs on **every** line, ordered cheapest-first, so the expensive path (AI) is reached only for genuinely unknown line patterns. Each distinct line pattern costs **one** AI call in its lifetime.

Conceptually the ordered stages are:

1. **Length / empty / binary gate** — cheapest first. Empty, over-long, or mostly-non-printable lines are dropped and logged locally. Never AI.
2. **Record templates** — try learned record templates; extract the target fields → a Parquet row. Records are tried *before* rubbish (see the asymmetry below).
3. **Rubbish templates** — high-confidence structural signatures only; match → drop + rubbish-log.
4. **AI, only for unknowns** — returns exactly one verdict:
   - `record-template` → cached, line parsed;
   - `rubbish-signature` → cached, dropped + logged;
   - `uncertain` → dead-letter for **human review** (never a guess).

### 2.1 Implementation — `src/services/stream_parser/LineClassifier.ts`

`LineClassifier.classify(line, byteOffset, byteLength)` returns `{ verdict: "parsed" | "rubbish" | "uncertain", row?, template_id?, template_version?, failure_class? }`. Its actual stage order is a faithful, slightly richer expansion of the intent:

| Stage | What it does | Result |
|-------|--------------|--------|
| 1. Length/empty/binary gate | empty → `rubbish` (`template_id: "length-gate"`); `line.length > 64KB` → `uncertain` (`TRANSFORM_ERROR`); >30% non-printable → `rubbish` (`binary-gate`) | local, no AI |
| 1b. Header detection (first data line only) | if the first line is unmistakably a header, capture a name→column map and drop the header itself (`template_id: "header"`) | rubbish |
| 2. Learned **record** templates | score every matching record template, pick the best; extract only `field_spec` fields | parsed |
| 3. AI-cached record | record learned earlier in this job (keyed by `quickFingerprint`) | parsed |
| 4. Structural recognizers | deterministic JSON-object (`parseJsonRecord`) and `Label: value` key-value (`parseKvRecord`) extraction, mapping by key name | parsed |
| 5. Learned **rubbish** templates | signature regex whose `confidence >= RUBBISH_CONFIDENCE_MIN` | rubbish |
| 6. AI-cached rubbish | rubbish learned earlier in this job | rubbish |
| 7. Validated delimited/CSV | header-mapped (`csv-mapped`) if a header was seen, else identify columns by **content** (email/phone) (`csv-auto`); returns null for unmappable junk | parsed |
| 8. Nothing matched | keep-and-check | `uncertain` (`UNCERTAIN`) → caller escalates |

The AI escalation entry points on the same class are `classifyWithAI(line, contextLines)` and `classifyWithTimeout(...)`; both consult the per-job `aiCache` (keyed by `quickFingerprint(line)`), call `classifyAi()` in the AI Classifier, cache the returned template, and re-run extraction. A returned `uncertain` (or a null template) yields `verdict: "uncertain"` — never a forced parse.

Field extraction (`extractLine`) walks `field_spec`, resolves each field through its `field_map` locator (`index:N`, `key:NAME`, or `regex:PATTERN` via `safeRegex`), and returns a row only if at least one target field was present. Missing target fields become `null`/`undefined` — no AI, no failure (see §7 open question 3).

### 2.2 The keep-and-check asymmetry (non-negotiable)

Rubbish is defined by the **absence** of a pattern, so rubbish templates are inherently unreliable. Three rules enforce a bias toward keeping data:

- A rubbish template matches **only** on a high-confidence structural signature (`confidence >= RUBBISH_CONFIDENCE_MIN`, default `0.9`; the AI system prompt and `makeRubbishTemplate` both hard-floor rubbish confidence at `0.90`).
- **Record templates are tried before rubbish templates** — stages 2–4 (records) precede stages 5–6 (rubbish) in the classifier.
- An ambiguous line goes to AI as a *candidate record*, and if the AI is `uncertain` it becomes a dead-letter for human review, **not** a drop.

The rationale is asymmetric cost: **dropping a real record is unrecoverable; an extra AI call is cheap.** Ambiguity always resolves toward "keep and check", never toward "drop".

### 2.3 Thrashing guard

Cached-template matching is local and cheap. A match-rate monitor (`src/services/stream_parser/MatchRateMonitor.ts`; defaults `MATCH_RATE_FLOOR = 0.1`, `MATCH_RATE_WINDOW = 1000`) watches the local-hit ratio so that a collapse in match rate flags the job instead of silently hammering AI. AI cost is bounded by the number of **distinct line patterns** per file, not by the number of lines.

---

## 3. Communication — Three Patterns Only

1. **Queues between stages** — async, retryable. Backend is Pub/Sub in production (`QUEUE_BACKEND=pubsub`) or SQS/LocalStack in dev (`QUEUE_BACKEND=sqs`). Queue/topic names (from `src/shared/config.ts`):

   | Purpose | Setting | Default name |
   |---------|---------|--------------|
   | Ingest | `INGEST_QUEUE_URL` | `fpp-ingest` |
   | Detect/Bootstrap | `CLASSIFY_QUEUE_URL` | `fpp-classify` |
   | Stream Parser | `PARSE_QUEUE_URL` | `fpp-parse` |
   | Line dead-letter | `DLQ_QUEUE_URL` | `fpp-line-dlq` |
   | Load | `LOAD_QUEUE_URL` | `fpp-load` |
   | Report | `REPORT_QUEUE_URL` | `fpp-report` |
   | Job events (fan-in) | `JOB_EVENTS_QUEUE_URL` | `fpp-job-events` |
   | Archive entries | `ARCHIVE_ENTRY_QUEUE_URL` | `fpp-archive-entry` |

2. **Synchronous calls ONLY to the AI Classifier.** No other service is called synchronously.
3. **Every service publishes `job-events`, but only the Job Service mutates job status.** This includes fan-out: Ingest emits `entry_discovered`; the Job Service is what creates child jobs in response. Each service owns its own tables.

---

## 4. Per-Service Contracts (8 services)

The system is eight *logical* services. Deployment packaging is a separate question (see §7 open question 9).

### 4.1 Job Service — the spine
`src/services/job_service/` (`router.ts`, `stateMachine.ts`, `finalize.ts`, `main.ts`)

- Owns the job **state machine** and the `parse_jobs` table.
- Issues **presigned PUT** URLs for uploads (`presignedPutUrl`, returned as `presigned_put_url` on `POST /jobs`, HTTP `202`).
- Attaches the **field spec** to every job; normalizes it to a plain array of field names on intake.
- Handles **archive passwords** as transient input — passed through to Ingest, never logged or stored.
- **Finalize** (`finalizeOutput` in `finalize.ts`): merge parts by template (parts under `MAX_MERGED_PART_BYTES`, default 64MB, are merged; larger groups are kept split), then **backfill true source line numbers** onto parsed rows, the rubbish-log, and DLQ entries. Line numbers are computed by `computeLineMap` — a single forward pass over the source that counts newlines up to each sorted `byte_offset` (a prefix-sum of newline counts), so `(job_id, byte_offset)` maps deterministically to a 1-based `line_no`.
- **Quality gate** runs on **failures, not rubbish**: if `failed / (parsed + dropped_rubbish + failed) > FAILED_LINE_RATIO_THRESHOLD` (default `0.05`), the job is transitioned to `held` (not `loading`). Rubbish is excluded from the ratio.
- Fan-in of `job-events`: `handleEvent` dispatches `JOB_STATUS_CHANGED`, `ENTRY_DISCOVERED` (→ `createChildJob`), `PARSING_COMPLETED` (→ `onParsingCompleted` → finalize → load), `LOADING_COMPLETED` (→ report), `REPORTING_COMPLETED` (→ `done`), `ERROR_OCCURRED` (→ `failed`).

### 4.2 Ingest — everything becomes an object
`src/services/ingest/` (`handler.ts`, `ssrf_guard.ts`, `normalizer.ts`, `http_server.ts`)

- Normalizes **every** source into a range-addressable object in the data bucket.
- **SSRF guard is mandatory** (`ssrf_guard.ts`, `checkUrl` / `fetchUrlStream`): blocks metadata IPs, RFC-1918 and other reserved ranges (IPv4 and IPv6), rejects embedded credentials and non-`http(s)`/`gs:` schemes, re-checks the target on every redirect (max 5, `redirect: "manual"`), and enforces a size cap (`ALLOWED_FETCH_SIZE_BYTES`, default 5GB) plus timeout (`FETCH_TIMEOUT_SECONDS`, default 600s). `gs://` internal URLs are allowed.
- **Archives** are extracted with bomb guards: max compression ratio `ARCHIVE_MAX_COMPRESSION_RATIO` (~100:1), nesting depth `ARCHIVE_MAX_NESTING_DEPTH` (1), uncompressed-size cap `ARCHIVE_MAX_UNCOMPRESSED_BYTES` (10GB), entry-count cap `ARCHIVE_MAX_ENTRIES` (10000). Each entry becomes **its own object + its own job**, emitted as an `ENTRY_DISCOVERED` event (the Job Service creates the child job).
- **Encrypted archives** → `awaiting_password`: the job holds no worker, allows bounded attempts (`ARCHIVE_PASSWORD_MAX_ATTEMPTS`, default 3), and on exhaustion transitions to `failed` (`password_unavailable`). Passwords arrive via `POST /jobs/:job_id/password` → an `action: "provide_password"` message on the ingest queue.

### 4.3 Detect / Bootstrap — adaptive probing
`src/services/detect_bootstrap/DetectBootstrapServiceHandler.ts`, `src/shared/probing.ts`

- **Adaptive probing** decides how to read a file. Probe window `W = max(PROBE_WINDOW_MIN_BYTES, avg_row × PROBE_TARGET_LINES, max_row × 4)` clamped to `PROBE_WINDOW_MAX_BYTES` — defaults `max(64KB, ~150×avg_row, 4×max_row)` capped at 1MB. The unit of information is **lines** (~100–150 per probe): one line can't identify a dialect.
- Probe **count** scales with file size: `ceil(fileSize / PROBE_SIZE_PER_COUNT)` (512MB per probe) clamped to `[PROBE_COUNT_MIN, PROBE_COUNT_MAX]` = `[5, 24]`, evenly spaced, with **head and tail always** included (`generateProbeOffsets`).
- A **fingerprint** = hash(columns + delimiter + quote). A registry **hit** reuses the template; a **miss** costs one AI call that seeds the first record template.
- `exec_path = "parallel"` is chosen **only** for a fully homogeneous file with no embedded quoted newlines, UTF-8, and size ≥ ~1GB. Mixed files are sequential by definition. (Parallel partitioning is deferred — see §7; the default `exec_path` is `stream`.)

### 4.4 Stream Parser — the single streaming pass
`src/services/stream_parser/` (`handler.ts`, `classifier.ts`, `matchRate.ts`, `parquetWriter.ts`)

- A **single synchronous streaming pass** over the object, using `FETCH_CHUNK_SIZE` fetch chunks (default 8MB; design range 8–16MB) via `streamLines`, carrying a remainder buffer and open-quote state across chunk boundaries. Small files (< `SMALL_FILE_SINGLE_GET_THRESHOLD`, default 128MB) can be single-GET.
- Runs the ordered classifier on every line. A missing target field in a matched line emits empty/null (no AI, no failure). An `uncertain` line goes to the DLQ (`fpp-line-dlq`).
- Each matched line writes **a Parquet row and its trace atomically** — one shared `record_index`, so there are no orphan rows or orphan traces.
- **RAM-watermark flush** (`RAM_FLUSH_WATERMARK`, default ~256MB) writes small per-template, homogeneous Parquet **parts** that finalize merges later. This keeps reader memory constant on multi-GB files.
- On completion the handler emits `PARSING_COMPLETED` with `part_s3_paths`, counts, and DLQ/rubbish pointers.

> **Implementation reality — read this.** The design intent is `unknown → SYNC AI call → cache verdict → act` *inside* the parse loop. The current `handler.ts` does **not** call AI in-loop: it dead-letters `uncertain` lines and lets the **Retry** service perform AI recovery out-of-band. The code comment states this is intentional until the match-rate monitor, AI rate limiter, and a per-job call budget are wired, to avoid serial 30s model calls stalling the loop. The synchronous-AI machinery (`classifyWithAI`, `AIRateLimiter`, `AI_CLASSIFY_TIMEOUT_MS`) exists and is exercised by the Retry path. Treat in-loop sync AI as the target state, not the current one.

### 4.5 AI Classifier — the only service that touches a model
`src/services/ai_classifier/` (`handler.ts`, `main.ts`, `templateRegistry.ts`, `mock.ts`)

- The **only** service allowed to call a model. `classify(unknown_line, field_spec, context)` returns one of the three verdicts.
- Output is **always a declarative template interpreted by the engine — NEVER executed code.** The system prompt in `handler.ts` enforces JSON-only output, three verdicts, rubbish confidence ≥ 0.90, "when in doubt → uncertain", and column names drawn only from the detected structure.
- It **validates each template against the triggering line before returning**, and it tries local fast paths first (direct CSV parse, fingerprint match, existing record/rubbish templates) before falling back to the model.
- It **owns the shared, versioned template registry**. Production uses Vertex AI (`VERTEX_MODEL`, default `gemini-2.5-flash`); local vs hosted model is a deployment knob (`mock.ts`, `BEDROCK_MODEL_ID`, `ANTHROPIC_*`). Templates persist to Firestore (`TEMPLATE_COLLECTION`, default `file-parsing-templates`).

### 4.6 Load — idempotent bulk load
`src/services/load/LoadServiceHandler.ts`

- Bulk-loads merged Parquet into `parsed_records`. **Idempotent** by design: rows are keyed on `(job_id, byte_offset)`, and the insert is `INSERT ... ON CONFLICT ("_job_id", "_byte_offset") DO NOTHING`, so a crash-and-rerun cannot duplicate rows.
- Kept a separate stage on purpose: the object store is a replayable checkpoint, so a DB outage never stalls parsing. Non-system columns are folded into a `fields` JSONB payload; system columns (`_job_id`, `_byte_offset`, `_line_no`, `_template_id`, …) are stored explicitly. Recovered rows from Retry are loaded through the same `upsertRows` path.

### 4.7 Report — terminal-state summary
`src/services/report/ReportServiceHandler.ts`

- Fires on any terminal state. Per file: counts (parsed / dropped-as-rubbish / failed), templates + versions used, failures by class with DLQ pointers, a rubbish-log pointer, and artifact locations. Also produces a batch roll-up.

### 4.8 Retry — DLQ only
`src/services/retry/RetryServiceHandler.ts`

- Consumes the DLQ (`fpp-line-dlq`) only. **Rubbish is never retried.**
- Refetches just the failed line by byte-range and re-classifies per failure class:
  - `transform_error` / `extraction_error` → retry after a template update exists;
  - `type_mismatch` → broader coercion;
  - `encoding_error` → try alternative encodings (`ALT_ENCODINGS = utf-8, iso-8859-1, cp1252, utf-16`);
  - `uncertain` → straight to human review.
- Schedule: **immediate → one delayed → human review.** `RETRY_MAX_ATTEMPTS` (default 2); delayed retries use `RETRY_DELAYED_DELAY_SECONDS` (default 300s). Recovered lines are emitted as a `recovered_row` to the Load queue; exhausted lines are marked `review` in `dead_letters`.

---

## 5. Data Model

Types live in `src/shared/models/` (`job.ts`, `template.ts`, `events.ts`). Names below use the model identifiers.

- **`ParseJob`** — `job_id`, `batch_id?`, `parent_job_id?`, `source_type` (`s3` | `upload` | `url` | `archive_entry`), `source_ref`, `s3_url`, `size`, `field_spec[]`, `exec_path` (`stream` | `parallel`), `status`, `output_paths[]`, `counts { parsed, dropped_rubbish, failed_by_class }`, `timings`. Table: `parse_jobs` (Postgres).
- **`Template`** — `template_id`, `kind` (`record` | `rubbish`), `fingerprint`, `version`, plus either `record { field_map: target→locator, structure, length_hint }` or `rubbish { signature, confidence }`, and `source` (`ai` | `bootstrap` | `user`). Persisted in Firestore.
- **`OutputPart`** — `part_id`, `job_id`, `template_id`, `s3_path`, `row_count`, `byte_size`.
- **`RubbishLogEntry`** — `job_id`, `byte_offset`, `line_no`, `raw_bytes`, `matched_template_id` (retrievable; NDJSON in GCS).
- **`DeadLetterEntry`** — `dlq_id`, `job_id`, `byte_offset`, `byte_length`, `line_no`, `raw_bytes`, `failure_class`, `error`, `attempts`, `status` (`pending` | `retry` | `review` | `resolved`). Table: `dead_letters`.
- **`TraceRecord`** — written atomically with each parsed record: `s3_url`, `byte_offset` + `byte_length`, `record_index`, `line_no`, `job_id`, `part_id`, `template_id` + `template_version`, `checksum`, `parsed_at`.

### 5.1 Job state machine
Defined in `src/shared/models/job.ts` (`VALID_TRANSITIONS`, `TERMINAL_STATUSES`) and driven by `stateMachine.ts`:

```
queued → ingesting [→ awaiting_password] → detecting → parsing
       → finalizing → loading → reporting → done | partial | held
```

- `held` is reachable from `finalizing` (quality gate) and can resume to `loading`.
- Any per-stage failure exits to `failed`. Terminal states are `done`, `partial`, `held`, `failed`.
- A `FailureClass` is one of `uncertain`, `transform_error`, `type_mismatch`, `encoding_error`, `extraction_error`.

---

## 6. The 14 Key Decisions (why-nots)

Each decision is a "we didn't do X because it breaks a rule; we did Y instead."

| # | We rejected… | …because | We do instead |
|---|--------------|----------|---------------|
| 1 | Per-line AI | Unaffordable at file scale | AI teaches cached templates (Rule 1) |
| 2 | Block-level detection | Misses interleaved junk between good blocks | Ordered per-line classifier (Rule 2) |
| 3 | Success-only learning | Re-hits AI on every rubbish block | Learn **both** record and rubbish kinds |
| 4 | Greedy rubbish matching | Silently drops real records | High-confidence rubbish signatures only |
| 5 | Forcing a verdict | Drops data on ambiguity | `uncertain` is a permitted verdict |
| 6 | Executing AI-generated code | Sandbox / injection surface | Declarative templates, never executed |
| 7 | Relying on a global line number mid-parse | Unknowable while streaming | `(job_id, byte_offset)` + backfilled `line_no` |
| 8 | Streaming remote links directly | No `Range`; they expire | Copy to object store first (Rule 4) |
| 9 | Stream-parsing inside archives | Breaks range addressability | Extract at ingest, fan out per entry |
| 10 | Rubbish tried before records | Would drop ambiguous records | Records before rubbish (the asymmetry) |
| 11 | Mixing failures and rubbish in one store | Rubbish would get retried / failures lost | Separate DLQ vs rubbish-log (Rule 5) |
| 12 | Loading straight from the parser | DB outage would stall parsing | Object store is a replayable checkpoint; Load is separate + idempotent |
| 13 | Unbounded AI on match-rate collapse | Silent, runaway cost | Match-rate monitor flags the job |
| 14 | Reading whole files into memory | Fails on multi-GB inputs | Streaming pass + RAM-watermark part flush + merge at finalize |

---

## 7. Tuning Defaults, Open Questions, Out of Scope

### 7.1 Tuning defaults (tune from real jobs)
All are overridable via env in `src/shared/config.ts`.

| Concern | Setting | Default |
|---------|---------|---------|
| Fetch chunk | `FETCH_CHUNK_SIZE` | 8MB (design range 8–16MB) |
| Small-file single GET | `SMALL_FILE_SINGLE_GET_THRESHOLD` | 128MB |
| RAM flush watermark | `RAM_FLUSH_WATERMARK` | 256MB |
| Merged-part cap | `MAX_MERGED_PART_BYTES` | 64MB |
| Max line bytes | `MAX_LINE_BYTES` | 1MB |
| Probe window | `PROBE_WINDOW_MIN/MAX_BYTES`, `PROBE_TARGET_LINES` | 64KB … 1MB, 150 lines |
| Probe count | `PROBE_COUNT_MIN/MAX`, `PROBE_SIZE_PER_COUNT` | 5 … 24, 1 per 512MB |
| Rubbish confidence floor | `RUBBISH_CONFIDENCE_MIN` | 0.9 (favor keep-and-check) |
| Match-rate floor / window | `MATCH_RATE_FLOOR`, `MATCH_RATE_WINDOW` | 0.1, 1000 |
| Quality gate (failed-line ratio; rubbish EXCLUDED) | `FAILED_LINE_RATIO_THRESHOLD` | 0.05 |
| Retry schedule | `RETRY_MAX_ATTEMPTS`, `RETRY_DELAYED_DELAY_SECONDS` | 2, 300s (immediate + 1 delayed → review) |
| Archive guards | ratio / depth / size / entries | ~100:1, 1, 10GB, 10000 |

### 7.2 Open questions (design-acknowledged unknowns, decisions pending)
1. **Rubbish-signature reliability** — the hardest unknown. Can AI produce signatures tight enough not to swallow records yet loose enough to match variants? Needs real junk samples. Fallback if unreliable: "unknown line → one AI call, cache **nothing** for rubbish" for that file.
2. **Field-spec format** — names? regex? semantic? And how AI maps each format. (Code currently normalizes to a plain array of field names with an alias table for `email`/`name`/`phone`/`address`.)
3. **Missing-target-field policy** — empty/null vs skip the row, per client. (Code currently emits null.)
4. **Quality-gate ratio value** — a policy number, not a technical one.
5. **Drift / thrash threshold** (match-rate floor) — set from real data.
6. **Target DB** — locks the output format and load mechanism.
7. **Model local vs hosted** — egress policy for unknown lines sent to AI (sensitive fields leave the boundary).
8. **Human-review queue** — owner + SLA for `uncertain` / unrecoverable lines.
9. **Deployment shape** — 8 *logical* services; for a small team, start with 2–3 binaries (Job+Ingest+Detect / Parser+Retry / Load+Report, AI Classifier standalone) and split when scaling demands. **Boundaries first, deployments later.** (The repo ships a Dockerfile per service — `Dockerfile.job`, `Dockerfile.ingest`, `Dockerfile.stream`, etc. — so the split is packaging-configurable.)

### 7.3 Out of scope / routed elsewhere
- **PDF** — not line-delimited text; needs a per-page layout → line-reconstruction front-end **before** this classifier. Same downstream model, a different reader; a separate effort.
- **SQL dumps** (statement tokenizer), **JSON/NDJSON**, **XLSX** — each is its own reader sharing the same downstream machinery.
- **Deferred:** multi-hypothesis parallel reading; parallel partitions for homogeneous files; fuzzy fingerprint matching; nested archives; non-UTF-8 transcode-at-ingest.

---

*Source of truth: `parser-design-spec.md` (project knowledge base). Verified against `src/` on 2026-07-17. Where implementation lags intent — notably in-loop synchronous AI in the Stream Parser (§4.4) — this document states the intent as the contract and flags the current behavior inline.*
