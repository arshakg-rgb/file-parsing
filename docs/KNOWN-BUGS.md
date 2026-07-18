# Parser — Known Bugs, Dead Code, and the 2026-07-17 Firefight

This document is the onboarding map to what is broken, what is dead, and what is fragile in the parser pipeline. It merges the project knowledge base (the `parser-known-bugs` and `parser-firefight-2026-07-17` memory notes, both captured on 2026-07-17) with a spot-verification pass against the code at the current `main` HEAD (`81ab663`). Read it top to bottom before you touch `stream_parser`, the DLQ, or the flush path. Every claim below carries a concrete file path and identifier so you can re-verify; where the code has moved on since the KB was written, the item is annotated **Resolved** or **Changed** with what actually changed. Nothing here is invented — if a claim could not be pinned to code it is called out as needing re-verification.

> **Ground truth caveat:** the KB snapshot describes the repo "as found on 2026-07-17." Several commits landed afterward (`de5299b`, `0d11495`, `4463d80`, and the CI commits `4091092`…`81ab663`). Those are folded in below. When in doubt, trust the code and update this file.

---

## 1. Pipeline shape (one paragraph of orientation)

A job flows through single-purpose Cloud Run services, each draining its own queue and publishing events to the shared `JOB_EVENTS_QUEUE_URL`: **ingest** (`src/services/ingest/`) → **detect_bootstrap** → **stream_parser** (`src/services/stream_parser/StreamParserServiceHandler.ts`, the heart of the system) → **load** → **report**, with **job_service** (`src/services/job_service/`) owning the state machine and **retry** (`src/services/retry/RetryServiceHandler.ts`) draining the DLQ out-of-band. The stream parser reads a file from GCS line by line, classifies each line, writes parsed rows to Parquet parts in the `DATA_BUCKET`, drops junk to a rubbish log, and dead-letters anything uncertain into the `dead_letters` Postgres table plus `DLQ_QUEUE_URL`. The tables that matter: `parse_jobs`, `dead_letters`, `output_parts`, and the trace/`parsed_records` store.

---

## 2. The 2026-07-17 parquet-flush / DLQ firefight

Between **11:19 and 12:24 on 2026-07-17**, one firefighting session produced eight commits that fixed *self-inflicted* bugs in `stream_parser`'s flush and DLQ paths. The bugs were introduced earlier that same morning when the flush trigger was changed from a RAM watermark to a fixed line count. This is the single most important section: the invariants below were paid for in a data-loss incident and must not be reintroduced.

### 2.1 The live writer moved — know which file you are editing

The KB (written mid-firefight) still calls `src/services/stream_parser/ParquetWriterPool.ts` the live writer. **That is no longer true.** The live parse loop now imports `OutputManager` from **`src/shared/parquetWriter.ts`** (see `handler.ts:8`), and the class that actually buffers and flushes rows is **`OutputBuffer`** there. The old `ParquetWriterPool` in `src/services/stream_parser/ParquetWriterPool.ts` is dead (see §4). All firefight fixes below live in `src/shared/parquetWriter.ts`.

### 2.2 The three flush bugs and their fixes

| # | Commit | Time | Bug | Fix |
|---|--------|------|-----|-----|
| 1 | `6cc00e4` | 11:32 | Changed flush trigger from RAM watermark to "exactly 1000 lines" but left `addRow()` calling `this.flush()` un-awaited → opened two races (2 and 3 below). | (the enabling change; races fixed in 2 & 3) |
| 2 | `2a495d4` | 12:19 | **ENOENT race.** Concurrent flushes shared one temp path (`os.tmpdir()/<partId>.parquet`) and one GCS key. The first flush's `fs.unlink` deleted the file out from under the second. | `flushPromise` single-flight guard (only one threshold flush in flight per buffer) + a `flushCounter` that makes every flush's `partId` unique. |
| 3 | `27df55d` | 12:24 | **Critical data loss.** `flush()` cleared `this.rows` *after* the async GCS upload, so any row appended during the upload window was wiped without ever being written. | Snapshot `rowsToFlush` and reset `this.rows = []` **synchronously before any `await`**; `flushAll()` calls `waitForPendingFlush()` per buffer before its final flush. |

**Verified in code** (`src/shared/parquetWriter.ts`):

- `OutputBuffer.addRow` (line 58) triggers a background flush only when `this.rows.length >= this.FLUSH_LINE_THRESHOLD` (=1000) **and** `!this.flushPromise` — the single-flight guard.
- `OutputBuffer.flush` (line 70) does the snapshot-before-await correctly:
  ```ts
  const rowsToFlush = this.rows;
  this.rows = [];                                  // synchronous — safe for concurrent addRow()
  const flushPartId = `${this.partId}-${this.flushCounter++}`;   // unique per flush
  // ... only now do we await ParquetWriter / GCS upload
  ```
- `OutputManager.flushAll` (line 139) awaits `buffer.waitForPendingFlush()` before the terminal `buffer.flush()`.

### 2.3 The DLQ duplication arc

Cloud Run rollouts restart a running parse job **from line 1**, which used to re-insert every dead-letter row → duplicate `dead_letters` entries.

| Commit | Time | What it did |
|--------|------|-------------|
| `a7985c0` | 11:44 | Added throwaway `classification_debug` logging for the first 5 lines to diagnose the issue. **Not a fix.** |
| `cdddf75` | 11:51 | The real fix: **migration 004** adds `UNIQUE (job_id, line_no)` on `dead_letters`, and `DLQManager.addEntry` now does `INSERT … ON CONFLICT (job_id, line_no) DO NOTHING RETURNING dlq_id`. A returned empty row set means "duplicate." |
| `8c9b46b` | 11:58 | Fixed the TypeScript errors `cdddf75` shipped with — it had been committed without compiling. |

**Verified in code:**

- `src/db/migrations/004_dlq_unique_constraint.sql` adds `CONSTRAINT dead_letters_job_id_line_no_key UNIQUE (job_id, line_no)`.
- `DLQManager.addEntry` (`src/shared/dlqManager.ts:35`) uses `ON CONFLICT (job_id, line_no) DO NOTHING RETURNING dlq_id`, logs `dlq_entry_duplicate_skipped`, and **returns `null` on duplicate** (line 59).
- The caller respects it: in `handler.ts` the `uncertain` branch (line 314) only bumps `counts.failed_by_class[...]` when `if (dlqId)` is truthy (line 323) — duplicates do not inflate counters. This is the third invariant, honored.

### 2.4 Invariants — DO NOT reintroduce

1. **Never put an `await` before the rows snapshot/clear in `OutputBuffer.flush()`.** The snapshot (`rowsToFlush = this.rows; this.rows = [];`) must be the first synchronous act.
2. **Keep the `flushPromise` single-flight guard and the unique-per-flush `partId`** (`flushCounter`). Two concurrent flushes must never share a temp path or GCS key.
3. **Treat `DLQManager.addEntry` returning `null` as "duplicate — skip counters."** Never count a `null` as a new dead-letter.

### 2.5 Leftover risks still living in these files

These were known-open at the end of the firefight and remain true in the code today:

- **Failed flush loses its snapshot.** If `OutputBuffer.flush()` throws after the snapshot, the ≤1000 snapshotted rows are gone — there is no re-queue. (The dead `ParquetWriterPool` actually *did* re-queue failed rows; the live `OutputBuffer` does not.)
- **Fire-and-forget background flush has no `.catch`.** In `addRow` the background `this.flush()` is stored in `this.flushPromise` with only a `.finally`. A rejected background flush is an **unhandled promise rejection** that can crash the consumer.
- **Background-flush GCS paths are discarded.** `addRow`'s threshold flush returns its `gcsPath` into the void; only `flushAll()`'s return values populate `part_s3_paths`. For any job with >1000 rows per template, `part_s3_paths` in `PARSING_COMPLETED` misses most parts. (Same root cause as the live bug in §3.)
- **The `a7985c0` debug logging was never reverted.** `console.log("classification_debug", …)` is still at `handler.ts:239`.

---

## 3. Live functional bugs

Status legend: **OPEN** = still present at HEAD; **Resolved** / **Partially resolved** / **Changed** = the code moved since the KB snapshot (details given).

### 3.1 OPEN bugs (verified present)

| Bug | Where | Detail |
|-----|-------|--------|
| **`PARSING_COMPLETED` always reports `dlq_count: 0`** | `handler.ts:355` | The event sets `dlq_count: counts.dlq_count \|\| 0`, but nothing in the parse loop ever assigns `counts.dlq_count` (the loop increments `counts.failed_by_class[...]`, never `counts.dlq_count`). Downstream always sees zero. |
| **`rubbish_log_path` is `undefined` in `PARSING_COMPLETED`** | `handler.ts:356` | Same shape: `rubbish_log_path: counts.rubbish_log_path`, never set. The report/finalize consumers run on empty data. |
| **`part_s3_paths` misses background-flush parts** | `handler.ts:336,354` + `src/shared/parquetWriter.ts` | Only `flushAll()` return values reach `part_s3_paths`. The 1000-row threshold flushes in `addRow` discard their paths. Any template with >1000 rows loses most of its parts from the event. See §2.5. |
| **Quality gate is a no-op** | `src/shared/qualityGate.ts:32-35` | `calculateMetrics` reads `SELECT counts FROM parse_jobs`, then computes `counts.parsed + counts.dropped_rubbish + counts.failed` and `counts.failed / totalLines`. But the `JobCounts` model (`src/shared/models/job.ts:52`) has **no scalar `failed` field** — failures live under `failed_by_class`. So `counts.failed` is `undefined`, the ratio is `NaN`/`0`, and the gate at `handler.ts:342` effectively always passes. (Wrong field name **and** it reads counts that the parse loop may not have persisted yet.) |
| **`uncertain` never reaches the AI classifier** | `src/services/retry/RetryServiceHandler.ts:52-54` | `uncertain` is the parser's default DLQ verdict (`FailureClass.UNCERTAIN`, set at `handler.ts:312`). In the retry service, the branch `else if (msg.failure_class === FailureClass.UNCERTAIN) { await markForReview(msg); return; }` routes it **straight to human review** — it never calls `classifyWithAI`. AI recovery only runs for `type_mismatch` (`retryBroadCoercion`) and `transform_error`/`extraction_error` (`retryAfterTemplateUpdate`). |
| **`type_mismatch` "recovery" can load fabricated rows** | `src/services/retry/RetryServiceHandler.ts:95-107` (`retryBroadCoercion`) | The broad-coercion retry path can emit a "parsed" row for a `type_mismatch` line; per the KB this can load all-NULL fabricated rows into output. *Mechanism not fully re-verified line-by-line in this pass — treat as a real risk in the `type_mismatch` retry path and verify before relying on coerced output.* |
| **`output_parts` is never populated by live code** | `src/shared/parquetWriter.ts` vs `src/services/report/ReportServiceHandler.ts:101` | Only the **dead** `ParquetWriterPool` (`src/services/stream_parser/ParquetWriterPool.ts:157`) INSERTs into `output_parts`. The live `OutputBuffer` does not. `report/handler.ts` reads `SELECT * FROM output_parts WHERE job_id = $1` → the report's `output_parts` section (line 70) comes back empty. |
| **Per-batch Parquet schema divergence** | `src/shared/parquetWriter.ts:30` (`buildSchema`) + `28` (`typeForValue`) | Schema is inferred per ≤1000-row batch from first-seen values. `typeForValue` returns `INT64` for integers, `DOUBLE` for non-integers, `UTF8` for null/string. The **same template** can therefore emit parts whose columns have different physical types across batches, breaking downstream readers that expect a stable schema. |
| **`http_server /upload` key/ref mismatch, wrong source_type, no job row** | `src/services/ingest/http_server.ts:35,47,48` | Uploads the object to `gcsKey = ${key}/${filename}` (line 35) but publishes `source_ref: gs://${DATA_BUCKET}/${filename}` (line 48) — **missing the `${key}` prefix**, so the ref points at a path the file was not written to. It also sends `source_type: "s3"` (line 47) on a GCS pipeline, and never INSERTs a `parse_jobs` row. |
| **`archive_entry_consumer` retry ceiling is unreachable** | `src/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.ts:314,255` | `handleArchiveEntry(payload, 1)` is the **only** call site and hard-codes `attempt = 1`. With `MAX_RETRIES = 3`, the guard `if (attempt >= MAX_RETRIES)` (line 255) is never true, so a permanent extraction failure re-throws → Pub/Sub redelivers → `attempt` is `1` again → **forever**. |
| **`emitRecovered` marks DLQ rows recovered before the send** | `src/services/retry/RetryServiceHandler.ts:147-149` | `emitRecovered` calls `updateDeadLetterStatus(msg.dlq_id, "recovered")` (line 148) *before* building and sending the `LoadMessage` (line 149+). A crash in that window marks the row recovered but never loads it → the row is lost. |
| **`finalize` reads job data into memory** | `src/services/job_service/FinalizationService.ts` | Per the KB, `finalize` reads the entire source file and all part rows into memory; a multi-GB source can OOM `job_service` (512Mi limit). *In this pass, `finalize.ts` was confirmed to load `dead_letters` rows into memory (`SELECT … FROM dead_letters`, lines 227-255); the whole-source-file read is per the KB and should be re-verified against the current `finalize.ts` before relying on it.* |
| **`reconciler.ts` crashes on launch under ESM** | `src/scripts/reconciler.ts:100` | `if (require.main === module)` is used as the entrypoint guard, but `package.json` has `"type": "module"` (line 5). `require` is undefined in ESM → the script throws on launch. Use `import.meta.url` instead. |

### 3.2 Resolved / changed since the KB snapshot

| KB item | Status | Evidence |
|---------|--------|----------|
| **Exotic encodings crash the detect probe (uncatchable `ERR_UNKNOWN_ENCODING`)** | **Resolved** | Commit `0d11495` ("Fix encoding detection: stop ERR_UNKNOWN_ENCODING crashes and UTF-8 misdetection") plus `355571a`/`821a1f0` added `normalizeEncoding`/`isLikelyUtf8` (`src/shared/encoding.js`), now used in the probe loop (`handler.ts:171-178`), and each probe is wrapped in try/catch (`handler.ts:188`). |
| **DLQ duplicate rows on job restart** | **Resolved** | Fixed during the firefight — see §2.3 (migration 004 + `ON CONFLICT … RETURNING`, callers respect the `null` return). |
| **Ordered classifier flow / in-loop AI concern** | **Changed** | Commit `de5299b` ("Make stream_parser follow the designed ordered classifier flow") reworked classification. In-loop synchronous AI is now **intentionally not done** (see the comment block at `handler.ts:233-236`); AI recovery is delegated to the out-of-band retry service. This does **not** fix the `uncertain`-never-reaches-AI bug above — `uncertain` still short-circuits to review. |

---

## 4. Dead-code map (do not extend; deletion candidates)

All of the following were confirmed to still exist on disk and to have no live import path, unless noted. Do not build on them.

| Path / identifier | Why it's dead | Verified |
|-------------------|---------------|----------|
| `src/services/stream_parser/ParquetWriterPool.ts` (`ParquetWriterPool`) | The diverged twin of the live writer. Only self-referenced; the live loop uses `src/shared/parquetWriter.ts` (`OutputManager`/`OutputBuffer`). It is also the *only* code that writes `output_parts`. | `grep` for `ParquetWriterPool` / `stream_parser/parquetWriter` returns only the file itself. |
| `src/services/stream_parser/MatchRateMonitor.ts` + registry match-rate tracking | The AI-cost kill switch was designed but never wired: `templateRegistry.ts` computes `getMatchRate()` / `hasMatchRateCollapsed()`, but the only consumer of `hasMatchRateCollapsed()` is the **dead** `src/shared/classifier.ts:110`. Nothing in the live path ever acts on a collapsed match rate. | `grep` shows `hasMatchRateCollapsed` consumed only by `shared/classifier.ts`, which is imported nowhere. |
| `AIRateLimiter` (`handler.ts:58`) + `aiRateLimiter` instance | Constructed at `handler.ts:102` but **`.acquire()` is never called anywhere**. | `grep -rn "\.acquire()" src/` → no hits. |
| `src/shared/classifier.ts` | Not imported by any live module. | `grep -rln "from.*shared/classifier" src/` → no hits. |
| `src/shared/cloudwatch.ts` | AWS-era leftover; dead. | File exists, unreferenced in the GCP pipeline. |
| `src/shared/secrets.ts` | Dead per KB. | File exists. |
| `@aws-sdk/client-dynamodb` | Unused dependency (AWS era). | KB. |
| `extractRar()` in `src/services/ingest/normalizer.ts` (line 499) | Dead alternate RAR path; the live RAR handling is the `spawn('unrar', …)` block earlier in the file (~lines 152-294). | `extractRar` defined at line 499; distinct from the active path. |
| `ANTHROPIC_*` / `AI_CLASSIFIER_URL` config keys | Dead config from an earlier AI-classifier design. | KB. |
| Stale `unrar-async` declaration in `src/types.d.ts` | Orphan type decl. | KB. |
| `DLQManager.retryEntry` / `DLQManager.batchRetryJob` (`src/shared/dlqManager.ts:83,176`) | Stubs. `retryEntry` fetches the line and returns `true` but only comments "In production, this would publish to retry queue" — it does not. `batchRetryJob` loops over those stubs. | Confirmed in `dlqManager.ts`. |

---

## 5. Fragile / surprising behavior

These are not outright bugs but will bite you if you don't know them.

- **`held` is in `TERMINAL_STATUSES` yet legally transitions to `loading`.** In `src/shared/models/job.ts`, `HELD` appears in `TERMINAL_STATUSES` (line 48) but `VALID_TRANSITIONS[HELD] = [LOADING]` (line 41). Code that treats "terminal" as "no further transitions" will be wrong about held jobs.
- **`publishEvent` swallows failures.** `src/shared/queueUtils.ts:264` — `publishEvent` ends in `.catch((err) => …)` and returns `null` on failure. A failed publish silently strands the job. Recovery path: `GET /v1/jobs/stuck` + the `reconciler` + `POST /jobs/:id/retry` (and note the reconciler itself is currently broken under ESM — §3.1).
- **Consumer loops start at module import.** `consumerLoop()` is invoked at the bottom of `stream_parser/handler.ts` (line 424). Importing a handler in a test starts an infinite poller. Guard imports in tests.
- **Two template registries and two fingerprint schemes.** A Postgres-backed and a Firestore-backed registry coexist, with two fingerprint schemes that (per KB) never match real lines. Know which one your service actually loads (`templateRegistry.loadFromDatabase()` in the parser).
- **`SYSTEM_PROMPT` is never sent to Vertex AI.** In `src/services/ai_classifier/AiClassifierServiceHandler.ts`, `SYSTEM_PROMPT` is defined (line 62) but `callVertexAI` (line 66) calls `askVertexAI(prompt)` with only the user prompt — the system prompt is never included. The `extractJson` leniency (line 119) papers over the resulting sloppier output.
- **Health endpoints are unconditional `200`, all services `--allow-unauthenticated`.** Health checks never reflect real liveness; every service is publicly reachable.
- **`transition()` builds its `SET` clause from object keys.** Safe today because keys are code-controlled, but it becomes **SQL injection** the moment it is handed user-controlled keys.
- **Every parsed line does a synchronous `parsed_records` INSERT in the hot loop.** Via `TraceSystem.createTrace` (awaited per row at `handler.ts:266`). This is a throughput ceiling and a per-row failure surface (each INSERT is wrapped in try/catch that converts a trace failure into a `dropped_rubbish`).

---

## 6. Quick "don't do this" checklist for new engineers

- Don't edit `src/services/stream_parser/ParquetWriterPool.ts` thinking it's live — the live writer is `src/shared/parquetWriter.ts`.
- Don't add an `await` before the snapshot/clear in `OutputBuffer.flush()`.
- Don't count a `null` return from `DLQManager.addEntry` as a new dead-letter.
- Don't assume `output_parts`, `dlq_count`, or `rubbish_log_path` are populated — they currently aren't (§3.1).
- Don't rely on the quality gate to actually reject anything (§3.1).
- Don't expect `uncertain` DLQ lines to get AI recovery — they go to human review (§3.1).
- Don't launch `reconciler.ts` expecting it to run under the current ESM config (§3.1).

---

*Sources: `parser-known-bugs` and `parser-firefight-2026-07-17` memory notes (2026-07-17), spot-verified against `main` @ `81ab663`. Where the code diverged from the KB, the code won and the item is annotated accordingly.*
