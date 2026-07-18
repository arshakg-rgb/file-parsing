# Ingest Subsystem Remediation Plan

This document is the actionable implementation plan for fixing the 32 defects found in the parser ingest subsystem (services `ingest`, `archive_entry_consumer`, `ssrf_guard`, and the `reconciler` script). It turns the raw finding list into concrete, dependency-ordered change-units (CUs), names the shared components that must be built once and reused across many fixes, and calls out the hazards that make ordering non-negotiable. **Nothing here is implemented yet** — the design was verified against `main` and all 32 findings are still present. Each finding is referenced by its number, e.g. `[5]`, matching the companion findings document (`parser-ingest-findings`). If you are new to this codebase, read this top to bottom: it is written to be followed in order, and the ordering is load-bearing (deploying certain fixes early actively duplicates or drops user data).

---

## Before you start: three decisions required before deploy

These are product/ops decisions that block their respective change-units. Get answers before shipping the affected CU; the rest of the plan can proceed without them.

| # | Decision | Blocks | Why it matters |
|---|----------|--------|----------------|
| 1 | **Source-bucket allow-list.** What is the real production allow-list for caller-supplied source buckets, and are legacy `s3://` references still validated? | `[22]` / **CU5** | Defaulting `ALLOWED_SOURCE_BUCKETS` to only `DATA_BUCKET` (`datalead-osint`) will break any job that legitimately ingests from a customer bucket. We need the real list before enabling the guard. |
| 2 | **Encrypted ZIP: fix or fail honestly?** The full fix adds a `7z` binary to the container image (the same way `unrar` is bundled today). If that is out of scope now, ship the "honest failure" variant instead. | `[9]` / **CU9** | `node-stream-zip` v1 cannot decrypt, so the current `password` option is a silent no-op. The honest-failure variant detects encryption and returns a clear terminal `FAILED` rather than telling the user to supply a password that can never work. Pick one. |
| 3 | **SSRF all-address block.** Confirm it is acceptable to reject a host when **any** resolved A/AAAA record is in a private/reserved range. | `[8]` / **CU4** | Rejecting on any-private may over-block hosts with mixed public/private DNS records. Confirm the safe default is acceptable, or we need an alternative policy. |

---

## Test-infrastructure blocker — do this first, inside CU6

The unit-test suite is currently **green while production is broken**, so fixing the code without fixing the test harness leaves you flying blind. In `src/scripts/local_test.ts` §3, the test defines its **own private, correct copy** of `detectArchiveType` (a shadow function at `local_test.ts:126`) and asserts against that shadow — it never imports the real, broken exported symbol from `src/services/ingest/normalizer.ts`. That is why finding `[3]` (magic-byte detection is broken in prod) coexists with a passing test.

Do the following as part of **CU6**, before relying on any test signal:

- Delete the shadow `detectArchiveType` in `local_test.ts` and import the **real** exported `detectArchiveType` from `normalizer.ts`.
- **Export** `handleArchiveEntry` from `src/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.ts` (currently module-private at `handler.ts:127`) so retry/permanent-error logic is unit-testable.
- Guard the module-bottom call `consumerLoop()` (`handler.ts:350`) behind an `import.meta.url` main-module check so importing the module for tests does not start the consumer loop.

These three changes make findings `[4]`, `[15]`, and `[21]` unit-testable.

---

## Shared components — build once, reuse everywhere

These are the foundation. Most change-units are thin call-site edits on top of one of these components. Build and review them carefully; a bug here multiplies across every fix that depends on it.

### Child-process and extraction primitives

- **`src/shared/childRunner.ts` — `runChildToStream(cmd, args, writeStream, { signal })`**
  The backbone of every `unrar` / `7z` invocation. It resolves **only** when the child closes with code 0 **and** the write stream emits `finish` — this kills the "false success on a truncated RAR" bug (`[30]`, where the promise resolved on stream `finish` before `unrar`'s CRC exit-3). On a write error it SIGKILLs the child (no zombie `unrar` pinning deleted scratch files). It forwards an `AbortSignal` (enabling cancel-on-timeout, `[28]`), captures stderr, and distinguishes an abort from a genuine CRC failure. Feeds `[0][9][10][11][12][18][28][30]`.

- **`src/shared/secret.ts` — sole owner of `unrar` argv construction.**
  Note this is a **new** file, distinct from the existing `src/shared/secrets.ts` (which loads GCP Secret Manager values). This component becomes the *only* place that builds `unrar` argument lists. It passes the password via the child's **stdin** using a bare `-p` flag (never `-p<password>` on argv, so it stays off `ps`/`/proc`), inserts a `--` separator before operands to stop arg-injection, validates entry names, and exposes `redactArgs()` for logging. Fixes `[0]` (password leaks 4 ways) + `[12]` (arg injection); reused by `[9][11]`.

  > Today the leak is concrete: `normalizer.ts:148,519` build `'-p' + password` and `console.log` the arg array (`normalizer.ts:151`), and the consumer does the same at `archive_entry_consumer/handler.ts:80,83`. All of these route through `secret.ts` after this CU.

- **`src/shared/safeExtract.ts` — `safeWalkFiles(root)`**
  A single recursive directory walk that gates each entry with `lstat` (reads only real regular files via `isFile()`; skips symlinks, devices, FIFOs) plus a realpath-containment check. Fixes `[2]` (arbitrary host-file read via symlink entries like `data.csv -> /etc/passwd`, currently possible because `extractTarArchive`/`extract7z` `stat`/`readFile` the extracted tree with no symlink guard). Name-validation logic is shared with `[12]`.

- **`enforceRatioStreaming` + a size-capped `Transform`**
  Enforces compression ratio, total uncompressed size, and entry count **during** decompression, on the **actual** bytes streamed — not after a full inflate. Replaces the current `checkRatio()` helper (`normalizer.ts:366`) which runs only *after* full decompression. Fixes `[10]`; reused by `[6][11]`.

- **`withDownloadedTempFile` + streaming size-capped download**
  Downloads to a temp file with the size cap enforced *before* buffering, and `unlink`s in a `finally` even when the body throws. Fixes `[6]` (uncapped whole-archive buffering → OOM) + `[18]` (temp-file/handle leaks on error paths).

- **`collectEntry` — per-entry isolation**
  Wraps each ZIP/tar/7z entry so one corrupt entry is logged-and-skipped instead of failing the whole job — but it **re-throws** `BombError` and password errors so those still terminate correctly. Fixes `[24]` ("a bad file never stops a batch," which RAR already honors but zip/tar/7z do not).

- **`ModAckController` keep-alive + `runWithConcurrency(items, limit)` pool** — ship together.
  `ModAckController` extends the Pub/Sub ack deadline for both in-flight and queued messages (bounded, and stopped in a `finally`), fixing `[17]` (a 50-min extraction against a 300s deadline redelivers mid-run). `runWithConcurrency` is a real bounded worker pool over thunks, fixing `[29]` (the current `.map(async …)` starts every promise eagerly, so chunking only staggers awaits). Fixes `[17]` + `[29]`.

- **AbortSignal plumbed `withTimeout` → extract → spawn**
  Threads one `AbortSignal` from the timeout/FAILED path through extraction into `spawn`, so a timeout aborts children, destroys write streams, and stops nested extractions and keep-alives. Fixes `[28]`.

### Network and source-guard primitives

- **SSRF `resolveAndPin` + expanded IPv6 blocklist**
  `checkUrl` returns the vetted IP, and `fetch` is driven through an `undici` Agent **pinned to that exact IP**, closing the DNS-rebinding TOCTOU (`[8]`: today `checkUrl` resolves once at `ssrf_guard.ts` and `fetch()` independently re-resolves, so a host can be public at check time and `169.254.169.254` at fetch time). Also adds the missing reserved IPv6 ranges: `2001:db8::/32`, `2002::/16` (6to4), `64:ff9b::/96` (NAT64), and re-checks IPv4-mapped addresses.

- **`src/shared/sourceGuard.ts` — `assertAllowedGcsBucket`**
  A bucket allow-list applied to every caller-supplied `gs://` or `s3://` reference at each dereference point. Fixes `[22]` (today `checkUrl` returns early for `gs://` at `ssrf_guard.ts:97-100`, so `source_ref=gs://internal-bucket/secrets.json` is copied into `DATA_BUCKET` and parsed). Gated on decision #1.

### Lifecycle and idempotency primitives (the keystones)

- **`getPendingEntryCount` → `{ pending, processing, completed, failed }` (H1) — THE keystone.**
  Today `getPendingEntryCount` (`src/shared/db.ts:175`) returns only `{ pending, completed, failed }` and its SQL counts only `status = 'pending'` — a still-extracting `processing` sibling does **not** block completion, so the parent goes DONE with missing data (`[5]`). The fix adds a `processing` count and every call site must treat `processing` as "still open." This single change unblocks `[7]`, `[21]`, `[27]`, and `[31]`.

- **Fanout barrier: `entries_expected` + `fanout_complete` columns on `parse_jobs` (H3)**
  A job is DONE only when `fanout_complete && (pending + processing == 0) && (completed + failed == entries_expected)`. Fixes `[31]` (a fast async entry completing before later entries of the same RAR are registered briefly makes `pending == 0`, triggering premature DONE); reused by `[27]`. **Hazard:** pending rows are double-registered today (once by the normalizer, once by the ingest handler, reconciled via `ON CONFLICT`) — the barrier must key off the **last** registration, not the first.

- **`PermanentEntryError` + `incrementPendingEntryAttempt` + an `attempts` column**
  An `instanceof PermanentEntryError` check in the ack path, and the pending row is marked `failed` **before** the message is acked. Fixes `[4]` (the consumer hardcodes `handleArchiveEntry(payload, 1)` at `handler.ts:314`, so `MAX_RETRIES = 3` is unreachable and poison messages redeliver forever) + `[21]` (a depth-check that throws before `markProcessing`/`markFailed` acks the message but leaves the row `pending` forever).

- **`getPendingEntryTotalSize(jobId, excludeEntryName)` (H2)**
  Excludes the current entry from the `SUM`. Fixes `[15]`: today `markPendingEntryProcessing` runs *before* `getPendingEntryTotalSize`, whose SQL sums `status IN ('completed', 'processing')` (`db.ts:192`), so the current entry is counted twice and any single async entry over ~5 GB is rejected as a bomb (entry + entry > 10 GB) and loops forever.

- **Deterministic `childJobId` via `uuidv5` + `UNIQUE(parent_job_id, source_ref)` + `ON CONFLICT DO NOTHING`**
  Makes child-job creation idempotent. Fixes `[19]`: today `createChildJob` (`stateMachine.ts:114`) generates a fresh `randomUUID` (`stateMachine.ts:117`) with no unique key, so an `ENTRY_DISCOVERED` redelivery inserts a duplicate child that re-parses and re-loads the entry. This is a prerequisite for `[16]` and `[20]`.

- **`ingestGuardDecision(status, override, ageMs)`**
  Centralizes the idempotency decision: skip **all** post-`QUEUED`/`AWAITING_PASSWORD` states (kills DONE re-extraction), honor a manual override, and allow re-ingest of a stale `INGESTING` job. Fixes `[16]` (today `handleIngest` short-circuits only on `INGESTING`/`FAILED` at `handler.ts:54,59` then acks, so redelivery after DONE re-extracts and duplicates every child, and a mid-extraction crash acks the message and strands the job in `INGESTING`); reused by `[7]`.

- **`publishEvent` propagates rejection (remove the `.catch(() => null)`)**
  Today `publishEvent` (`src/shared/queueUtils.ts:263`) swallows failures with `.catch((err) => …)` returning null, so an `ENTRY_DISCOVERED` publish can fail while the entry is already marked `completed` — the child is never created and data is dropped under a "successful" job (`[20]`). The fix lets the rejection propagate; callers `await` and `nack`, marking an entry `completed` only **after** the publish resolves. **This is only safe on top of the idempotency work** (see hazards).

---

## Change-units by phase (dependency-ordered)

24 change-units across 7 phases. Within a phase, CUs are parallelizable unless a dependency is noted. **Between** phases, respect the order — later phases assume earlier ones are live.

### Phase 0 — Foundational (no behavior flips)

| CU | Fixes | Summary | Depends on |
|----|-------|---------|-----------|
| **CU1** | `[30]` | Build `src/shared/childRunner.ts` (`runChildToStream`). No behavior flip on its own; it is the substrate for the RAR/7z CUs. | — |
| **CU6** | `[3]` | Fix magic-byte constants and delete the test shadow. Replace `Buffer.from("\x1f\x8b")` / `Buffer.from("7z\xbc\xaf\x27\x1c")` (UTF-8 mis-encodes bytes ≥ 0x80) with byte arrays `Buffer.from([0x1f,0x8b])` and `Buffer.from([0x37,0x7a,0xbc,0xaf,0x27,0x1c])` in `normalizer.ts:76-78`. Includes the test-infra unblock described above. | — |

### Phase 1 — Security (parallelizable)

| CU | Fixes | Summary | Depends on |
|----|-------|---------|-----------|
| **CU2** | `[0][12]` | Build `src/shared/secret.ts`; route all `unrar` argv through it (password via stdin, `--` separator, redacted logs). | CU1 |
| **CU3** | `[2]` | Build `src/shared/safeExtract.ts` (`safeWalkFiles`); use it in `extractTarArchive`/`extract7z` so symlink/device/fifo entries are skipped and containment is enforced. | — |
| **CU4** | `[8]` | SSRF `resolveAndPin` + pinned `undici` Agent + expanded IPv6 blocklist in `src/services/ingest/ssrf_guard.ts`. | — (gated on decision #3) |
| **CU5** | `[22]` | Build `src/shared/sourceGuard.ts` (`assertAllowedGcsBucket`); call it at every `gs://`/`s3://` deref. | — (gated on decision #1) |

### Phase 2 — Broken features

| CU | Fixes | Summary | Depends on |
|----|-------|---------|-----------|
| **CU7** | `[26][14]` | Write RAR entries to `DATA_BUCKET` instead of the source archive bucket (today `entryKey = archive/${jobId}/${file.name}` is written to `bucket` at `normalizer.ts:274`, causing 403s on read-only customer buckets); and fix the `batchId` → `batch_id` mismatch (emitters send camelCase `batchId` at `normalizer.ts:384,241`; readers expect `batch_id` per `events.ts:46` and `stateMachine.ts:128`, so archive children get `batch_id = NULL`). | — |
| **CU8** | `[23]` | Re-run archive detection on `.gz`/nested/depth-1 entries instead of routing them straight to CLASSIFY (`stateMachine.ts:144`). | CU6 |
| **CU9** | `[9]` | Encrypted-ZIP: full 7z-decrypt path, or honest-failure variant per decision #2. | CU1 + CU2 (gated on decision #2) |
| **CU10** | `[1]` | Fix or remove `POST /upload`. Today the object-key has a leading-slash writer↔reader mismatch (`http_server.ts:13`), it never inserts a `parse_jobs` row (so `GET /jobs/:id` 404s forever), sets the wrong `source_type` (`"s3"`), and buffers multipart in memory with no auth or size cap. Standalone. | — |

### Phase 3 — Lifecycle keystones (strict order within the phase)

| Order | CU | Fixes | Summary | Depends on |
|-------|----|-------|---------|-----------|
| 1st | **CU11** | `[5]` | H1: `getPendingEntryCount` returns and counts `processing`; every call site treats `processing` as open. **Must land first in this phase.** | — |
| with CU11 | **CU12** | `[15]` | H2: `getPendingEntryTotalSize(jobId, excludeEntryName)` excludes the current entry from the `SUM`. | rides with CU11 |
| after CU11 | **CU13** | `[4][21]` | Retry ceiling + `PermanentEntryError`: pass the real attempt count (not hardcoded `1`) and mark the row `failed` before ack. | CU11 |
| after CU11 | **CU14** | `[27][31]` | Async-route failure handling + fanout barrier (`entries_expected` + `fanout_complete`): don't leave a `pending` row with no queue message when `sendRaw` throws, and don't publish DONE mid-fanout. | CU11 |
| last | **CU15** | `[7]` | Reconciler: fix ESM startup (replace `require.main === module` at `reconciler.ts:100`, which throws `ReferenceError` under ESM, with an `import.meta.url` check), schedule it (Dockerfile + cloudbuild + Cloud Scheduler), and adopt the H1 `processing` count. **Last in this phase.** | CU11 |

### Phase 4 — Idempotency (redelivery-safe order)

Ship strictly in this order: **CU18 → CU16 → CU17 → CU19.**

| Order | CU | Fixes | Summary | Depends on |
|-------|----|-------|---------|-----------|
| 1st | **CU18** | `[19]` | Idempotent child creation: deterministic `uuidv5` `childJobId`, `UNIQUE(parent_job_id, source_ref)`, `ON CONFLICT DO NOTHING`. Migration must de-dup existing children before adding the UNIQUE index. | — |
| 2nd | **CU16** | `[13]` | Password-resume transition + TTL: allow `AWAITING_PASSWORD → INGESTING` in `VALID_TRANSITIONS` (today `VALID_TRANSITIONS[AWAITING_PASSWORD] = [DETECTING, FAILED]` at `models/job.ts:33`, so the post-password `ingesting` transition throws), add a TTL, and stop relying on per-process attempt/cache maps. | — |
| 3rd | **CU17** | `[16]` | Idempotent ingest guard: `ingestGuardDecision(status, override, ageMs)` at the ingest entry point. | CU18 |
| 4th | **CU19** | `[20]` | `publishEvent` fails loud (remove `.catch(() => null)`); callers `await` + `nack`; mark `completed` only after publish resolves. | CU18 + CU17 (+ CU22 lease) |

### Phase 5 — Resource safety / OOM

| CU | Fixes | Summary | Depends on |
|----|-------|---------|-----------|
| **CU20** | `[6][10][11]` | Streaming intake + guards enforced **inside** decompression, with `[18]` leak cleanup folded in: cap before buffering, use `withDownloadedTempFile`, and enforce ratio/size/entry-count on actual streamed bytes (RAR included — today streaming RAR never calls `checkRatio` and trusts attacker-declared listing sizes at `normalizer.ts:231-266`). | CU1 |
| **CU21** | `[18]` | Temp-file/handle leak cleanup — folded into CU20 (download-before-`try/finally` and straight-line cleanups become `finally`). | with CU20 |
| **CU22** | `[17][29]` | `ModAckController` keep-alive lease + `runWithConcurrency` bounded pool — ship together. | — |
| **CU23** | `[28]` | Abort-on-timeout: plumb `AbortSignal` through `withTimeout` → extract → spawn so a FAILED/timeout stops children, write streams, nested extractions, and keep-alives. | CU1 + CU22 |

### Phase 6 — Architecture

| CU | Fixes | Summary | Depends on |
|----|-------|---------|-----------|
| **CU24** | `[25]` | Make the Job Service the sole owner of `parse_jobs` writes and DONE decisions (today `ingest/handler.ts:79` UPDATEs `s3_url`/size and `archive_entry_consumer` reads `parse_jobs` to decide DONE, racing the Job Service). This is an event-contract change across 3 services — do it **last**. Note `src/shared/qualityGate.ts:76` has the same direct-write violation (an `UPDATE parse_jobs SET status = 'held' …`) — track it as a follow-up. | everything prior |

---

## Hazards — read before deploying anything out of order

The ordering above is not a preference; violating it duplicates or drops user data. The specific traps:

- **Do not flip CU19 (`publishEvent` fails loud) before CU18 + CU17 + CU22 are live.** Without idempotent child creation, an idempotent ingest guard, and the ack lease, making publish loud means redelivery re-does work and duplicates children/entries. CU19 is the *last* idempotency step for a reason.
- **Do not deploy CU15 (reconciler) before CU11 (H1 processing-count).** The reconciler is the safety net; if it runs while still blind to `processing` entries, it will re-commit the exact premature-DONE it is supposed to catch.
- **CU18's migration must de-dup existing `parse_jobs` children before adding the `UNIQUE(parent_job_id, source_ref)` index**, or the index creation fails on existing duplicate rows. (Compare the pattern in `src/db/migrations/003_add_unique_constraint_pending_entries.sql`, which `DELETE`s duplicates before `ALTER TABLE … ADD CONSTRAINT`. Existing migrations go up to `004`, so new migrations start at `005`.)
- **Pending rows are double-registered today** (once by the normalizer, once by the ingest handler, reconciled via `ON CONFLICT`). The H3 fanout barrier in CU14 must key off the **last** registration, not the first, or `entries_expected` will be computed before all entries are known and the barrier will open early.

---

## Reference: names and locations you will touch

**Services / files**

| Path | Role |
|------|------|
| `src/services/ingest/IngestServiceHandler.ts` | Ingest message handler; idempotency guard, `_passwordCache` (`:19`), status transitions |
| `src/services/ingest/normalizer.ts` | Archive detection + extraction (RAR/zip/gz/tar/7z), magic bytes (`:76-78`), `unrar` argv |
| `src/services/ingest/http_server.ts` | `POST /upload` + health endpoint |
| `src/services/ingest/ssrf_guard.ts` | `checkUrl` / `fetchUrlStream`, IP blocklist |
| `src/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.ts` | Async per-entry consumer; `handleArchiveEntry` (`:127`), `consumerLoop` (`:283`, invoked `:350`) |
| `src/services/job_service/stateMachine.ts` | `createChildJob` (`:114`), event handling |
| `src/shared/db.ts` | `getPendingEntryCount` (`:175`), `getPendingEntryTotalSize` (`:188`), pending-entry mutators |
| `src/shared/queueUtils.ts` | `publishEvent` (`:263`), `sendRaw` (`:242`), `pubReceive` (`:120`) |
| `src/shared/models/job.ts` | `JobStatus` enum, `VALID_TRANSITIONS` (`:30`) |
| `src/shared/models/events.ts` | Event contract, `batch_id` field (`:46,56`) |
| `src/shared/qualityGate.ts` | Direct `parse_jobs` write follow-up (`:76`) |
| `src/scripts/reconciler.ts` | Stuck-job reconciler; ESM startup bug (`:100`) |
| `src/scripts/local_test.ts` | Unit tests; shadow `detectArchiveType` (`:126`) to delete |
| `src/db/migrations/` | SQL migrations; latest is `004`, new work starts at `005` |

**New shared components to create**

`src/shared/childRunner.ts`, `src/shared/secret.ts` (distinct from existing `src/shared/secrets.ts`), `src/shared/safeExtract.ts`, `src/shared/sourceGuard.ts`.

**Queues** (Pub/Sub, defaults from `src/shared/config.ts`)

`fpp-ingest` (`INGEST_QUEUE_URL`), `fpp-classify` (`CLASSIFY_QUEUE_URL`), `fpp-parse` (`PARSE_QUEUE_URL`), `fpp-load` (`LOAD_QUEUE_URL`), `fpp-report` (`REPORT_QUEUE_URL`), `fpp-job-events` (`JOB_EVENTS_QUEUE_URL`), `fpp-archive-entry` (`ARCHIVE_ENTRY_QUEUE_URL`), `fpp-line-dlq` (`DLQ_QUEUE_URL`).

**GCS buckets / tables**

`DATA_BUCKET` default `datalead-osint` (extraction target for all entries after CU7). Table `parse_jobs` (jobs + archive children). Table `pending_archive_entries` (per-entry rows with `status IN ('pending','processing','completed','failed')`; `UNIQUE(job_id, entry_name)` from migration 003).

**Config knobs of interest** (`src/shared/config.ts`)

`ARCHIVE_MAX_COMPRESSION_RATIO` (100), `ARCHIVE_MAX_NESTING_DEPTH` (1), `ARCHIVE_MAX_UNCOMPRESSED_BYTES`, `ARCHIVE_MAX_ENTRIES` (10000), `ARCHIVE_PASSWORD_MAX_ATTEMPTS` (3). New: `ALLOWED_SOURCE_BUCKETS` (CU5, decision #1).

---

## Finding → change-unit index

| Finding | CU | | Finding | CU | | Finding | CU |
|---------|-----|-|---------|-----|-|---------|-----|
| `[0]` | CU2 | | `[9]` | CU9 | | `[21]` | CU13 |
| `[1]` | CU10 | | `[10]` | CU20 | | `[22]` | CU5 |
| `[2]` | CU3 | | `[11]` | CU20 | | `[23]` | CU8 |
| `[3]` | CU6 | | `[12]` | CU2 | | `[24]` | (shared `collectEntry`, in CU20) |
| `[4]` | CU13 | | `[13]` | CU16 | | `[25]` | CU24 |
| `[5]` | CU11 | | `[14]` | CU7 | | `[26]` | CU7 |
| `[6]` | CU20 | | `[15]` | CU12 | | `[27]` | CU14 |
| `[7]` | CU15 | | `[16]` | CU17 | | `[28]` | CU23 |
| `[8]` | CU4 | | `[17]` | CU22 | | `[29]` | CU22 |
| | | | `[18]` | CU21 | | `[30]` | CU1 |
| | | | `[19]` | CU18 | | `[31]` | CU14 |
| | | | `[20]` | CU19 | | | |

_Design produced 2026-07-17, verified against `main` (all 32 findings still present, none fixed). See also: `parser-ingest-findings`, `parser-design-spec`, `parser-design-vs-implementation`._
