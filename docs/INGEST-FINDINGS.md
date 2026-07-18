# Parser Ingest Subsystem — Adversarially-Verified Defect Findings

> **Read-only review findings, dated 2026-07-17.** These 32 defects were found during a deep,
> five-lens review of the ingest subsystem versus the parser design spec, then hardened with a
> two-skeptic adversarial verification pass (32 confirmed, 0 refuted). **Nothing here has been
> fixed** — this is a review record, not a changelog. Line numbers reflect the code state on the
> review date and *will* have drifted; the anchors in this document were spot-re-verified against
> the working tree and several had already moved by a few lines. **Always re-confirm each
> `file:line` anchor before acting on it.** File paths are relative to the repository root
> (`/Users/user/Projects/parser`).

## Introduction

This document is a map of everything currently wrong with the parser's *ingest path* — the code
that pulls a source file in (from a URL, S3/GCS object, or an HTTP upload), normalizes it, and, if
it is an archive (ZIP / TAR / 7z / RAR / gzip), expands it into child entries that flow downstream
to classification, parsing, loading, and reporting. The review covered five subsystems:
`src/services/ingest/` (the `handler.ts`, `normalizer.ts`, `http_server.ts`, and `ssrf_guard.ts`),
the `src/services/archive_entry_consumer/` worker that extracts individual archive entries
asynchronously, the `src/scripts/reconciler.ts` stuck-job sweeper, and the shared plumbing they
lean on (`src/shared/db.ts`, `src/shared/queueUtils.ts`, `src/shared/models/job.ts`,
`src/shared/models/events.ts`, and the Job Service state machine in
`src/services/job_service/stateMachine.ts`). Findings are grouped A–E by theme, ordered by ID
within each group, and every entry carries a severity, a concrete `file:line` anchor, and a failure
scenario written so a new engineer can see exactly how it goes wrong. The counts: **7 critical, 14
high, 11 medium.**

### How to read a finding

Each finding is labeled with its stable review ID (the number in brackets, e.g. `[0]`), which is
how it is referenced in the suggested fix order and in related knowledge-base notes. The severity
ladder is:

- **CRITICAL** — data loss, security breach, or a job class that is broken end-to-end / stuck forever.
- **HIGH** — silent data drops, duplication, resource exhaustion, or a feature that is a no-op.
- **MEDIUM** — narrower correctness/design gaps that still violate a spec guarantee.

### Severity summary

| Group | Theme | Critical | High | Medium |
|-------|-------|:---:|:---:|:---:|
| A | Secrets & injection (security) | 2 | 2 | 1 |
| B | Broken features / silent data loss | 2 | 2 | 2 |
| C | Async flow / stuck jobs / duplication | 3 | 6 | 2 |
| D | Resource safety / OOM / leaks | 1 | 4 | 3 |
| E | Design conformance | 0 | 0 | 2 |
| **Total** | | **8*** | **14** | **10** |

> \* Group A/B/C/D critical counts sum to 8 because ID `[1]` (POST `/upload`) is both a broken
> feature and, taken end-to-end, critical; the headline project count is **7 critical / 14 high /
> 11 medium = 32**. The per-row tallies above are for orientation, not authority — trust the
> individual findings.

---

## A. Secrets & injection (security)

Archive passwords and untrusted entry names are the two attack surfaces here. Passwords are meant
to be transient and never logged; entry names come straight from an attacker-controlled archive.

### [0] CRITICAL — Archive passwords leak four different ways

**Anchors:** `src/services/ingest/normalizer.ts:148,280,519,561`;
`src/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.ts:80,83`; `src/services/ingest/normalizer.ts:151`;
`src/services/job_service/JobServiceRouter.ts:150-154`; `src/services/ingest/IngestServiceHandler.ts:19` (module-global
`_passwordCache`); `src/services/ingest/normalizer.ts:246`.

The RAR path shells out to `unrar` and passes the password as a single `'-p' + password` argv
token. That token is visible to any process on the box via `ps`. Concretely, in `normalizer.ts`
the list step does `listArgs.push('-p' + password)` (line ~148) and the extract step does
`extractArgs.push('-p' + password)` (line ~280); the same pattern repeats in the encrypted-ZIP
sniff at ~519/561 and in `archive_entry_consumer/handler.ts` at ~80. Four distinct leaks:

1. **`ps` visibility** — the password rides in `argv` to `spawn('unrar', ...)`.
2. **Plaintext logs** — `archive_entry_consumer/handler.ts:83` logs `extract_args` (which contains
   `-p<password>`) via `logger.info("archive_entry_extract_start", { extract_args })`, and
   `normalizer.ts:151` logs `listArgs` via `console.log("rar_list_starting", { args: listArgs })`.
3. **Persisted in queue/DLQ bodies** — the password is written into message payloads
   (`job_service/router.ts:150-154`, `ingest/handler.ts`, `normalizer.ts:246` sets
   `password: password || undefined` on the async entry message), so it lands in queue bodies and,
   on failure, the dead-letter queue.
4. **Cached forever in memory** — `const _passwordCache = new Map<string, Buffer>()` at
   `ingest/handler.ts:19` is a module global that is set on receipt and read on resume but **never
   evicted**.

**Contract violated:** passwords must be transient and never logged.
**Failure scenario:** a customer submits an encrypted RAR with a real password; that password is
now readable in `ps` output, in structured logs, in the queue body, in any DLQ entry the job
produces, and lives in process memory for the container's lifetime.

### [2] CRITICAL — Arbitrary host-file read via symlink entries

**Anchors:** `src/services/ingest/normalizer.ts:436` (`extractTarArchive`), `:452-454` (stat/read),
`:466` (`extract7z`), `:485-487` (stat/read).

`extractTarArchive` and `extract7z` extract the whole archive to a temp tree, then walk it doing
`await fs.stat(fpath)` and `await fs.readFile(fpath)` (lines ~452/454 and ~485/487) with **no
symlink guard**. A malicious archive can contain an entry `data.csv` that is actually a symlink to
`/etc/passwd` (or the mounted service-account key, or `/proc/self/environ`). When the walker reads
that path it follows the link and uploads the target's contents to `DATA_BUCKET`, and returns them
as "parsed" data to the caller.
**Failure scenario:** attacker crafts a TAR whose `secrets.csv` symlinks to the SA key file;
extraction reads and exfiltrates the key into a user-readable artifact.

### [12] HIGH — `unrar` argument injection via entry names

**Anchors:** `src/services/ingest/normalizer.ts:278`;
`src/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.ts:78`.

Attacker-controlled entry names are passed positionally to `unrar` with **no `--` end-of-options
separator** (e.g. `spawn('unrar', ['p','-inul', tmpPath, entryName])`). An entry whose name begins
with `-` is parsed by `unrar` as a switch rather than a filename.
**Failure scenario:** an entry named `-x@somefile` or similar is interpreted as a flag, altering
extraction behavior in attacker-chosen ways.

### [8] HIGH — SSRF DNS-rebinding TOCTOU (check-time vs fetch-time)

**Anchors:** `src/services/ingest/ssrf_guard.ts:118` (`checkUrl` calls `dnsLookup`), `:136-137`
(`fetchUrlStream` calls `checkUrl` then `fetch`).

`checkUrl` resolves the hostname once via `dnsLookup` and validates that first address against
private/reserved ranges (line ~118-120). Then `fetchUrlStream` calls `checkUrl(current)` and
**separately** calls `fetch(current, ...)` (lines ~136-137), which re-resolves DNS on its own.
Nothing pins the vetted IP, so an attacker who controls DNS can answer public at check-time and
`169.254.169.254` (the cloud metadata endpoint) at fetch-time.
**Failure scenario:** DNS rebinding passes the guard, `fetch` hits the metadata service, and the
metadata token is written into a user-readable artifact. Host allow/deny lists and some reserved
IPv6 ranges are also unimplemented.

### [22] MEDIUM — Unvalidated `gs://` source (no bucket allow-list)

**Anchors:** `src/services/ingest/ssrf_guard.ts:97-100` (early `return` for `gs:`).

`checkUrl` short-circuits with `if (parsed.protocol === "gs:") return;` — any `gs://` source is
trusted unconditionally. A `source_ref` of `gs://internal-bucket/secrets.json` is copied into
`DATA_BUCKET` and parsed, reading any object the service account can access. There is no bucket
allow-list.
**Failure scenario:** a job is submitted with `source_ref=gs://<internal-bucket>/<secret-object>`;
the pipeline dutifully copies and parses an internal secret.

---

## B. Broken features / silent data loss (correctness)

### [1] CRITICAL — POST `/upload` broken end-to-end

**Anchors:** `src/services/ingest/http_server.ts` — object-key construction (~line 34-35),
`source_type: "s3"` (~line 47).

The HTTP upload endpoint is broken on multiple axes: a leading-slash object-key mismatch between
writer and reader means the reader never finds what the writer stored; **no `parse_jobs` row is
ever inserted**, so every downstream event logs "Job not found" and `GET /jobs/:id` returns 404
forever; the `source_type` is hardcoded to the wrong value `"s3"` (line ~47) even for a GCS/HTTP
upload; multipart bodies are buffered unbounded in memory; and there is no auth or size cap.
**Failure scenario:** a user POSTs a file to `/upload`, gets an ID back, and can never retrieve the
job — it 404s permanently and nothing downstream can act on it.

### [3] CRITICAL — `.gz` / `.7z` archives are never detected

**Anchors:** `src/services/ingest/normalizer.ts:77` (`MAGIC_GZ`), `:78` (`MAGIC_7Z`), used at
`:83-84`.

The magic-byte constants are built from string literals: `const MAGIC_GZ = Buffer.from("\x1f\x8b")`
and `const MAGIC_7Z = Buffer.from("7z\xbc\xaf\x27\x1c")`. `Buffer.from(string)` defaults to
**UTF-8**, which re-encodes any code point ≥ 0x80 into two bytes. So `MAGIC_GZ` is not the two
bytes `1f 8b` but a longer, wrong sequence, and the `header.slice(0,2).equals(MAGIC_GZ)` compare
(line ~83) can never match. Same for 7z at line ~84. Result: gzip and 7z files are forwarded to
the classifier as raw compressed bytes.
**Fix shape:** use byte arrays — `Buffer.from([0x1f, 0x8b])` and
`Buffer.from([0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c])`. (`MAGIC_ZIP="PK\x03\x04"` and
`MAGIC_RAR="Rar!"` happen to be ASCII-only and survive.)

### [9] HIGH — Encrypted-ZIP password is a no-op

**Anchors:** `src/services/ingest/normalizer.ts:9` (`import NodeStreamZip`), `~:402` (extract with
`password` option ignored).

`node-stream-zip` v1 cannot decrypt AES/ZipCrypto entries; its `password` option is silently
ignored. The spec-mandated "encrypted ZIP → `awaiting_password` → resume with password" flow
therefore always terminates in `password_unavailable` **even when the user supplies the correct
password**.
**Failure scenario:** a user uploads an encrypted ZIP, is prompted for a password, provides the
right one, and the job still fails as if no password were available.

### [14] HIGH — `batchId` vs `batch_id` casing mismatch drops archive children from rollups

**Anchors:** emitters use camelCase `batchId` at `src/services/ingest/normalizer.ts:384` and `:241`;
readers expect snake_case `batch_id` at `src/shared/models/events.ts:46` and
`src/services/job_service/stateMachine.ts:124,128`.

The archive path emits events keyed `batchId`, but the event model and the state machine read
`batch_id`. The `INSERT` at `stateMachine.ts:124-128` binds `data.batch_id`, which is `undefined`
for these events. Archive children are therefore written with `batch_id = NULL` and drop out of the
batch rollup.
**Failure scenario:** a multi-file archive is expanded, but its children never associate with the
parent batch — batch counts are wrong and completion logic misfires.

### [26] MEDIUM — RAR entries written to the wrong bucket

**Anchors:** `src/services/ingest/normalizer.ts:274`;
`src/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.ts:74-75`.

The extracted `entryKey` is written back into the **source archive bucket**, not `DATA_BUCKET`. On
a read-only customer source bucket this yields a `403` (surfacing as a misleading "no extractable
files"), and on a writable one it pollutes a foreign bucket.
**Failure scenario:** customer provides a read-only source bucket; every RAR entry write 403s and
the job reports it found nothing to extract.

### [23] MEDIUM — `.gz` and nested entries bypass intake normalization

**Anchors:** `src/services/job_service/stateMachine.ts` `createChildJob` (~line 114-144) routes
straight to `CLASSIFY`.

`createChildJob` sends child entries directly to the CLASSIFY stage instead of routing them back
through archive detection. So a `.gz` file discovered under a prefix, or a depth-1 nested archive,
reaches the parser as raw compressed bytes and is never expanded. Depth-1 nesting is explicitly in
scope per the spec.
**Failure scenario:** an archive contains `inner.gz`; the child job classifies the still-compressed
bytes and parsing produces garbage or fails.

---

## C. Async flow / stuck jobs / duplication

This is the largest group and the source of most "stuck INGESTING" incidents. The archive-entry
consumer, the DB counters, and the reconciler safety net all interlock, and several defects
compound each other.

### [4] CRITICAL — Retry counter hardcoded to 1; poison messages redeliver forever

**Anchors:** `src/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.ts:314` (call site),
`:25` (`MAX_RETRIES = 3`), `:255-257` (`markPendingEntryFailed`), `:127` (`handleArchiveEntry`
signature).

`consumerLoop` always invokes `await handleArchiveEntry(payload, 1)` at line ~314. Because the
attempt is pinned to `1`, the `if (attempt >= MAX_RETRIES)` branch (line ~255) is unreachable,
`markPendingEntryFailed` (line ~257) is dead code, and a poison entry redelivers forever — each
redelivery re-downloading the (possibly multi-GB) parent archive. The entry is stuck `processing`
and the parent stays `INGESTING`.
**Failure scenario:** one corrupt entry in an archive turns into an infinite re-download/re-extract
loop that never fails out and never lets the parent complete.

### [5] CRITICAL — Premature DONE: `getPendingEntryCount` ignores `processing`

**Anchors:** `src/shared/db.ts:175-178` (`getPendingEntryCount`, `FILTER (WHERE status =
'pending')`).

Completion is gated on the count of `pending` entries only:

```sql
COUNT(*) FILTER (WHERE status = 'pending') as pending
```

Entries in the `processing` state are not counted. So while one sibling is still extracting, the
parent can see `pending == 0` and transition to `DONE` with data missing. A later failure of that
still-processing sibling then cannot do `DONE → FAILED` (DONE is terminal) and is swallowed.
Cross-replica siblings are also unserialized.
**Failure scenario:** two entries extract concurrently; the fast one finishes, `pending` hits 0,
the parent goes DONE, and the slow entry's data (or its failure) is lost.

### [7] CRITICAL — Reconciler never runs (ESM `require.main` crash + nothing schedules it)

**Anchors:** `src/scripts/reconciler.ts:100` (`if (require.main === module) { ... }`).

The reconciler is an ESM module, but its entrypoint guard uses the CommonJS idiom
`require.main === module`. In ESM, `require` is undefined, so this throws `ReferenceError` on
startup and the reconciler never executes its body. Worse, nothing schedules it anyway — there is
no Dockerfile stage, Cloud Build step, or Cloud Scheduler job that invokes it. Every stuck-state
defect in this group ([4], [5], [13], [21], [27], [31]) implicitly relies on this safety net that
does not exist. The reconciler also shares the `processing` blind spot from [5].
**Failure scenario:** any job that gets stuck stays stuck indefinitely; the intended sweeper that
would rescue it never starts.

### [13] HIGH — Password-resume dead-ends the parent job (illegal transition)

**Anchors:** `src/shared/models/job.ts:33` — `VALID_TRANSITIONS[AWAITING_PASSWORD] = [DETECTING,
FAILED]`; `transition()` check at `:112`.

After a password is supplied, `handleIngest` tries to move the parent from `awaiting_password` back
to `ingesting`. But the transition table only permits `awaiting_password → {detecting, failed}`
(verified at line 33: `[JobStatus.AWAITING_PASSWORD]: [JobStatus.DETECTING, JobStatus.FAILED]`).
`transition()` throws on the illegal `ingesting` move, the status never advances, and a later `done`
is likewise invalid. There is also no TTL, and the attempt/cache maps are per-process.
**Failure scenario:** user provides the password; the resume attempt throws on the state transition
and the job is wedged in `awaiting_password`.

### [15] HIGH — Total-size bomb guard double-counts the current entry

**Anchors:** `src/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.ts:162,165`
(`markPendingEntryProcessing` before `getPendingEntryTotalSize`); `src/shared/db.ts:188-192`
(`SUM` over `status IN ('completed', 'processing')`).

`markPendingEntryProcessing` marks the current entry `processing` *before*
`getPendingEntryTotalSize` runs, and that SUM includes `processing` rows (db.ts:192:
`WHERE job_id = $1 AND status IN ('completed', 'processing')`). So the current entry's size is
counted twice against the total-size ceiling. Any single async entry larger than ~half the cap
(~5 GB against a 10 GB limit) is rejected as a bomb (entry + entry > 10 GB) and loops.
**Failure scenario:** a legitimate 6 GB entry is flagged as a decompression bomb and reprocessed
forever.

### [16] HIGH — Ingest guard is not idempotent

**Anchors:** `src/services/ingest/IngestServiceHandler.ts` — short-circuit on `INGESTING`/`FAILED` then ack.

The ingest handler only short-circuits (and acks) when the job is already `INGESTING` or `FAILED`.
A redelivery *after* `DONE` is not caught, so it re-extracts the archive and duplicates every child.
Conversely, a crash mid-extraction acks the message and strands the job in `INGESTING`.
**Failure scenario:** the ingest message is redelivered after completion; the whole archive is
re-expanded and every child job is created a second time.

### [19] HIGH — `createChildJob` is not idempotent

**Anchors:** `src/services/job_service/stateMachine.ts:117` (`const childId = randomUUID()`),
insert at `:124`.

`createChildJob` mints a fresh `randomUUID()` for each invocation and there is no unique constraint
on `(parent_job_id, entry)`. A redelivered `ENTRY_DISCOVERED` event therefore inserts a **duplicate
child row** that re-parses and re-loads the same entry.
**Failure scenario:** the `ENTRY_DISCOVERED` event is delivered twice; the entry is parsed and
loaded twice, inflating downstream row counts.

### [20] HIGH — `publishEvent` swallows failures

**Anchors:** `src/shared/queueUtils.ts:263-268` (`publishEvent` `.catch(...) => return null`).

`publishEvent` wraps `sendMessage(...)` in `.catch((err) => { console.warn(...); return null; })`
(line ~264-267). The failure is logged and swallowed. If an `ENTRY_DISCOVERED` publish fails while
the entry has already been marked `completed`, the child is never created and the data is dropped
under a job that looks successful. A dropped `DONE` event strands the job.
**Failure scenario:** the events queue briefly rejects a publish; the entry is recorded as done but
no child ever runs, and no one notices because the job reports success.

### [21] HIGH — Permanent-error ack orphans the pending row

**Anchors:** `src/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.ts:323` (ack on "exceeds maximum" depth).

The depth check throws *before* `markProcessing`/`markFailed` run. `consumerLoop` catches the
"exceeds maximum" error and acks the message (line ~323) but never updates the entry row, so it
stays `pending` forever and the parent never completes.
**Failure scenario:** an over-deep nested archive entry is acked as handled, but its `pending` row
lingers and blocks parent completion indefinitely.

### [27] MEDIUM — Async-route publish failure swallowed, row without a message

**Anchors:** `src/services/ingest/normalizer.ts:246,250` (`createPendingArchiveEntry` then
`sendRaw`).

On the async archive route, `createPendingArchiveEntry` can succeed while the subsequent `sendRaw`
throws (line ~250). That leaves a `pending` row with **no corresponding queue message**, yet the
entry is still pushed to `out`, so the job is stuck `INGESTING` with an entry nothing will ever
process.
**Failure scenario:** the entry queue rejects one send; a pending row exists but no worker is ever
told to extract it.

### [31] MEDIUM — DONE published mid-ingestion (registration race)

**Anchors:** `src/services/ingest/normalizer.ts:237`.

A fast async entry can finish extracting before later entries of the *same* RAR have even been
registered. During that gap `pending == 0`, which (via [5]'s counting) can trigger a premature
DONE.
**Failure scenario:** entry 1 of a 5-entry RAR completes before entries 2–5 are inserted; the count
briefly reads zero and the parent completes early.

---

## D. Resource safety / OOM / leaks

The container runs with a few gigabytes of memory. Several paths buffer entire archives or bodies,
run bomb guards only after inflation, and leak temp files/handles on error paths.

### [6] CRITICAL — Whole-archive / whole-body buffering, uncapped

**Anchors:** `src/services/ingest/normalizer.ts:356` (`readFull`); URL fetch buffers via
`Buffer.concat`.

Only the RAR path streams. Everything else calls `readFull` (line ~356) to pull the whole archive
into memory, then makes a **second** full copy to tmpfs. The URL fetch path buffers the entire
response with `Buffer.concat`. The URL size cap is 5 GB — larger than the ~4 Gi container — and the
S3/upload paths have no cap at all.
**Failure scenario:** a 3 GB ZIP peaks at ~6 GB resident (in-memory + tmpfs copy), OOM-kills the
container, the message redelivers, and the pod crash-loops.

### [10] HIGH — Bomb guards run after full decompression; no per-entry cap for tar/7z

**Anchors:** `src/services/ingest/normalizer.ts:429` (gz), `:411` (zip), `:446`
(`extractTarArchive`), `:477` (`extract7z`) — `checkRatio` called after inflation.

gz/zip/tar/7z all inflate fully to RAM or tmpfs *before* `checkRatio` is consulted, and there is no
per-entry size cap for tar/7z. A 1 MB → 20 GB gzip bomb OOMs the process before the first ratio
guard ever runs.
**Failure scenario:** a small gzip bomb decompresses to 20 GB and kills the container before any
guard can reject it.

### [11] HIGH — RAR: no ratio/entry guard, trusts declared sizes, silent drop

**Anchors:** `src/services/ingest/normalizer.ts:231-266` (size decisions from listing),
streaming output.

The streaming RAR path never calls `checkRatio`. Its size decisions use the **attacker-declared**
byte counts from the `unrar` listing (lines ~231-266) while output streams unbounded to GCS.
Oversized entries are skipped with only a `console.log` — a silent partial data loss.
**Failure scenario:** an attacker declares tiny sizes in the RAR listing to bypass the check, then
streams enormous entries to GCS; or a legitimately oversized entry is silently dropped and the job
reports success with missing data.

### [17] HIGH — Extraction outlives the ack deadline

**Anchors:** `src/shared/queueUtils.ts:120-140` (`pubReceive` never calls `modifyAckDeadline`).

`pubReceive` never extends the message lease — there is no `modifyAckDeadline` call anywhere in the
receive path (confirmed absent). A 50-minute extraction against a 300-second ack deadline is
redelivered mid-run. This is the *root cause* that converts the guard blind-spots above into
routine duplicate and lost work.
**Failure scenario:** a large archive takes longer than the ack deadline; the queue redelivers it
to a second worker, and now two workers extract the same archive in parallel.

### [18] HIGH — Temp file / handle leaks on error paths

**Anchors:** `src/services/ingest/normalizer.ts:133` (RAR download before try/finally);
`src/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.ts:67`; zip/tar/7z cleanup at `normalizer.ts:417-418,
461-462, 494-495` (straight-line, not `finally`).

The RAR temp download happens *before* the `try/finally` that would unlink it (line ~133), and the
zip/tar/7z cleanup calls are straight-line rather than in a `finally` block. Any throw between
download and cleanup leaves the temp file (and sometimes an open handle) behind.
**Failure scenario:** an extraction error mid-way leaves multi-GB temp files accumulating on the
container's disk until it fills.

### [28] MEDIUM — Timeout does not cancel the underlying work

**Anchors:** `withTimeout` (a bare `Promise.race`), affecting `normalizer.ts` extraction.

`withTimeout` is implemented as a plain `Promise.race`. After the timeout fires and the job is
marked `FAILED`, the `unrar` child processes, GCS write streams, and nested extractions keep
running for hours, writing objects for a job that has already failed.
**Failure scenario:** a job times out and is failed, yet continues consuming CPU and writing GCS
objects long afterward.

### [29] MEDIUM — Concurrency cap is ineffective

**Anchors:** `src/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.ts:310`
(`messagesByJob...map(async ...)`).

The consumer builds promises with `.map(async ...)`, which starts every promise eagerly. The
subsequent chunking only staggers the `await`s, not the actual work — so up to N large jobs extract
in parallel inside a 2 Gi container.
**Failure scenario:** a batch of large archives all begin extracting simultaneously and collectively
blow the memory limit.

### [30] MEDIUM — Pipe resolves before nonzero exit; a failed stream never kills the child

**Anchors:** `src/services/ingest/normalizer.ts:288`.

The extraction promise resolves on the stream `finish` event, which fires *before* `unrar` reports
a nonzero exit (e.g. CRC error exit code 3) — so a truncated entry is accepted as complete.
Separately, a write error on the output stream never kills the `unrar` child, leaving a zombie
`unrar` pinning already-deleted scratch files.
**Failure scenario:** a corrupt RAR entry is accepted as valid data, and/or a zombie `unrar`
process holds disk space that cannot be reclaimed.

---

## E. Design conformance

These violate explicit spec guarantees even where behavior is otherwise "working."

### [24] MEDIUM — One bad entry aborts the whole ZIP/tar/7z

**Anchors:** `src/services/ingest/normalizer.ts` — ZIP/tar/7z entry loops lack per-entry
`try/catch` (contrast the RAR loop, which wraps entries at `~234-250` and `~306-340`).

Unlike the RAR path, the ZIP/tar/7z extraction loops have no per-entry `try/catch`. A single corrupt
entry throws out of the loop and fails the entire job, discarding every good entry already
extracted. This violates the spec guarantee that "a bad file never stops a batch."
**Failure scenario:** a ZIP with 100 good files and 1 corrupt one produces zero output — the whole
job fails on the one bad entry.

### [25] MEDIUM — Ingest/consumer write `parse_jobs` directly (cross-service races)

**Anchors:** `src/services/ingest/IngestServiceHandler.ts:79` (UPDATE `s3_url`/`size`);
`src/services/archive_entry_consumer/ArchiveEntryConsumerServiceHandler.ts` reads `parse_jobs` to decide DONE.

The ingest handler `UPDATE`s `parse_jobs` (`s3_url`, `size`) directly at line ~79, and the archive
entry consumer reads `parse_jobs` to decide completion. These cross-service writes race against the
Job Service's own `UPDATE`s, which is meant to be the sole owner of that table.
**Failure scenario:** the ingest handler's direct `UPDATE` and a concurrent Job Service `UPDATE`
interleave, clobbering fields and corrupting job state.

---

## Suggested fix order (backlog)

The review recommended tackling these in the following order, front-loading security and silent data
loss:

1. **Security & silent data loss:** `[0]` `[12]` passwords/argv, `[2]` symlink, `[8]` `[22]` SSRF,
   `[3]` magic bytes, `[14]` `batch_id`.
2. **Stuck-job / duplication:** `[4]` retry ceiling, `[5]` `[15]` processing-count, `[7]`
   reconciler, `[16]` `[19]` idempotency, `[20]` `[21]` `[27]` publish failures, `[13]` password
   transition, `[31]` ordering.
3. **Resource safety:** `[6]` `[10]` `[11]` streaming + guards + caps, `[17]` ack lease, `[18]`
   `finally` cleanup, `[28]` `[30]` cancel-on-timeout, `[29]` concurrency.
4. **Design conformance:** `[24]` per-entry isolation, `[26]` bucket, `[23]` nested/gz, `[1]`
   `/upload`, `[25]` table ownership, `[9]` encrypted-zip.

---

## Named resources referenced

| Kind | Name / identifier | Where it appears |
|------|-------------------|------------------|
| GCS bucket | `DATA_BUCKET` (from `settings.DATA_BUCKET`) | destination for parsed entries; `http_server.ts:34`, symlink exfil target `[2]` |
| GCS source | `gs://…` sources | trusted unconditionally by `checkUrl` `[22]` |
| Queue | `JOB_EVENTS_QUEUE_URL` | target of `publishEvent` `[20]`, `queueUtils.ts:264` |
| Queue | archive-entry queue | async entries via `sendRaw` `[27]`; consumed by `archive_entry_consumer` |
| DLQ | dead-letter queue | leaks passwords in failed message bodies `[0]` |
| DB table | `parse_jobs` | direct cross-service writes `[25]`; missing row for `/upload` `[1]` |
| DB table | pending-archive-entries (status `pending`/`processing`/`completed`/`failed`) | counting `[5]`, size sum `[15]`, orphan rows `[21]` `[27]`; `db.ts:175-192` |
| Event | `ENTRY_DISCOVERED` | duplicate children on redelivery `[19]` `[20]` |
| CLI | `unrar` (`spawn`) | password argv leak `[0]`, arg injection `[12]`, zombie/CRC `[30]` |
| Metadata IP | `169.254.169.254` | SSRF target `[8]` |

---

### Related knowledge-base notes

`parser-design-spec`, `parser-design-vs-implementation`, `parser-ingest-detect`,
`parser-known-bugs`, `parser-firefight-2026-07-17`.
