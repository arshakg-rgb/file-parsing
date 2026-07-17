# Changelog / Worklog — 2026-07-17

Parser project session log. Everything below was merged to `main`. Commits
are listed in the order they were made. Each entry records **what** changed,
**why**, the **files touched**, and how it was verified.

**Verification baseline for the whole session:**
- `tsc` (TypeScript compile) clean on every commit.
- `src/scripts/local_test.ts` — the offline `node:test` suite, grown to
  **87 cases** this session. It runs fully offline (no GCS, no DB, no
  Pub/Sub). New coverage: section 16 (encoding), section 17 (classifier
  ordered flow), section 18 (line-splitting + CSV output).

---

## 1. `0d11495` — Fix encoding detection

**What.** Stopped `ERR_UNKNOWN_ENCODING` crashes when the input declared a
legacy charset (`latin-1`, `iso-8859-1`, `cp1252`, and friends). Decoding now
goes through a `TextDecoder`-based `decode()` helper instead of handing raw
labels to Node's `Buffer`. Also changed the heuristic to **prefer UTF-8 over
low-confidence `jschardet` guesses** — jschardet was confidently-wrong on
short/ambiguous inputs and was corrupting otherwise-clean UTF-8.

**Why.** Node's `Buffer` only knows a small fixed set of encoding labels;
anything else throws `ERR_UNKNOWN_ENCODING` and killed the parse outright.
Separately, jschardet's low-confidence guesses were mangling valid UTF-8 text.

**Files touched.**
- `src/shared/encoding.ts` (**new**) — `decode()` via `TextDecoder`, charset
  normalization, UTF-8-preference logic.

**Verified.** `tsc` clean; `local_test.ts` section 16 (encoding) exercises the
legacy-charset labels and the UTF-8-vs-jschardet preference.

---

## 2. `de5299b` — Make `stream_parser` follow the designed ordered classifier flow

**What.** Reworked `stream_parser` to run the classifier in the intended,
ordered pipeline:

1. length / binary gate
2. header capture
3. record templates
4. structural recognizers (JSON / key-value)
5. rubbish detection
6. validated CSV
7. uncertain

The parser now extracts **only the `field_spec` fields**, and **declines
junk** instead of forcing a parse. Removed two shortcuts that were silently
overriding the ordered flow:
- the `formatDetector` bypass, and
- the greedy positional `csv-auto` path.

**Why.** The implementation had drifted from the design: the bypass and the
greedy CSV-auto path were classifying and emitting records that the ordered
flow was specifically meant to gate out (junk, binary, mis-shaped rows),
producing garbage output.

**Hardening.** After the rewrite, the change went through a **26-agent
adversarial review**, which surfaced **20 bugs — all found and fixed** before
merge.

**Files touched.** `stream_parser` classifier path and its ordered-stage
wiring (formatDetector bypass and positional csv-auto removed).

**Verified.** `tsc` clean; `local_test.ts` section 17 (classifier ordered
flow) asserts stage ordering, field_spec-only extraction, and junk declines.

---

## 3. `79d5cca` — Fix `field_spec` JSONB insert

**What.** Fixed `invalid input syntax for type json` on insert by
`JSON.stringify`-ing the `field_spec` array before binding it to the JSONB
column.

**Why.** A raw JS array was being passed to the JSONB parameter; the driver
handed Postgres something it could not parse as JSON, so every insert of a
`field_spec` failed.

**Files touched.**
- `router.ts`
- `stateMachine.ts`

**Verified.** `tsc` clean; offline suite still green.

---

## 4. `4463d80` — Line-splitting recovery for stray quotes + per-job CSV output

**What.** Two related pieces:
- **Line-splitting recovery** for stray / unbalanced quotes. `gcsUtils`
  gained a `scanLines` path with `MAX_QUOTED_NEWLINES` (default **0**) and a
  `MAX_LINE_BYTES` cap, so an unterminated quote can no longer swallow the
  rest of the file into one giant "line".
- **Per-job CSV output writer** that writes results to
  `gs://DATA_BUCKET/output/<jobId>.csv`.

**Why.** A single stray quote was causing the scanner to merge many physical
lines into one record (the root of the merged-blob symptom). The dedicated
CSV writer gives each job a clean, addressable output artifact.

**Files touched.**
- `gcsUtils` (`scanLines`, `MAX_QUOTED_NEWLINES`, `MAX_LINE_BYTES`)
- `src/shared/csvOutputWriter.ts` (**new**)

**Verified.** `tsc` clean; `local_test.ts` section 18 (line-splitting + CSV
output) covers the stray-quote recovery and the writer.

---

## 5. `4091092` + `c7d7388` — Trim Cloud Build to 4 test services

**What.** Reduced `cloudbuild.yaml` to build/deploy only the **4 services**
needed for fast test builds:
- `job-service`
- `ingest`
- `detect-bootstrap`
- `stream-parser`

Disabled the rest — `ai-classifier`, `load`, `report`, `retry`,
`archive-entry-consumer` — each commented with
`DISABLED FOR FAST TEST BUILDS`.

**Why.** Full builds were slow, and the disabled services are not needed for
the current test loop. `ai-classifier` in particular is **not** a separate
deploy anymore — AI runs **in-process** inside `detect` / `stream` via
`await import(...)`, so a standalone ai-classifier service is redundant.

- `4091092` — disable `ai-classifier`, `retry`, `archive-entry-consumer`.
- `c7d7388` — also disable `load` + `report` (leaving the 4-service set).

**Files touched.** `cloudbuild.yaml`.

**Verified.** `tsc` unaffected; offline suite unaffected (build-config only).

---

## 6. `8b8f91d` + `81ab663` — CI/CD: stop the broken GitHub Actions pipeline

**What.** The GitHub Actions workflow `ci-cd.yml` was a **broken parallel
deploy pipeline** and was switched to **`workflow_dispatch`-only** (manual
trigger; no longer runs on push).

**Why.** The Actions pipeline was structurally broken:
- Cloud Run service names used **underscores** (invalid — Cloud Run requires
  hyphenated names),
- it pushed to the **wrong registry** (`gcr.io`), and
- it was missing the **WIF (Workload Identity Federation) secrets** needed to
  authenticate.

The **real** deploy is a **Cloud Build trigger** that runs the repo's
`cloudbuild.yaml`. Having Actions also fire on push meant a second, failing,
conflicting pipeline on every push.

- `8b8f91d` — delegate CI/CD to `cloudbuild.yaml` instead of the broken
  parallel deploy.
- `81ab663` — stop GitHub Actions running on push (Cloud Build trigger is the
  real pipeline).

**Files touched.** `.github/workflows/ci-cd.yml`.

**Verified.** No code impact; `tsc` and offline suite unaffected.

---

## Test suite state

`src/scripts/local_test.ts` now runs **87 offline `node:test` cases**. New
sections this session:
- **16** — encoding (legacy charsets, UTF-8-vs-jschardet preference)
- **17** — classifier ordered flow (stage ordering, field_spec-only
  extraction, junk declines)
- **18** — line-splitting + CSV output (stray-quote recovery, per-job writer)

Run offline (no GCS / DB / Pub/Sub). Green as of the last commit, alongside a
clean `tsc`.

---

## OPEN BLOCKER — deployed `stream-parser` still runs OLD code

**Symptom.** After a **successful build**, the deployed `stream-parser` is
still executing pre-session code. Proven from the parquet output:
- a record with `email = "1368866"` — a bare numeric ID, which the **new**
  field_spec-only extraction could never produce, and
- a **~50-line merged row-0 blob** — exactly the stray-quote merge that
  `4463d80`'s line-splitting recovery eliminates.

Both artifacts are **impossible under the new code**, so the running revision
is old.

**Assessment.** This is a **deploy-side** problem, **not** a code problem. The
code on `main` is correct and verified. Leading suspects:
1. The Cloud Build **trigger** is using an **inline config**, not the repo's
   `cloudbuild.yaml`, so the intended build steps never ran; and/or
2. The **new revision did not replace** the running Pub/Sub-consumer instance
   (the old instance kept consuming).

**Fix under way.**
1. Install `gcloud`.
2. Build/deploy explicitly from the repo config:
   ```
   gcloud builds submit --config cloudbuild.yaml --project data-etl-499916 .
   ```
3. And/or **repoint the Cloud Build trigger** to the repo `cloudbuild.yaml`
   (away from any inline config) so future pushes deploy the correct code.

Until a fresh revision built from `cloudbuild.yaml` is confirmed serving,
treat any parquet output as potentially produced by the old code.
