# Design vs. Implementation

This document maps the parser's authoritative design (the "six rules" + ordered classifier described in the internal design spec) onto the code as it actually stands after the **2026-07-17 classifier refactor** (commit `de5299b`, "Make stream_parser follow the designed ordered classifier flow"). It is written for a new engineer who wants to understand, top to bottom, where the running system already matches intent, where it still diverges, and — most importantly — how the refactor closed the three biggest correctness gaps (a hardcoded `formatDetector` that bypassed the classifier, `field_spec` being silently ignored, and junk/header lines being force-parsed into garbage rows). Two design features remain deliberately deferred: **in-loop synchronous AI escalation** and the **match-rate (thrash) monitor**. Every claim below was spot-verified against the source at the paths cited; where the design and code disagree by design (e.g. GCS vs. "S3"), that is called out rather than treated as a defect.

> Note on terminology: the design document says "S3" throughout because it predates the port to Google Cloud. Production runs on **GCS** (bucket `datalead-osint`) and **Pub/Sub** (`QUEUE_BACKEND=pubsub`). Read "S3" in the design as "the object store", and "queue" as a Pub/Sub topic/subscription pair. This is a naming difference, not a behavioral divergence.

## The intended architecture in one paragraph

Take a client file (upload, link, or archive), copy it into the object store as one range-addressable source, stream-read it in a single pass, and classify **every line** against learned templates in a fixed, cheapest-first order. Extract **only** the fields the client asked for (the `field_spec`, e.g. `["email","name","phone","address"]`), write matched lines to per-template Parquet parts plus a DB row, and log every dropped-as-rubbish and failed line so no line's fate is silent. AI is consulted **only** when no known template matches, and its verdict is cached as a template so each distinct line pattern costs at most one AI call in its lifetime. That is the design. The sections below track how close the code is.

## Concrete names (verified)

| Thing | Name(s) in code | Where |
|---|---|---|
| Object-store bucket | `datalead-osint` (`DATA_BUCKET`) | `src/shared/config.ts:27` |
| Queue backend | Pub/Sub (`QUEUE_BACKEND=pubsub`) | `src/shared/config.ts:31` |
| Queues (topics) | `fpp-ingest`, `fpp-classify`, `fpp-parse`, `fpp-line-dlq`, `fpp-load`, `fpp-report`, `fpp-job-events`, `fpp-archive-entry` | `src/shared/config.ts:36-43` |
| Postgres tables | `parse_jobs`, `output_parts`, `rubbish_log`, `dead_letters`, `parsed_records`, `templates`, `pending_archive_entries` | `src/db/migrations/001_initial_schema.sql`, `src/shared/db.ts:200-305` |
| Firestore | AI Classifier's own template store (separate from Postgres `templates`) | `src/services/ai_classifier/templateRegistry.ts` |

The eight logical services live under `src/services/`: `job_service`, `ingest`, `detect_bootstrap`, `stream_parser`, `ai_classifier`, `load`, `report`, `archive_entry_consumer`, with a shared library in `src/shared/`.

## The six design rules and their implementation status

| # | Rule | Status | Notes |
|---|---|---|---|
| 1 | AI teaches templates; the engine runs them (never per-line) | Partial | The engine runs templates. AI is reached only out-of-band via retry/DLQ, not from the hot loop (see "Deferred"). |
| 2 | Two template stores, one classifier (record + rubbish) | Partial | `LineClassifier` tries record templates before rubbish, as designed. But there are **two disconnected registries** (Postgres vs. Firestore) — see below. |
| 3 | Extract only what the client wants (`field_spec`) | **Closed 2026-07-17** | Was violated; the refactor now extracts only `field_spec` fields on every path. |
| 4 | Everything becomes an object-store object first | Yes | Ingest copies uploads/links/archive entries into `datalead-osint`. |
| 5 | Bad row never stops a file; rubbish and failures go to separate stores, rubbish never retried | Mostly | `rubbish_log` vs. `dead_letters` are separate; retry skips rubbish. Line-fate accounting has known holes (see "Other divergences"). |
| 6 | Every line addressed by `(job_id, byte_offset)` + true `line_no` | Mostly | Byte offsets are carried; true source `line_no` is backfilled at finalize via prefix-sum only partially. |

## The designed classifier (the core)

The design specifies an ordered, cheapest-first classifier: (1) length/empty gate → drop, no AI; (2) record templates → extract target fields → Parquet row; (3) rubbish templates → drop + rubbish-log; (4) AI, only for unknowns, returning exactly one verdict — `record-template` (cached, parsed), `rubbish-signature` (cached, dropped+logged), or `uncertain` (→ dead-letter for human review, never a guess). A non-negotiable **asymmetry** governs it: record templates are tried *before* rubbish, rubbish matches only on a high-confidence structural signature, and ambiguity always resolves toward "keep and check" — dropping a real record is unrecoverable, an extra AI call is cheap.

This is implemented in `src/services/stream_parser/LineClassifier.ts` as `LineClassifier.classify()`. The verified order is:

1. **Length / empty / binary gate** (`classifier.ts:63-74`) — empty → rubbish `length-gate`; `> 64 KB` → `uncertain`; non-printable ratio `> 0.3` → rubbish `binary-gate`. Declined locally, never AI.
2. **Header capture** (first data line only, `classifier.ts:78-85`, `detectHeader` at `:353`) — a genuine header row is detected, used to build a name→column map, and **declined itself** (rubbish `header`), never emitted as a data row.
3. **Learned record templates** (`classifier.ts:91-110`) — scored best-match extraction; records take priority over rubbish, honoring the asymmetry.
4. **Structural recognizers** (`classifier.ts:121-124`) — `parseJsonRecord` (a JSON object) and `parseKvRecord` (a `Label: value - Label: value` line), both of which extract **only** `field_spec` fields via `extractFromObject` matching keys by name/alias.
5. **Rubbish templates** (`classifier.ts:127-131`) — only when `confidence >= RUBBISH_CONFIDENCE_MIN`, matching a structural `signature`.
6. **Validated delimited/CSV** (`parseDelimitedRecord`, `classifier.ts:372-408`) — header-mapped when a header was seen (`csv-mapped`), else identify columns by **content** for strongly-typed fields like email/phone (`csv-auto`). Returns `null` for unmappable rows so they are declined, not force-parsed.
7. **Uncertain** (`classifier.ts:147`) — nothing matched → `verdict: "uncertain"`, `failure_class: UNCERTAIN`, which the caller dead-letters.

The `classifyWithAI` / `classifyWithTimeout` methods (`classifier.ts:150-179`) exist and would fill design step 4, but **the parse loop does not call them** — see "Still deferred".

## How the 2026-07-17 refactor closed the biggest gaps

Before commit `de5299b`, the stream parser did not run the designed classifier at all for most lines. Three concrete defects were fixed.

### Gap 1 — `formatDetector` bypassed the classifier, template registry, and AI

Commit `0507455` had added `src/shared/formatDetector.ts` (`parseLine` classifying each line as BINARY/JSON/TWITTER_USER/CSV). In `stream_parser/handler.ts` it ran **first**: BINARY → dropped rubbish; JSON and TWITTER_USER → parsed **directly** into Parquet with hardcoded `templateId` `"json"`/`"twitter_user"`, dumping `...sanitizedRow` (every parsed key), bypassing `LineClassifier`, the template registry, and the AI path; only CSV fell through to the classifier. Consequences: extraction was not `field_spec`-targeted for JSON/Twitter (rule 3 violated), no template was ever learned for those shapes (rule 1 violated), and `parseTwitterUserLine` truncated any name/email containing `-`.

**Fix (verified):** `handler.ts` no longer imports `formatDetector`; it constructs one `LineClassifier` per job (`handler.ts:202`) and routes **every** line through `classifier.classify(line, byteOffset, byteLength)` (`handler.ts:226`). JSON and key-value shapes are now handled by the classifier's structural recognizers, which extract only `field_spec` fields.

### Gap 2 — `field_spec` was silently ignored / dropped

Two failures existed. (a) At job creation, `router.ts` accepted a `field_spec` only as an array or `{fields:[...]}`; a client sending a **JSON string** (`field_spec: "[\"email\",\"name\"]"`) matched neither and was stored as `[]` in `parse_jobs`, even though the raw string was still forwarded on the ingest queue and re-parsed downstream — so the DB and report saw no spec while parsing limped on. (b) The headerless CSV path mapped **positionally** (`parts[0]→field_spec[0]`, …), so a real row like `1416779,2231849,"OD2667900",…` yielded `email=1416779, name=2231849, phone="OD2667900"` — garbage, because nothing checked that a column's *content* matched the field's *meaning*.

**Fix (verified):** `src/services/job_service/JobServiceRouter.ts:20-37` now normalizes `field_spec` from any of: a plain array, a JSON-array string, a JSON-`{fields:[…]}` string, a `{fields}` object, or a plain comma-separated string. The normalized array is stored (`router.ts:59`) and re-echoed consistently. On the extraction side, the headerless CSV path identifies columns by **content** (`validateField`, `classifier.ts:261-277`: email must match an email regex; phone must be 10–15 digits and contain no `@`) instead of by position; weak fields (name/address) are left `null` rather than guessed. The header-mapped path (`csv-mapped`) trusts the captured header when one is present, which is the reliable route.

### Gap 3 — junk and header lines were force-parsed into rows

The old csv-auto accepted **any** line with at least `max(2, |field_spec|)` delimited parts, and `detect_bootstrap`'s header-skip regex only affected template *seeding*, not the actual re-read parse. So a header row or a delimiter-bearing junk line became a "parsed" garbage row in the output.

**Fix (verified):** the classifier now **declines** rather than force-parses. `detectHeader` (`classifier.ts:353-370`) treats the first line as a header only when it is unmistakably one — every cell a bare label (no `@`, no ≥7-digit run), and it locates a **majority** (`Math.max(2, ceil(|field_spec|/2))`) of requested fields — so a words-only first *data* row like `Cell,Berlin` is not misread as a header (which would both drop that record and corrupt the column map for every following row). `parseDelimitedRecord` returns `null` (→ uncertain → DLQ) when no field can be confidently placed. Fragile `Label: value` lines require either a strong email/phone match or ≥2 requested fields to be accepted (`extractFromObject` with `requireStrong=true`, `classifier.ts:286-312`). The refactor also fixed the Parquet/trace `record_index` off-by-one.

The first cut of this refactor went through a multi-agent adversarial review that surfaced ~20 bugs, all fixed before merge: strict header detection, the stronger key-value acceptance rule, aliases trimmed so `username` no longer maps to `name`, the 10–15-digit phone rule, and removal of the value-truncating `k=v` whitespace fallback (see the removed fallback note at `classifier.ts:323-334`). Sample result on the heterogeneous test file: 183 parsed (180 with email), 28 declined, and every parsed row carries only `field_spec` keys.

> Why this file mattered: `test-files/test version 1.csv` (211 non-empty lines) is, despite its name, a mix of **three** formats — 35 `Email: … - Name: … - ScreenName: …` key-value lines, 28 JSON Twitter records with emoji, and 147 genuine quoted CSV rows — exactly the "alternating good/other/rubbish" scenario the design targets. It is what exposed all three gaps.

## Still deferred (by design, not oversight)

### In-loop synchronous AI escalation (design step 4)

The design's centerpiece is: an unknown line triggers a **synchronous** AI call from inside the streaming pass, the verdict is cached as a template, and the parser acts on it — so the very lines the AI is meant to learn from are the ones it sees. **This is intentionally not wired.** The hot loop calls only the deterministic `classifier.classify()`; unmatched lines return `uncertain` and are dead-lettered to `fpp-line-dlq`. The retry service picks them up out-of-band: `src/services/retry/RetryServiceHandler.ts:52` routes `FailureClass.UNCERTAIN` straight to `updateDeadLetterStatus(dlq_id, "review")` (`retry/handler.ts:137`) — i.e. human review, not AI. The scaffolding for in-loop AI exists but is dormant:

- `LineClassifier.classifyWithAI` / `classifyWithTimeout` (`classifier.ts:150-179`) — never called from `handler.ts`.
- `AIRateLimiter` is defined and instantiated in `stream_parser/handler.ts:58,102`, but its `acquire()` is never awaited on the parse path.
- The AI is reached (from the retry side and via in-process dynamic `import("../ai_classifier/handler.js")`, `classifier.ts:162`) only for encoding/transform/extraction/type-mismatch retries — not for `uncertain`.

The commit message states the reason plainly: unbounded serial in-loop AI, without a rate limiter, a match-rate monitor, and a per-job call budget all wired first, would stall the parse loop and hammer the model. So "one AI call per distinct pattern" is, for now, effectively zero AI calls in the hot loop; unknowns reach AI (if at all) only through the DLQ/retry path.

### Match-rate (thrash) monitor

The design's thrash guard watches the local-template hit ratio and **flags the job** on collapse instead of silently hammering AI. `MatchRateMonitor` exists (`src/services/stream_parser/MatchRateMonitor.ts:3`) but is **never imported or instantiated** in `stream_parser/handler.ts` — there is no collapse flagging. Because it is a precondition for safely enabling in-loop AI, it is deferred together with step 4.

## Other notable divergences (still open)

These are pre-existing gaps the refactor did not target; they are carried in the backlog and worth knowing.

- **Two template registries that never sync.** The parser and in-process AI use the **Postgres `templates`** table via `src/shared/templateRegistry.ts` (`SELECT * FROM templates WHERE kind = 'record' | 'rubbish'`). The standalone Express AI Classifier uses a **separate Firestore** store (`src/services/ai_classifier/templateRegistry.ts`). The design wanted one registry owned by the AI Classifier. They do not sync, and the standalone service defaults to a mock model (`src/services/ai_classifier/main.ts:20-21`, `BEDROCK_MODEL_ID === "mock"` → `mockClassify`). The design's `AI_CLASSIFIER_URL` sync-call path is effectively dead; the live path is the in-process dynamic import.
- **Quality gate reads the wrong field and includes rubbish in the denominator.** `src/shared/qualityGate.ts:34-35` computes `totalLines = counts.parsed + counts.dropped_rubbish + counts.failed` and `failedLineRatio = counts.failed / totalLines`. Two problems vs. design: the design says the gate is a **failed-line** ratio with **rubbish excluded** from the denominator (here rubbish is included), and the parser actually tracks `failed_by_class`, so `counts.failed` is often `undefined`. The `held` state is set at `stateMachine.ts` when `failed/total > 0.05`, but because the gate reads a different field, "held with 0 failures" can appear spurious. Release via `POST /v1/jobs/:id/release-hold`.
- **Trace is not atomic with the record.** Design requires the Parquet row and its trace to be one write, no orphans. Implementation appends to the Parquet buffer and separately `INSERT`s into `parsed_records` per line — not atomic, and a hot-loop bottleneck; a trace failure miscounts the row as rubbish.
- **Load idempotency simplified.** Design says stage on `(job_id, byte_offset)` then merge; implementation inserts directly with `ON CONFLICT DO NOTHING`. Same net effect for duplicates, but no staging table — an accepted simplification.
- **Flush is line-count, not RAM-watermark.** Design says flush at a ~256 MB RAM watermark; implementation flushes on a hardcoded 1000-line threshold (`RAM_FLUSH_WATERMARK` is read only by dead code). Related constant-memory claims are also violated: finalize reads the whole source file, load reads whole parts into memory, and archive ingest buffers whole archives.
- **Line-fate accounting has holes.** `PARSING_COMPLETED` events carry `dlq_count=0` and `rubbish_log_path=undefined`, `output_parts` is not populated by live code, and background-flush part paths are dropped from `part_s3_paths` — so the report's "every line's fate recorded" guarantee does not hold end-to-end, even though the underlying `parsed_records` / `rubbish_log` / `dead_letters` tables do capture the data.
- **`type_mismatch` retry can fabricate rows.** The "broader coercion" strategy can emit ALL-NULL rows loaded as "recovered", which arguably violates "never a guess". DLQ statuses in code (`pending`/`recovered`/`review`) also exceed the design's `retry|review|resolved`.
- **`exec_path` parallel path is unimplemented.** The `exec_path` column exists but is always `'stream'`; the parallel path for homogeneous ≥1 GB files is explicitly deferred in the design, so this is low priority.
- **Password handling is best-effort.** Design wants transient, never-logged/stored passwords with bounded attempts and a TTL → `failed(password_unavailable)`. Implementation keeps password state in per-process Maps (lost on restart, counted per replica) with no TTL, so bounded attempts are only best-effort.

## Design-acknowledged non-defects

Several apparent "gaps" are actually pending design decisions (the spec's §8 open questions), not bugs: the quality-gate ratio value, the match-rate floor, the `field_spec` format, the missing-target-field policy (current code emits `null`, one of the sanctioned options), the target DB, and human-review-queue ownership. The spec's §9 also marks nested archives and non-UTF-8 transcode-at-ingest as **deferred** — so the fact that exotic encodings are not fully handled is spec-sanctioned rather than a defect, and ingest already exceeds spec by doing depth-1 nested extraction. Encoding itself was fixed on 2026-07-17 (commit `0d11495`): `src/shared/encoding.ts` (`decode()`, `isLikelyUtf8()`, `normalizeEncoding()`, `bufferEncodingFor()`) replaced the crashing `Buffer.toString(label)` calls that rejected labels like `latin-1`/`iso-8859-1`, and it is wired into `detect_bootstrap`, the `stream_parser` probe loop, and `gcsUtils.streamLines`. The §8 note also blesses consolidating the eight logical services into 2–3 binaries for a small team; the current split preserves the service boundaries regardless of deployment shape.
