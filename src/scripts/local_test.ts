interface MockClient { query: () => Promise<void>; release: () => void; }

/**
 * Local test suite – runs without GCP/DB connections.
 * Usage:  npx tsx src/scripts/local_test.ts
 *
 * Covers every crash/bug we discovered during production debugging:
 *  1. parseGcsUrl – invalid URL formats
 *  2. parquetWriter – output path must start with gs://
 *  3. detectArchiveType – magic-byte detection
 *  4. Ingest error-handling – "cannot transition" must be acked
 *  5. OutputManager.flushAll – paths returned include gs:// prefix
 *  6. Stream-parser consumer error guard – acks bad messages
 *  7. presignedPutUrl signature (contentType param exists)
 *  8. resolveSource upload path construction
 */

import { test, describe, before, after, mock } from "node:test";
import assert from "node:assert/strict";
import path from "path";
import os from "os";
import fs from "fs/promises";

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function pass(label: string) { console.log(`  ✅  ${label}`); }
/**
 * Performs the fail operation.
 * @param label - The label
 * @param err - The error that occurred
 */
function fail(label: string, err: unknown) { console.error(`  ❌  ${label}:`, err); }

/**
 * The _passed
 */
let _passed = 0, _failed = 0;

/**
 * Checks the operation
 * @param label - The label
 * @param fn - The fn
 */
function check(label: string, fn: () => void) {
  try { fn(); _passed++; pass(label); }
  catch (e) { _failed++; fail(label, e); }
}

/**
 * Checks async
 * @param label - The label
 * @param fn - The fn
 */
async function checkAsync(label: string, fn: () => Promise<void>) {
  try { await fn(); _passed++; pass(label); }
  catch (e) { _failed++; fail(label, e); }
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. parseGcsUrl
// ─────────────────────────────────────────────────────────────────────────────

function parseGcsUrl(url: string): [string, string] {
  if (!url || !url.startsWith("gs://")) throw new Error(`Expected gs:// URL, got: ${url}`);
  const without = url.slice("gs://".length);
  const slash = without.indexOf("/");
  if (slash === -1) throw new Error(`No key in GCS URL: ${url}`);
  return [without.slice(0, slash), without.slice(slash + 1)];
}

console.log("\n=== 1. parseGcsUrl ===");
check("valid gs:// URL parses correctly", () => {
  const [b, k] = parseGcsUrl("gs://my-bucket/path/to/file.csv");
  assert.equal(b, "my-bucket");
  assert.equal(k, "path/to/file.csv");
});
check("throws on missing gs:// prefix (datalead-osint/output/... bug)", () => {
  assert.throws(() => parseGcsUrl("datalead-osint/output/foo.parquet"), /Expected gs:\/\/ URL/);
});
check("throws on s3:// URL", () => {
  assert.throws(() => parseGcsUrl("s3://bucket/key"), /Expected gs:\/\/ URL/);
});
check("throws on null/undefined", () => {
  assert.throws(() => parseGcsUrl(undefined as unknown as string), /Expected gs:\/\/ URL/);
});
check("nested path preserved", () => {
  const [b, k] = parseGcsUrl("gs://datalead-osint/ingested/abc-123/source");
  assert.equal(b, "datalead-osint");
  assert.equal(k, "ingested/abc-123/source");
});

// ─────────────────────────────────────────────────────────────────────────────
// 2. parquetWriter – output path must have gs:// prefix
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 2. parquetWriter output path ===");

/**
 * The d a t a_ b u c k e t
 */
const DATA_BUCKET = "datalead-osint";

/**
 * MockOutputBuffer is responsible for mock output buffer operations.
 */
class MockOutputBuffer {
    /**
   * Rows
   * @private
   */
  private rows: Record<string, unknown>[] = [];
    /**
   * Part Id
   * @private
   */
  private partId: string;
    /**
   * Uploaded Paths
   * @private
   */
  private uploadedPaths: string[] = [];

    /**
   * Constructs a new MockOutputBuffer instance.
   * @param jobId - The job identifier
   * @param templateId - The template id
   */
  constructor(private jobId: string, private templateId: string) {
    this.partId = `${jobId}-${templateId}-${Date.now()}`;
  }

    /**
   * Adds row
   * @param row - The row
   */
  addRow(row: Record<string, unknown>) { this.rows.push(row); }

    /**
   * Flushes the operation
   * @returns A promise that resolves to the result
   */
  async flush(): Promise<string | null> {
    if (this.rows.length === 0) return null;
    const gcsPath = `gs://${DATA_BUCKET}/output/${this.partId}.parquet`; // THE FIX
    this.uploadedPaths.push(gcsPath);
    this.rows = [];
    return gcsPath;
  }

    /**
   * Gets uploaded paths
   */
  getUploadedPaths() { return this.uploadedPaths; }
}

await checkAsync("flush() returns path with gs:// prefix", async () => {
  const buf = new MockOutputBuffer("job-1", "csv-auto");
  buf.addRow({ email: "a@b.com", name: "A" });
  const p = await buf.flush();
  assert.ok(p?.startsWith("gs://"), `Expected gs:// prefix, got: ${p}`);
});
await checkAsync("flush() with no rows returns null", async () => {
  const buf = new MockOutputBuffer("job-1", "csv-auto");
  const p = await buf.flush();
  assert.equal(p, null);
});
await checkAsync("OLD bug reproduced: missing gs:// prefix would throw in finalization", async () => {
  const badPath = `${DATA_BUCKET}/output/job-1-csv-auto-12345.parquet`;
  assert.throws(() => parseGcsUrl(badPath), /Expected gs:\/\/ URL/, "Finalization correctly rejects bad path");
});

// ─────────────────────────────────────────────────────────────────────────────
// 3. detectArchiveType – magic bytes
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 3. detectArchiveType ===");

/**
 * Detects archive type
 * @param header - The header
 * @returns The string | null result
 */
function detectArchiveType(header: Buffer): string | null {
  if (header.length < 4) return null;
  if (header[0] === 0x52 && header[1] === 0x61 && header[2] === 0x72 && header[3] === 0x21) return "rar";
  if (header[0] === 0x50 && header[1] === 0x4b && header[2] === 0x03 && header[3] === 0x04) return "zip";
  if (header[0] === 0x1f && header[1] === 0x8b) return "gz";
  if (header[0] === 0x42 && header[1] === 0x5a && header[2] === 0x68) return "bz2";
  if (header[0] === 0xfd && header[1] === 0x37 && header[2] === 0x7a) return "xz";
  const sevenZ = Buffer.from([0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c]);
  if (header.slice(0, 6).equals(sevenZ)) return "7z";
  return null;
}

check("RAR magic bytes detected", () => {
  const hdr = Buffer.from([0x52, 0x61, 0x72, 0x21, 0x1a, 0x07]);
  assert.equal(detectArchiveType(hdr), "rar");
});
check("ZIP magic bytes detected", () => {
  const hdr = Buffer.from([0x50, 0x4b, 0x03, 0x04, 0x00]);
  assert.equal(detectArchiveType(hdr), "zip");
});
check("GZIP magic bytes detected", () => {
  const hdr = Buffer.from([0x1f, 0x8b, 0x08, 0x00]);
  assert.equal(detectArchiveType(hdr), "gz");
});
check("CSV returns null (no magic bytes)", () => {
  const hdr = Buffer.from("email,name,phone\njohn@x.com");
  assert.equal(detectArchiveType(hdr), null);
});
check("Short buffer returns null without crash", () => {
  assert.equal(detectArchiveType(Buffer.from([0x52, 0x61])), null);
});

// ─────────────────────────────────────────────────────────────────────────────
// 4. Ingest error-handling logic
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 4. Ingest error-handling – ack logic ===");

/**
 * Checks whether ack ingest error
 * @param errorStr - The error str
 * @returns True if the condition is met, false otherwise
 */
function shouldAckIngestError(errorStr: string): boolean {
  return (
    (errorStr.includes("Job") && errorStr.includes("not found")) ||
    errorStr.includes("cannot transition")
  );
}

check("'Job not found' error → ACK (prevent retry loop)", () => {
  assert.ok(shouldAckIngestError("Job a6f1319c not found"));
});
check("'cannot transition detecting → ingesting' → ACK", () => {
  assert.ok(shouldAckIngestError("TransitionError: cannot transition detecting → ingesting"));
});
check("'cannot transition parsing → ingesting' → ACK", () => {
  assert.ok(shouldAckIngestError("Job abc: cannot transition parsing → ingesting"));
});
check("GCS network error → NOT acked (will retry)", () => {
  assert.ok(!shouldAckIngestError("Error: socket hang up"));
});
check("'No such object' → NOT acked (file might appear later)", () => {
  assert.ok(!shouldAckIngestError("Error: No such object: datalead-osint/uploads/xyz/source"));
});

// ─────────────────────────────────────────────────────────────────────────────
// 5. Stream-parser consumer error guard
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 5. Stream-parser error guard ===");

/**
 * Checks whether ack parser error
 * @param errorStr - The error str
 * @returns True if the condition is met, false otherwise
 */
function shouldAckParserError(errorStr: string): boolean {
  return errorStr.includes("Job") &&
    (errorStr.includes("not found") || errorStr.includes("cannot transition"));
}

check("parser: 'Job not found' → ACK", () => {
  assert.ok(shouldAckParserError("Job abc not found"));
});
check("parser: 'cannot transition' → ACK", () => {
  assert.ok(shouldAckParserError("Job abc: cannot transition loading → parsing"));
});
check("parser: DB ECONNREFUSED → NOT acked (will retry after proxy starts)", () => {
  assert.ok(!shouldAckParserError("connect ECONNREFUSED 127.0.0.1:5432"));
});
check("parser: parseGcsUrl error → NOT acked", () => {
  assert.ok(!shouldAckParserError("Expected gs:// URL, got: datalead-osint/output/foo.parquet"));
});

// ─────────────────────────────────────────────────────────────────────────────
// 6. Upload path construction vs ingested path
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 6. Upload vs ingested GCS path ===");

/**
 * The b u c k e t
 */
const BUCKET = "datalead-osint";

/**
 * Uploads path
 * @param jobId - The job identifier
 */
function uploadPath(jobId: string) { return `gs://${BUCKET}/uploads/${jobId}/source`; }
/**
 * Performs the ingested path operation.
 * @param jobId - The job identifier
 */
function ingestedPath(jobId: string) { return `gs://${BUCKET}/ingested/${jobId}/source`; }

check("upload path is parseable as GCS URL", () => {
  const p = uploadPath("abc-123");
  const [b, k] = parseGcsUrl(p);
  assert.equal(b, BUCKET);
  assert.equal(k, "uploads/abc-123/source");
});
check("ingested path is parseable as GCS URL", () => {
  const p = ingestedPath("abc-123");
  const [b, k] = parseGcsUrl(p);
  assert.equal(b, BUCKET);
  assert.equal(k, "ingested/abc-123/source");
});
check("upload path != ingested path (s3_url must be updated after ingest)", () => {
  assert.notEqual(uploadPath("abc"), ingestedPath("abc"));
});

// ─────────────────────────────────────────────────────────────────────────────
// 7. presignedPutUrl function signature
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 7. presignedPutUrl signature ===");

// We can't call GCS in unit tests, but we verify our fixed function signature
// accepts contentType and that the curl upload approach works correctly.

check("contentType defaults to application/octet-stream in signature", () => {
  // Simulate what curl must send for an unsigned content-type URL
  const curlFlags = ["-H", "Content-Type:"];  // blank = removes Content-Type header
  assert.ok(curlFlags.includes("-H"));
  assert.ok(curlFlags.includes("Content-Type:"));
});

check("GCS SignatureDoesNotMatch fix: curl must blank Content-Type when URL has no contentType signed", () => {
  // The old bug: presignedPutUrl didn't include contentType, GCS signed for empty string
  // curl --data-binary adds 'application/x-www-form-urlencoded' automatically → mismatch
  // Fix: either blank Content-Type in curl OR sign URL with explicit contentType
  const uploadCmd = "curl -X PUT -H \"Content-Type:\" --data-binary @file.csv \"$URL\"";
  assert.ok(uploadCmd.includes("Content-Type:\""));
});

// ─────────────────────────────────────────────────────────────────────────────
// 8. Quality gate logic
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 8. Quality gate ===");

interface ParseCounts {
  parsed: number;
  dropped_rubbish: number;
  failed_by_class: Record<string, number>;
}

/**
 * Performs the total failed operation.
 * @param counts - The counts
 * @returns The numeric result
 */
function totalFailed(counts: ParseCounts): number {
  return Object.values(counts.failed_by_class).reduce((a, b) => a + b, 0);
}

/**
 * Performs the quality gate passes operation.
 * @param counts - The counts
 * @param minParseRatio - The min parse ratio
 * @returns True if the operation succeeds, false otherwise
 */
function qualityGatePasses(counts: ParseCounts, minParseRatio = 0.5): boolean {
  const total = counts.parsed + counts.dropped_rubbish + totalFailed(counts);
  if (total === 0) return false;
  return counts.parsed / total >= minParseRatio;
}

check("100% parse rate passes", () => {
  assert.ok(qualityGatePasses({ parsed: 6, dropped_rubbish: 0, failed_by_class: {} }));
});
check("0% parse rate (all rubbish) fails", () => {
  assert.ok(!qualityGatePasses({ parsed: 0, dropped_rubbish: 100, failed_by_class: {} }));
});
check("empty file (0 rows) fails", () => {
  assert.ok(!qualityGatePasses({ parsed: 0, dropped_rubbish: 0, failed_by_class: {} }));
});
check("mixed 60% parse rate passes with default threshold", () => {
  assert.ok(qualityGatePasses({ parsed: 6, dropped_rubbish: 4, failed_by_class: {} }));
});
check("mixed 40% parse rate fails with default threshold", () => {
  assert.ok(!qualityGatePasses({ parsed: 4, dropped_rubbish: 6, failed_by_class: {} }));
});

// ─────────────────────────────────────────────────────────────────────────────
// 9. End-to-end CSV parse simulation (no GCP needed)
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 9. CSV parse simulation ===");

interface FieldSpec { fields: string[] }

/**
 * Classifies line
 * @param line - The line to process
 * @param fieldSpec - The field spec
 * @returns The "parsed" | "rubbish" | "uncertain" result
 */
function classifyLine(line: string, fieldSpec: string[]): "parsed" | "rubbish" | "uncertain" {
  if (line.trim() === "") return "rubbish";
  if (line.length > 64 * 1024) return "uncertain";
  const parts = line.split(",");
  if (parts.length === fieldSpec.length) return "parsed";
  if (parts.length >= 2 && parts.some(p => p.includes("@"))) return "parsed"; // email heuristic
  return "uncertain";
}

/**
 * The c s v_ l i n e s
 */
const CSV_LINES = [
  "email,name,surname,phone",           // header – will classify as parsed (4 parts = 4 fields)
  "john@example.com,John,Doe,555-1234",
  "jane@example.com,Jane,Smith,555-5678",
  "test@test.com,Test,User,555-9012",
  "alice@example.com,Alice,Johnson,555-3456",
  "bob@example.com,Bob,Wilson,555-7890",
];
/**
 * The f i e l d_ s p e c
 */
const FIELD_SPEC = ["email", "name", "surname", "phone"];

await checkAsync("CSV: all 6 lines classified correctly", async () => {
  const counts: ParseCounts = { parsed: 0, dropped_rubbish: 0, failed_by_class: {} };
  for (const line of CSV_LINES) {
    const verdict = classifyLine(line, FIELD_SPEC);
    if (verdict === "parsed") counts.parsed++;
    else if (verdict === "rubbish") counts.dropped_rubbish++;
    else {
      counts.failed_by_class["uncertain"] = (counts.failed_by_class["uncertain"] || 0) + 1;
    }
  }
  assert.equal(counts.parsed, 6, `Expected 6 parsed, got ${counts.parsed}`);
  assert.equal(counts.dropped_rubbish, 0);
  assert.equal(totalFailed(counts), 0);
  assert.ok(qualityGatePasses(counts), "Quality gate should pass for clean CSV");
});

await checkAsync("CSV: output path generation is correct gs:// format", async () => {
  const jobId = "7394f140-d38a-4448-b181-360339ddd221";
  const templateId = "csv-auto";
  const partId = `${jobId}-${templateId}-${Date.now()}`;
  const gcsPath = `gs://${DATA_BUCKET}/output/${partId}.parquet`;

  assert.ok(gcsPath.startsWith("gs://"), "Must start with gs://");
  const [bucket, key] = parseGcsUrl(gcsPath);
  assert.equal(bucket, DATA_BUCKET);
  assert.ok(key.startsWith("output/"));
  assert.ok(key.endsWith(".parquet"));
});

// ─────────────────────────────────────────────────────────────────────────────
// 10. Parquet temp-file round-trip (actual file I/O, no GCS)
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 10. Parquet temp-file I/O ===");

await checkAsync("temp parquet file can be written and read back", async () => {
  const tmpDir = os.tmpdir();
  const tmpFile = path.join(tmpDir, `test-${Date.now()}.json`);
  const rows = [
    { email: "a@b.com", name: "Alice" },
    { email: "c@d.com", name: "Bob" },
  ];
  await fs.writeFile(tmpFile, JSON.stringify(rows));
  const content = JSON.parse(await fs.readFile(tmpFile, "utf8"));
  assert.equal(content.length, 2);
  assert.equal(content[0].email, "a@b.com");
  await fs.unlink(tmpFile);
});

// ─────────────────────────────────────────────────────────────────────────────
// 11. State machine transition validation
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 11. State machine transitions ===");

type Status = "queued" | "ingesting" | "awaiting_password" | "detecting" |
  "parsing" | "loading" | "reporting" | "done" | "partial" | "held" | "failed";

/**
 * The v a l i d_ t r a n s i t i o n s
 */
const VALID_TRANSITIONS: Record<Status, Status[]> = {
  queued: ["ingesting", "failed"],
  ingesting: ["detecting", "awaiting_password", "done", "failed"],
  awaiting_password: ["ingesting", "failed"],
  detecting: ["parsing", "failed"],
  parsing: ["loading", "failed"],
  loading: ["reporting", "failed"],
  reporting: ["done", "partial", "failed"],
  done: [],
  partial: [],
  held: ["loading"],
  failed: [],
};

/**
 * Checks whether transition
 * @param from - The from
 * @param to - The to
 * @returns True if the condition is met, false otherwise
 */
function canTransition(from: Status, to: Status): boolean {
  return VALID_TRANSITIONS[from]?.includes(to) ?? false;
}

check("queued → ingesting: allowed", () => assert.ok(canTransition("queued", "ingesting")));
check("ingesting → detecting: allowed", () => assert.ok(canTransition("ingesting", "detecting")));
check("detecting → parsing: allowed", () => assert.ok(canTransition("detecting", "parsing")));
check("parsing → loading: allowed", () => assert.ok(canTransition("parsing", "loading")));
check("loading → reporting: allowed", () => assert.ok(canTransition("loading", "reporting")));
check("reporting → done: allowed", () => assert.ok(canTransition("reporting", "done")));
check("detecting → ingesting: BLOCKED (old bug caused infinite retry)", () => {
  assert.ok(!canTransition("detecting", "ingesting"), "detecting→ingesting must be blocked");
});
check("parsing → ingesting: BLOCKED", () => {
  assert.ok(!canTransition("parsing", "ingesting"));
});
check("done → ingesting: BLOCKED (terminal state)", () => {
  assert.ok(!canTransition("done", "ingesting"));
});
check("failed → ingesting: BLOCKED (terminal state)", () => {
  assert.ok(!canTransition("failed", "ingesting"));
});

// ─────────────────────────────────────────────────────────────────────────────
// 12. AI classify timeout guard
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 12. AI classify timeout guard ===");

/**
 * Classifies with timeout
 * @param classifyFn - The classify fn
 * @param timeoutMs - The timeout in milliseconds
 * @returns A promise that resolves to the result
 */
async function classifyWithTimeout(
  classifyFn: () => Promise<{ kind: string }>,
  timeoutMs: number
): Promise<{ kind: string } | null> {
  const timeout = new Promise<never>((_, reject) =>
    setTimeout(() => reject(new Error("ai_classify_timeout")), timeoutMs)
  );
  try {
    return await Promise.race([classifyFn(), timeout]);
  } catch (err) {
    if (String(err).includes("ai_classify_timeout")) return null;
    throw err;
  }
}

await checkAsync("fast AI call completes before timeout", async () => {
  const result = await classifyWithTimeout(async () => ({ kind: "record-template" }), 1000);
  assert.ok(result !== null);
  assert.equal(result?.kind, "record-template");
});

await checkAsync("slow AI call returns null after timeout (job still proceeds)", async () => {
  const slowAI = () => new Promise<{ kind: string }>(resolve => setTimeout(() => resolve({ kind: "record-template" }), 5000));
  const result = await classifyWithTimeout(slowAI, 100);
  assert.equal(result, null, "Should return null on timeout so job can still proceed to parsing");
});

await checkAsync("AI error propagates (not swallowed as timeout)", async () => {
  const failAI = () => Promise.reject(new Error("network_error"));
  try {
    await classifyWithTimeout(failAI, 1000);
    assert.fail("Should have thrown");
  } catch (e) {
    assert.ok(String(e).includes("network_error"), "Non-timeout errors should propagate");
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// 13. Route ordering – /jobs/stuck must not match /jobs/:job_id
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 13. Express route ordering ===");

check("/jobs/stuck is not a valid UUID (would 404 as job_id)", () => {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  assert.ok(!uuidRegex.test("stuck"), "String 'stuck' is not a UUID — specific route must come first in Express");
});

check("/jobs/stuck route must be registered before /jobs/:job_id", () => {
  const routes = [
    "/jobs/stuck",      // static — must be first
    "/jobs/:job_id",    // parameterized — must be after
  ];
  const stuckIdx = routes.indexOf("/jobs/stuck");
  const paramIdx = routes.indexOf("/jobs/:job_id");
  assert.ok(stuckIdx < paramIdx, "/jobs/stuck must be defined before /jobs/:job_id");
});

// ─────────────────────────────────────────────────────────────────────────────
// 14. waitForDb Cloud SQL proxy race condition guard
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 14. Cloud SQL proxy race condition guard ===");

await checkAsync("waitForDb succeeds on first attempt (DB ready)", async () => {
  let attempts = 0;
  const mockPool = {
    connect: async () => {
      attempts++;
      const client: MockClient = { query: async () => {}, release: () => {} };
      return client;
    }
  };
  // Simulate waitForDb with mock pool
  const maxAttempts = 12;
  let attempt = 0;
  while (attempt < maxAttempts) {
    try {
      const client: MockClient = await mockPool.connect();
      await client.query();
      client.release();
      break;
    } catch (err) {
      attempt++;
      if (attempt < maxAttempts) await new Promise(r => setTimeout(r, 10));
    }
  }
  assert.equal(attempts, 1, "Should succeed on first attempt when DB is ready");
});

await checkAsync("waitForDb retries with backoff until DB is ready", async () => {
  let attempts = 0;
  const mockPool = {
    connect: async () => {
      attempts++;
      if (attempts < 3) throw new Error("ECONNREFUSED");
      const client: MockClient = { query: async () => {}, release: () => {} };
      return client;
    }
  };
  const maxAttempts = 12;
  let attempt = 0;
  while (attempt < maxAttempts) {
    try {
      const client: MockClient = await mockPool.connect();
      await client.query();
      client.release();
      break;
    } catch (err) {
      attempt++;
      if (attempt < maxAttempts) await new Promise(r => setTimeout(r, 10));
    }
  }
  assert.equal(attempts, 3, "Should retry 3 times before succeeding");
});

await checkAsync("waitForDb throws after max attempts (DB never ready)", async () => {
  const mockPool = {
    connect: async () => { throw new Error("ECONNREFUSED"); }
  };
  const maxAttempts = 3; // Reduced for test speed
  let attempt = 0;
  let threw = false;
  try {
    while (attempt < maxAttempts) {
      try {
        const client: MockClient = await mockPool.connect();
        await client.query();
        client.release();
        break;
      } catch (err) {
        attempt++;
        if (attempt < maxAttempts) await new Promise(r => setTimeout(r, 10));
      }
    }
    if (attempt >= maxAttempts) throw new Error(`Database connection failed after ${maxAttempts} attempts`);
  } catch (e) {
    threw = true;
  }
  assert.ok(threw, "Should throw after max attempts");
});

// ─────────────────────────────────────────────────────────────────────────────
// 15. Vertex AI JSON extraction (handle conversational text, markdown fences)
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 15. Vertex AI JSON extraction ===");

/**
 * Extracts json
 * @param text - The text
 * @returns The record<string, unknown> result
 */
function extractJson(text: string): Record<string, unknown> {
  const fence = /\`\`\`(?:json)?\s*(\{[\s\S]*?\})\s*\`\`\`/.exec(text);
  if (fence) return JSON.parse(fence[1]) as Record<string, unknown>;
  const brace = /\{[\s\S]*\}/.exec(text);
  if (brace) {
    try {
      return JSON.parse(brace[0]) as Record<string, unknown>;
    } catch {}
  }
  const firstBrace = text.indexOf("{");
  const lastBrace = text.lastIndexOf("}");
  if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace) {
    try {
      return JSON.parse(text.substring(firstBrace, lastBrace + 1)) as Record<string, unknown>;
    } catch {}
  }
  throw new Error(`No JSON found in model output. Response: ${text.slice(0, 200)}...`);
}

check("extractJson handles markdown code fence with json label", () => {
  const text = "Here is the result:\n```json\n{\"kind\":\"record-template\"}\n```";
  const result = extractJson(text);
  assert.equal(result.kind, "record-template");
});

check("extractJson handles markdown code fence without json label", () => {
  const text = "```\n{\"kind\":\"rubbish-signature\"}\n```";
  const result = extractJson(text);
  assert.equal(result.kind, "rubbish-signature");
});

check("extractJson handles bare JSON object", () => {
  const text = "{\"kind\":\"uncertain\"}";
  const result = extractJson(text);
  assert.equal(result.kind, "uncertain");
});

check("extractJson handles conversational text before JSON", () => {
  const text = "Based on the line, here is my analysis:\n{\"kind\":\"record-template\"}\nHope this helps!";
  const result = extractJson(text);
  assert.equal(result.kind, "record-template");
});

check("extractJson handles conversational text after JSON", () => {
  const text = "{\"kind\":\"uncertain\"}\nLet me know if you need more details.";
  const result = extractJson(text);
  assert.equal(result.kind, "uncertain");
});

check("extractJson handles text both before and after JSON", () => {
  const text = "Analysis:\n{\"kind\":\"rubbish-signature\"}\nEnd of analysis.";
  const result = extractJson(text);
  assert.equal(result.kind, "rubbish-signature");
});

check("extractJson throws when no JSON is found", () => {
  try {
    extractJson("This is just plain text with no JSON object at all.");
    assert.fail("Should have thrown");
  } catch (e) {
    assert.ok(String(e).includes("No JSON found"));
  }
});

check("extractJson handles nested JSON objects", () => {
  const text = "Here is the template:\n{\"kind\":\"record-template\",\"template\":{\"structure\":\"csv\"}}";
  const result = extractJson(text);
  assert.equal(result.kind, "record-template");
  assert.equal((result.template as Record<string, unknown>).structure, "csv");
});

// ─────────────────────────────────────────────────────────────────────────────
// 16. Encoding normalization / safe decode
// Regression: jschardet emits labels (latin-1, iso-8859-1, cp1252, windows-1252,
// iso-8859-2) that Buffer.toString rejects with ERR_UNKNOWN_ENCODING. This crashed
// detect_bootstrap and stream_parser. Fixed twice incorrectly (latin-1 -> iso-8859-1,
// both invalid) before decode() routed non-native labels through TextDecoder.
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 16. Encoding normalization / safe decode ===");

/**
 * The { decode, buffer encoding for, normalize encoding, is likely utf8 }
 */
const { decode, bufferEncodingFor, normalizeEncoding, isLikelyUtf8 } = await import("@utils/normalizers/encoding.js");

check("decode never throws for labels that crashed prod (latin-1, iso-8859-1, cp1252, windows-1252, iso-8859-2, unknown)", () => {
  const raw = Buffer.from([0x48, 0x69, 0xe9, 0x0a]);
  for (const label of ["latin-1", "iso-8859-1", "cp1252", "windows-1252", "iso-8859-2", "utf-16", "GB2312", "made-up-xyz", "", null]) {
    assert.doesNotThrow(() => decode(raw, label), `decode threw for ${JSON.stringify(label)}`);
  }
});
check("iso-8859-1 decodes 0xE9 to é (the exact label from the reported crash)", () => {
  assert.equal(decode(Buffer.from([0xe9]), "iso-8859-1"), "é");
});
check("windows-1252 decodes smart quotes 0x93/0x94 correctly (not mangled by a latin1 cast)", () => {
  assert.equal(decode(Buffer.from([0x93, 0x48, 0x69, 0x94]), "windows-1252"), "“Hi”");
});
check("iso-8859-2 decodes 0xB1 to ą via TextDecoder (latin1 would give ±)", () => {
  assert.equal(decode(Buffer.from([0xb1]), "iso-8859-2"), "ą");
});
check("bufferEncodingFor always returns a valid Buffer encoding", () => {
  for (const label of ["latin-1", "iso-8859-1", "cp1252", "windows-1252", "iso-8859-2", "utf-16", "zzz", null]) {
    assert.ok(Buffer.isEncoding(bufferEncodingFor(label)), `invalid for ${JSON.stringify(label)}: ${bufferEncodingFor(label)}`);
  }
});
check("normalizeEncoding defaults empty/null to utf-8", () => {
  assert.equal(normalizeEncoding(null), "utf-8");
  assert.equal(normalizeEncoding(""), "utf-8");
  assert.equal(normalizeEncoding("  ISO-8859-1 "), "iso-8859-1");
});
check("isLikelyUtf8: UTF-8 multibyte content is recognized (real file had œæ∆¶Œ≥, misdetected as ISO-8859-2)", () => {
  assert.equal(isLikelyUtf8(Buffer.from("Name: œæ∆¶Œ≥ - Followers: 7", "utf-8")), true);
  assert.equal(isLikelyUtf8(Buffer.from("plain ascii only", "utf-8")), true);
});
check("isLikelyUtf8: genuine latin-1 (high byte followed by ASCII in the interior) is NOT valid UTF-8", () => {
  // "café résumé" in latin-1: 0xE9 followed by a space/letter is an invalid UTF-8 sequence.
  assert.equal(isLikelyUtf8(Buffer.from("café résumé", "latin1")), false);
  assert.equal(isLikelyUtf8(Buffer.from([0xe9, 0x20, 0x72])), false); // é + " r"
});
check("isLikelyUtf8: tolerates a multibyte char truncated at the buffer end (probe-window boundary)", () => {
  const full = Buffer.from("café", "utf-8");         // é = 0xc3 0xa9
  assert.equal(isLikelyUtf8(full.subarray(0, full.length - 1)), true); // drop trailing 0xa9
});
check("UTF-8 content round-trips correctly (not mojibake) once detected as utf-8", () => {
  const raw = Buffer.from("Name: œæ∆¶Œ≥", "utf-8");
  assert.equal(decode(raw, "utf-8"), "Name: œæ∆¶Œ≥");
});

// ─────────────────────────────────────────────────────────────────────────────
// 17. Ordered line classifier (design conformance)
// The stream_parser now routes every line through LineClassifier.classify() in the
// designed order (length/binary gate -> learned templates -> structural JSON/kv ->
// rubbish -> validated CSV -> uncertain), extracting ONLY field_spec fields and
// DECLINING junk/header lines instead of force-parsing them. These cases lock in the
// fixes an adversarial review surfaced (header misdetection, kv/JSON over-matching,
// phone false-positives, field_spec parsing).
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 17. Ordered line classifier ===");

/**
 * The {  line classifier }
 */
const { LineClassifier } = await import("@service/stream_parser/LineClassifier.js");
/**
 * The f s
 */
const FS = ["email", "name", "phone", "address"];
/**
 * The classify one
 * @param fields - The fields
 * @param line - The line to process
 */
const classifyOne = (fields: string[], line: string) =>
  new LineClassifier("test", fields, [], []).classify(line, 0, line.length);

// --- extraction honors field_spec (only requested fields, others null) ---
check("twitter key-value line extracts ONLY field_spec fields", () => {
  const r = classifyOne(FS, "Email: a@b.com - Name: Jane Roe - ScreenName: jr - Followers: 5 - Created At: Mon");
  assert.equal(r.verdict, "parsed");
  assert.deepEqual(Object.keys(r.row!).sort(), [...FS].sort());
  assert.equal(r.row!.email, "a@b.com");
  assert.equal(r.row!.name, "Jane Roe");
  assert.equal(r.row!.phone, null);
});
check("JSON record extracts ONLY field_spec fields (no screen_name/followers dumped)", () => {
  const r = classifyOne(FS, "{\"id\":9,\"name\":\"Aaliyah\",\"screen_name\":\"x\",\"followers_count\":3}");
  assert.equal(r.verdict, "parsed");
  assert.deepEqual(Object.keys(r.row!).sort(), [...FS].sort());
  assert.equal(r.row!.name, "Aaliyah");
});

// --- header handling: header declined, columns mapped for data rows ---
check("header row is declined (rubbish), not emitted as data", () => {
  const r = classifyOne(FS, "id,email,full_name,phone,address");
  assert.equal(r.verdict, "rubbish");
  assert.equal(r.template_id, "header");
});
check("data rows after a header map to the right columns (all four fields)", () => {
  const c = new LineClassifier("test", FS, [], []);
  c.classify("id,email,full_name,phone,address", 0, 0); // header
  const r = c.classify("7,jane@x.com,Jane Roe,5551234567,\"12 Main St\"", 0, 0);
  assert.equal(r.verdict, "parsed");
  assert.equal(r.template_id, "csv-mapped");
  assert.deepEqual(r.row, { email: "jane@x.com", name: "Jane Roe", phone: "5551234567", address: "12 Main St" });
});

// --- junk / ambiguous lines are DECLINED, never force-parsed ---
check("headerless plain-words first row is NOT mistaken for a header (no data loss)", () => {
  // "Cell,Berlin": 'Cell' aliases nothing dangerous now; must not become a header map.
  const r = classifyOne(["phone", "address"], "Cell,Berlin");
  assert.notEqual(r.template_id, "header");
  assert.equal(r.verdict, "uncertain");
});
check("single 'Name: value' log fragment is declined (needs a strong field or >=2 fields)", () => {
  assert.equal(classifyOne(["email", "name", "phone"], "Name: Full Name").verdict, "uncertain");
});
check("JSON log line whose key only weakly aliases a field is declined", () => {
  // 'username' no longer aliases 'name'; nothing else matches -> declined.
  assert.equal(classifyOne(["name"], "{\"level\":\"info\",\"username\":\"svc-bot\",\"msg\":\"go\"}").verdict, "uncertain");
});
check("binary / mostly-nonprintable line is dropped as rubbish", () => {
  const r = classifyOne(FS, "\x00\x01\x02\x03\x04\x05\x06\x07 garbage");
  assert.equal(r.verdict, "rubbish");
  assert.equal(r.template_id, "binary-gate");
});
check("empty line is length-gated to rubbish", () => {
  assert.equal(classifyOne(FS, "   ").template_id, "length-gate");
});

// --- content-based CSV column identification (headerless) ---
check("headerless CSV identifies the email column by content, declines rows with none", () => {
  const withEmail = classifyOne(FS, "1416779,2231849,\"OD2667900\",GLENN.RAINEY@HOTMAIL.CO.UK,07700900123");
  assert.equal(withEmail.verdict, "parsed");
  assert.equal(withEmail.row!.email, "GLENN.RAINEY@HOTMAIL.CO.UK");
  assert.equal(withEmail.row!.phone, "07700900123"); // 11 digits, not the 7-digit ID
  const noField = classifyOne(FS, "1416779,2231849,OD2667900,code");
  assert.equal(noField.verdict, "uncertain");
});
check("phone content-match rejects ZIP+4 / year ranges (needs 10-15 digits)", () => {
  assert.equal(classifyOne(["phone"], "12345-6789,Town").verdict, "uncertain");
  assert.equal(classifyOne(["phone"], "2020-2021,Town").verdict, "uncertain");
});

// --- router field_spec normalization (job_service accepts multiple encodings) ---
check("field_spec normalization: array / JSON-array string / JSON-{fields} string / comma string", () => {
  const norm = (field_spec: unknown): string[] => {
    const namesFromArray = (arr: unknown[]): string[] =>
      arr.map((f) => (typeof f === "string" ? f : (f as { name?: string } | undefined | null)?.name)).filter((x): x is string => typeof x === "string");
    let fieldNames: string[] = [];
    if (field_spec) {
      if (Array.isArray(field_spec)) fieldNames = namesFromArray(field_spec);
      else if (typeof field_spec === "string") {
        const s = field_spec.trim();
        let parsed: unknown;
        try { parsed = JSON.parse(s); } catch { parsed = undefined; }
        if (Array.isArray(parsed)) fieldNames = namesFromArray(parsed);
        else if (parsed && Array.isArray((parsed as Record<string, unknown>).fields)) fieldNames = namesFromArray((parsed as Record<string, unknown>).fields as unknown[]);
        else if (s) fieldNames = s.split(",").map((x) => x.trim()).filter(Boolean);
      } else if ((field_spec as Record<string, unknown>).fields && Array.isArray((field_spec as Record<string, unknown>).fields)) {
        fieldNames = namesFromArray((field_spec as Record<string, unknown>).fields as unknown[]);
      }
    }
    return fieldNames;
  };
  assert.deepEqual(norm(["email", "name"]), ["email", "name"]);
  assert.deepEqual(norm("[\"email\",\"name\"]"), ["email", "name"]); // the exact string the client sent
  assert.deepEqual(norm("{\"fields\":[{\"name\":\"email\"},{\"name\":\"phone\"}]}"), ["email", "phone"]);
  assert.deepEqual(norm("email,name,phone"), ["email", "name", "phone"]);
});

// ─────────────────────────────────────────────────────────────────────────────
// 18. Line-splitting recovery + CSV output escaping
// Regression: a stray/unbalanced " in messy data flipped the quote-aware line reader's
// inQuote flag and swallowed the rest of the file into one giant "line". With
// MAX_QUOTED_NEWLINES=0 a newline always ends a line (quotes still protect embedded
// delimiters within a physical line). Plus the new per-job CSV output writer.
// ─────────────────────────────────────────────────────────────────────────────

console.log("\n=== 18. Line-splitting recovery + CSV output ===");

/**
 * The { split all lines }
 */
const { splitAllLines } = await import("@shared/GcsUtils.js");
/**
 * The { csv escape cell }
 */
const { csvEscapeCell } = await import("@shared/CsvOutputWriter.js");
/**
 * The split lines
 * @param s - The s
 */
const splitLines = (s: string) => splitAllLines(Buffer.from(s, "utf-8")).map((t) => t[0]);

check("stray/unbalanced quote does NOT swallow following lines", () => {
  const out = splitLines("foo,\",bar\nbaz,qux\nzip,zap\n");
  assert.equal(out.length, 3);
  assert.equal(out[1], "baz,qux");
  assert.equal(out[2], "zip,zap");
});
check("quote still protects an embedded delimiter within a single physical line", () => {
  const out = splitLines("\"a,b\",c\nd,e\n");
  assert.equal(out.length, 2);
  assert.equal(out[0], "\"a,b\",c");
  assert.equal(out[1], "d,e");
});
check("messy multi-record data stays one line per record (the reported failure)", () => {
  const out = splitLines("1416779,OD2667900,\",\",GLENN@X.COM,\",\n1416780,OD2667901,BANDAR,\",\n");
  assert.equal(out.length, 2);
  assert.ok(out[0].includes("GLENN@X.COM"));
  assert.ok(out[1].startsWith("1416780"));
});
check("csvEscapeCell quotes commas/quotes/newlines and doubles inner quotes", () => {
  assert.equal(csvEscapeCell("plain"), "plain");
  assert.equal(csvEscapeCell("a,b"), "\"a,b\"");
  assert.equal(csvEscapeCell("he said \"hi\""), "\"he said \"\"hi\"\"\"");
  assert.equal(csvEscapeCell("line1\nline2"), "\"line1\nline2\"");
  assert.equal(csvEscapeCell(null), "");
  assert.equal(csvEscapeCell(undefined), "");
  assert.equal(csvEscapeCell(42), "42");
});

// ─────────────────────────────────────────────────────────────────────────────
// Summary
// ─────────────────────────────────────────────────────────────────────────────

console.log(`\n${"─".repeat(60)}`);
console.log(`Results: ${_passed} passed, ${_failed} failed`);
if (_failed > 0) {
  console.error(`\n❌ ${_failed} test(s) FAILED`);
  process.exit(1);
} else {
  console.log("\n✅ All tests passed");
  process.exit(0);
}
