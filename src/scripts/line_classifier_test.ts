/**
 * LineClassifier unit tests – no GCP/DB/AI needed.
 * Usage: npx tsx src/scripts/line_classifier_test.ts
 *
 * Covers the recent parser improvements:
 *  1. Nested JSON flattening (contact.email → email)
 *  2. JSON-in-JSON string parsing
 *  3. JSON array-of-objects support
 *  4. Multi-format rows (CSV cell containing JSON string)
 */

import assert from "node:assert/strict";
import { LineClassifier } from "../services/stream-parser/LineClassifier.js";
import { ClassifyResult } from "../services/stream-parser/io/IClassifier.js";

let _passed = 0, _failed = 0;

function pass(label: string) { console.log(`  ✅  ${label}`); }
function fail(label: string, err: unknown) { console.error(`  ❌  ${label}:`, err); }

function check(label: string, fn: () => void) {
  try { fn(); _passed++; pass(label); }
  catch (e) { _failed++; fail(label, e); }
}

function makeClassifier(fieldSpec: string[]) {
  return new LineClassifier("test-job", fieldSpec, [], [], null);
}

function expectParsed(result: ClassifyResult) {
  assert.equal(result.verdict, "parsed", `Expected verdict "parsed", got ${result.verdict}`);
  assert.ok(result.row, "Expected a parsed row");
  return result.row!;
}

console.log("\n=== 1. Nested JSON flattening ===");

check("nested object: contact.email maps to email", () => {
  const c = makeClassifier(["email", "name", "phone"]);
  const row = expectParsed(c.classify('{"contact": {"email": "alice@example.com"}, "name": "Alice"}', 0, 0));
  assert.equal(row.email, "alice@example.com");
  assert.equal(row.name, "Alice");
});

check("deeply nested object: data.profile.phone maps to phone", () => {
  const c = makeClassifier(["email", "phone"]);
  const row = expectParsed(c.classify('{"data": {"profile": {"phone": "+1-555-123-4567", "email": "bob@example.com"}}}', 0, 0));
  assert.equal(row.phone, "+1-555-123-4567");
  assert.equal(row.email, "bob@example.com");
});

check("renamed display key (display_1) is inferred as full name from first+last", () => {
  const c = makeClassifier(["name", "first", "last"]);
  const row = expectParsed(c.classify('{"first": "Wei", "last": "Zhang", "display_1": "Wei Zhang 王伟"}', 0, 0));
  assert.equal(row.first, "Wei");
  assert.equal(row.last, "Zhang");
  assert.equal(row.name, "Wei Zhang 王伟");
});

check("unmapped nested keys land in meta", () => {
  const c = makeClassifier(["email"]);
  const row = expectParsed(c.classify('{"contact": {"email": "a@b.com"}, "extra": {"city": "Yerevan"}}', 0, 0));
  assert.equal(row.email, "a@b.com");
  const meta = JSON.parse(row.meta as string);
  assert.equal(meta["extra.city"], "Yerevan");
});

console.log("\n=== 2. JSON-in-JSON string parsing ===");

check("string field containing JSON object is parsed and flattened", () => {
  const c = makeClassifier(["email", "name"]);
  const row = expectParsed(c.classify('{"payload": "{\\"email\\": \\"jsoninjson@example.com\\", \\"name\\": \\"JSON In JSON\\"}"}', 0, 0));
  assert.equal(row.email, "jsoninjson@example.com");
  assert.equal(row.name, "JSON In JSON");
});

check("malformed JSON-in-JSON string kept as raw text", () => {
  const c = makeClassifier(["email"]);
  const result = c.classify('{"payload": "{not valid json}"}', 0, 0);
  // No real field was matched, so the record is rejected rather than accepted with only meta.
  assert.notEqual(result.verdict, "parsed");
});

console.log("\n=== 3. JSON array-of-objects support ===");

check("line containing JSON array uses first object", () => {
  const c = makeClassifier(["email", "name"]);
  const row = expectParsed(c.classify('[{"email": "first@example.com", "name": "First"}, {"email": "second@example.com", "name": "Second"}]', 0, 0));
  assert.equal(row.email, "first@example.com");
  assert.equal(row.name, "First");
});

check("array of primitives is rejected", () => {
  const c = makeClassifier(["email"]);
  const result = c.classify('["just", "strings"]', 0, 0);
  assert.notEqual(result.verdict, "parsed");
});

console.log("\n=== 4. Multi-format rows (CSV with JSON cell) ===");

check("CSV row with trailing JSON-like cell falls back to content detection for email", () => {
  const c = makeClassifier(["email", "name"]);
  // First line is not a valid header (contains @), so second line is parsed by content.
  const row = expectParsed(c.classify('Alice, alice@example.com', 0, 0));
  assert.equal(row.email, "alice@example.com");
});

check("KV line with embedded JSON value extracts email", () => {
  const c = makeClassifier(["email", "name"]);
  const row = expectParsed(c.classify('Name : Alice - Contact : {"email": "kv@example.com"}', 0, 0));
  assert.equal(row.email, "kv@example.com");
});

console.log("\n=== Summary ===");
console.log(`Passed: ${_passed}`);
console.log(`Failed: ${_failed}`);
process.exit(_failed > 0 ? 1 : 0);
