/**
 * Unit tests for LineClassifierServiceImpl's JSON flattening behavior:
 *
 * - A single JSON object (or an array wrapping exactly one object) under a
 *   key is flattened into dot-notation columns so its leaves can still match
 *   user-selected field_spec columns (e.g. "_source": {AGE:...} -> "_source.AGE").
 * - Multiple JSON records under one key (array of 2+ objects) are kept
 *   intact as a single column instead of being exploded into "key[0].x",
 *   "key[1].x", ... regardless of whether the key itself is one of the
 *   user-selected field_spec columns.
 * - Whichever headers the caller puts in field_spec are extracted as
 *   columns; everything else (including intact multi-record arrays) is
 *   folded into the "meta" column as JSON.
 *
 * Run with: npx tsx --test tests/line-classifier-json.test.ts
 */

import test from "node:test";
import assert from "node:assert/strict";
import { LineClassifierServiceImpl } from "@service/stream-parser/impl/LineClassifierServiceImpl.js";

let jobCounter = 0;

/**
 * Builds a fresh LineClassifierServiceImpl bound to a unique job id (the
 * class is a per-jobId singleton) so each test gets its own instance with
 * its own field_spec, instead of reusing a cached instance from a prior test.
 */
function makeClassifier(fieldSpec: string[]): LineClassifierServiceImpl
{
  jobCounter += 1;
  return LineClassifierServiceImpl.getInstance(`test-job-${jobCounter}`, fieldSpec, [], []);
}

/**
 * Narrow accessor for the private flattenObject/extractFromObject methods
 * under test. Private is a compile-time-only construct; at runtime these
 * are ordinary methods on the instance.
 */
type ClassifierInternals = {
  flattenObject(obj: Record<string, unknown>, prefix?: string): Record<string, unknown>;
  extractFromObject(
    rawObj: Record<string, unknown>,
    templateId: string,
    fieldSpecOverride?: string[],
    loose?: boolean
  ): { row: Record<string, unknown>; template_id: string; ambiguous?: boolean } | null;
};

function internals(classifier: LineClassifierServiceImpl): ClassifierInternals
{
  return classifier as unknown as ClassifierInternals;
}

test("flattenObject: single nested JSON object is flattened into dot-notation columns", () =>
{
  const classifier = makeClassifier(["AGE", "BIRTHDAY", "BPLACE", "meta"]);
  const obj = { _source: { AGE: 30, BIRTHDAY: "1990-01-01", BPLACE: "NY" } };

  const flat = internals(classifier).flattenObject(obj);

  assert.equal(flat["_source.AGE"], 30);
  assert.equal(flat["_source.BIRTHDAY"], "1990-01-01");
  assert.equal(flat["_source.BPLACE"], "NY");
  assert.equal(flat["_source"], undefined);
});

test("flattenObject: multiple JSON records under a key not in field_spec stay as one column", () =>
{
  const classifier = makeClassifier(["fullname", "email", "meta"]);
  const experience = [{ company: "A" }, { company: "B" }];
  const obj = { fullname: "John Doe", email: "john@example.com", experience };

  const flat = internals(classifier).flattenObject(obj);

  assert.deepEqual(flat["experience"], experience);
  assert.equal(flat["experience[0].company"], undefined);
  assert.equal(flat["experience[1].company"], undefined);
});

test("flattenObject: multiple JSON records under a key that matches field_spec also stay as one column", () =>
{
  const classifier = makeClassifier(["fullname", "experience", "meta"]);
  const experience = [{ company: "A" }, { company: "B" }];
  const obj = { fullname: "John Doe", experience };

  const flat = internals(classifier).flattenObject(obj);

  assert.deepEqual(flat["experience"], experience);
});

test("flattenObject: a single-object array is flattened like a plain nested object", () =>
{
  const classifier = makeClassifier(["fullname", "skill", "meta"]);
  const obj = { fullname: "Jane", tags: [{ skill: "JS" }] };

  const flat = internals(classifier).flattenObject(obj);

  assert.equal(flat["tags.skill"], "JS");
  assert.equal(flat["tags"], undefined);
});

test("flattenObject: JSON-string-encoded multi-record array stays as one column", () =>
{
  const classifier = makeClassifier(["fullname", "meta"]);
  const details = [{ skill: "JS" }, { skill: "TS" }];
  const obj = { fullname: "John Doe", details: JSON.stringify(details) };

  const flat = internals(classifier).flattenObject(obj);

  assert.equal(JSON.stringify(flat["details"]), JSON.stringify(details));
});

test("flattenObject: JSON-string-encoded single object is flattened into dot-notation columns", () =>
{
  const classifier = makeClassifier(["fullname", "skill", "meta"]);
  const obj = { fullname: "Jane", info: JSON.stringify({ skill: "JS" }) };

  const flat = internals(classifier).flattenObject(obj);

  assert.equal(flat["info.skill"], "JS");
  assert.equal(flat["info"], undefined);
});

test("extractFromObject: unselected multi-record array is folded whole into meta", () =>
{
  const classifier = makeClassifier(["fullname", "meta"]);
  const experience = [{ company: "A" }, { company: "B" }];
  const obj = { fullname: "John Doe", email: "john@x.com", experience };

  const extracted = internals(classifier).extractFromObject(obj, "json");

  assert.ok(extracted, "expected extraction to succeed");
  assert.equal(extracted!.row.fullname, "John Doe");

  const meta = JSON.parse(extracted!.row.meta as string) as Record<string, unknown>;
  assert.equal(meta.email, "john@x.com");
  assert.deepEqual(meta.experience, experience);
});

test("extractFromObject: selecting the array field's own name extracts it intact instead of meta", () =>
{
  const classifier = makeClassifier(["fullname", "experience", "meta"]);
  const experience = [{ company: "A" }, { company: "B" }];
  const obj = { fullname: "John Doe", experience };

  const extracted = internals(classifier).extractFromObject(obj, "json");

  assert.ok(extracted, "expected extraction to succeed");
  assert.equal(extracted!.row.fullname, "John Doe");
  assert.deepEqual(extracted!.row.experience, experience);

  const meta = extracted!.row.meta ? JSON.parse(extracted!.row.meta as string) as Record<string, unknown> : {};
  assert.equal(meta.experience, undefined);
});

test("extractFromObject: a nested single-JSON-object's leaves map to user-selected columns", () =>
{
  const classifier = makeClassifier(["AGE", "BIRTHDAY", "BPLACE", "meta"]);
  const obj = { _source: { AGE: 30, BIRTHDAY: "1990-01-01", BPLACE: "NY" } };

  const extracted = internals(classifier).extractFromObject(obj, "json");

  assert.ok(extracted, "expected extraction to succeed");
  assert.equal(extracted!.row.AGE, 30);
  assert.equal(extracted!.row.BIRTHDAY, "1990-01-01");
  assert.equal(extracted!.row.BPLACE, "NY");
});

test("extractFromObject: user selecting fewer headers pushes the rest into meta, not dropped", () =>
{
  const classifier = makeClassifier(["fullname", "meta"]);
  const obj = { fullname: "John Doe", email: "john@x.com", phone: "1234567890" };

  const extracted = internals(classifier).extractFromObject(obj, "json");

  assert.ok(extracted, "expected extraction to succeed");
  assert.equal(extracted!.row.fullname, "John Doe");
  assert.equal(extracted!.row.email, undefined);
  assert.equal(extracted!.row.phone, undefined);

  const meta = JSON.parse(extracted!.row.meta as string) as Record<string, unknown>;
  assert.equal(meta.email, "john@x.com");
  assert.equal(meta.phone, "1234567890");
});
