/**
 * Unit tests for header-mapped CSV rows where one raw column's value is itself a
 * JSON object (e.g. an Elasticsearch-style export with a "_source" column holding
 * a per-row JSON record). When the target field_spec asks for dot-notation
 * sub-fields of that column (e.g. "_source.AGE", "_source.BPLACE"), the classifier
 * must:
 *
 * - Detect the real header row and keep the RAW column list (_id, _source, _type,
 *   sort, meta) as headerParts - the physical CSV columns never change shape.
 * - Extract each dotted field_spec entry's value from the matching raw column's
 *   embedded JSON (LineClassifierServiceImpl.liftFieldsFromEmbeddedJson), keyed by
 *   leaf name (e.g. "AGE" inside "_source").
 * - Not duplicate the fully-expanded raw column's JSON blob into the "meta" catch-all.
 *
 * Run with: npx tsx --test tests/line-classifier-csv-json-column.test.ts
 */

import test from "node:test";
import assert from "node:assert/strict";
import { LineClassifierServiceImpl } from "@service/stream-parser/impl/LineClassifierServiceImpl.js";

let jobCounter = 0;

function makeClassifier(fieldSpec: string[]): LineClassifierServiceImpl
{
  jobCounter += 1;
  return LineClassifierServiceImpl.getInstance(`csv-json-col-test-${jobCounter}`, fieldSpec, [], []);
}

test("header-mapped CSV: single-JSON-object column is split into dot-notation field_spec columns", () =>
{
  const fieldSpec = ["_id", "_source.AGE", "_source.BIRTHDAY", "_source.BPLACE", "_type", "sort", "meta"];
  const classifier = makeClassifier(fieldSpec);

  const headerLine = "_id,_source,_type,sort,meta";
  const headerVerdict = classifier.classify(headerLine, 0, headerLine.length);

  assert.equal(headerVerdict.verdict, "rubbish");
  assert.equal(headerVerdict.template_id, "header");

  const dataLine = "AXtaNMttjcEAuoI0T2av,\"{\"\"AGE\"\":63,\"\"BIRTHDAY\"\":\"\"1958\"\",\"\"BPLACE\"\":\"\"NY\"\"}\",a,0,\"{\"\"_index\"\":\"\"person_address_label_info_master\"\",\"\"_score\"\":null}\"";
  const dataVerdict = classifier.classify(dataLine, 0, dataLine.length);

  assert.equal(dataVerdict.verdict, "parsed", JSON.stringify(dataVerdict));
  const row = dataVerdict.row!;

  assert.equal(row["_id"], "AXtaNMttjcEAuoI0T2av");
  assert.equal(row["_source.AGE"], 63);
  assert.equal(row["_source.BIRTHDAY"], "1958");
  assert.equal(row["_source.BPLACE"], "NY");
  assert.equal(row["_type"], "a");
  assert.equal(row["sort"], "0");
});

test("header-mapped CSV: the expanded raw column's JSON blob is not duplicated into meta", () =>
{
  const fieldSpec = ["_id", "_source.AGE", "_source.BIRTHDAY", "_source.BPLACE", "_type", "sort", "meta"];
  const classifier = makeClassifier(fieldSpec);

  const headerLine = "_id,_source,_type,sort,meta";
  classifier.classify(headerLine, 0, headerLine.length);

  const dataLine = "AXtaNMttjcEAuoI0T2av,\"{\"\"AGE\"\":63,\"\"BIRTHDAY\"\":\"\"1958\"\",\"\"BPLACE\"\":\"\"NY\"\"}\",a,0,\"{\"\"_index\"\":\"\"person_address_label_info_master\"\",\"\"_score\"\":null}\"";
  const dataVerdict = classifier.classify(dataLine, 0, dataLine.length);
  const row = dataVerdict.row!;

  assert.ok(row["meta"], "expected a meta value");
  const metaText = row["meta"] as string;
  assert.equal(metaText.includes("AGE"), false, "AGE should not leak into meta once extracted into its own column");
  assert.equal(metaText.includes("BIRTHDAY"), false, "BIRTHDAY should not leak into meta once extracted into its own column");
  assert.equal(metaText.includes("BPLACE"), false, "BPLACE should not leak into meta once extracted into its own column");
});

test("header-mapped CSV: a row missing an optional key in the JSON column leaves that field null", () =>
{
  const fieldSpec = ["_id", "_source.AGE", "_source.BIRTHDAY", "_source.BPLACE", "_type", "sort", "meta"];
  const classifier = makeClassifier(fieldSpec);

  const headerLine = "_id,_source,_type,sort,meta";
  classifier.classify(headerLine, 0, headerLine.length);

  const dataLine = "AXtaK4rnFHhJ2Qrxsqvr,\"{\"\"AGE\"\":100,\"\"BIRTHDAY\"\":\"\"1921\"\"}\",a,0,\"{\"\"_index\"\":\"\"person_address_label_info_master\"\",\"\"_score\"\":null}\"";
  const dataVerdict = classifier.classify(dataLine, 0, dataLine.length);

  assert.equal(dataVerdict.verdict, "parsed", JSON.stringify(dataVerdict));
  const row = dataVerdict.row!;

  assert.equal(row["_source.AGE"], 100);
  assert.equal(row["_source.BIRTHDAY"], "1921");
  assert.equal(row["_source.BPLACE"], null);
});
