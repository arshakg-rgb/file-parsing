/**
 * Unit tests for JsonColumnExpander against the user's real sample data (an
 * Elasticsearch-style export with "_id,_index,_source,_type,sort,meta" columns,
 * where "_source" is a large per-row JSON record and "meta" is a small
 * {"_index":...,"_score":null} JSON record).
 *
 * Run with: npx tsx --test tests/json-column-expander.test.ts
 */

import test from "node:test";
import assert from "node:assert/strict";
import { expandJsonColumns, parseCsvLine } from "@service/detect-bootstrap/impl/JsonColumnExpander.js";

const HEADERS = ["_id", "_index", "_source", "_type", "sort", "meta"];

const DATA_LINES: string[] = [
  `AXtaNMttjcEAuoI0T2av,person_address_label_info_master,"{""AGE"":63,""BIRTHDAY"":""1958"",""BPLACE"":""NY"",""HEIGHT"":""160"",""IDNO"":""211103195812082329"",""IDTYPE"":""01"",""PROF"":""x"",""QUERY_STRING"":""x"",""RNAME"":""x"",""SEX"":""F""}",a,0,"{""_score"":null}"`,
  `AXtaK4rnFHhJ2Qrxsqvr,person_address_label_info_master,"{""AGE"":100,""BIRTHDAY"":""1921"",""BPLACE"":""x"",""HEIGHT"":""152"",""IDNO"":""42900119210906326x"",""IDTYPE"":""01"",""QUERY_STRING"":""x"",""RNAME"":""x"",""SEX"":""F""}",a,0,"{""_score"":null}"`,
  `AXtaLaSiVXqVCFA1_Fei,person_address_label_info_master,"{""AGE"":9,""BIRTHDAY"":""2012"",""BPLACE"":""x"",""PHOTO"":""{\""a\"":[\""http://example.com/1.jpg\""],\""b\"":[\""http://example.com/2.jpg\""]}"",""RNAME"":""x"",""SEX"":""M""}",a,0,"{""_score"":null}"`,
  `AXtaVxb2Mo41myuBiE2Q,person_address_label_info_master,"{""AGE"":84,""BIRTHDAY"":""1937"",""IDNO"":""120723193704130019"",""IDTYPE"":""01"",""NATION"":""x"",""RNAME"":""x"",""SEX"":""M""}",a,0,"{""_score"":null}"`,
];

test("expandJsonColumns expands both _source and meta into their own leaf columns", () =>
{
  const expanded = expandJsonColumns(HEADERS, ",", DATA_LINES);

  console.log("expanded headers:", expanded);

  assert.ok(expanded.includes("_source.AGE"), `expected _source.AGE, got: ${JSON.stringify(expanded)}`);
  assert.ok(expanded.includes("_source.BIRTHDAY"));
  assert.ok(expanded.includes("meta._score"), `expected meta._score, got: ${JSON.stringify(expanded)}`);
  assert.equal(expanded.includes("_source"), false, "_source should have been replaced by its leaf columns");
});

test("diagnostic: parseCsvLine column counts per sample row", () =>
{
  for (const line of DATA_LINES)
  {
    const parts = parseCsvLine(line, ",", "\"");
    console.log(`cols=${parts.length}`, JSON.stringify(parts));
    assert.equal(parts.length, HEADERS.length, `line did not split into ${HEADERS.length} columns: ${line}`);
  }
});
