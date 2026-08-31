/**
 * One-off demo script: feeds a few real rows from the user's sample CSV through
 * LineClassifierServiceImpl to show the actual extracted output row, proving that
 * the "_source" JSON column is now split into "_source.AGE", "_source.BIRTHDAY",
 * "_source.BPLACE" (etc.) columns instead of being kept as one raw JSON blob.
 *
 * Run with: npx tsx scripts/demo_source_json_expansion.ts
 */

import { LineClassifierServiceImpl } from "@service/stream-parser/impl/LineClassifierServiceImpl.js";

const fieldSpec = ["_id", "_source.AGE", "_source.BIRTHDAY", "_source.BPLACE", "_type", "sort", "meta"];
const classifier = LineClassifierServiceImpl.getInstance("demo-source-json", fieldSpec, [], []);

const headerLine = "_id,_source,_type,sort,meta";
const dataLines = [
  `AXtaNMttjcEAuoI0T2av,"{""AGE"":63,""BIRTHDAY"":""1958"",""BPLACE"":""辽宁省盘锦市兴隆台区"",""HEIGHT"":""160"",""IDNO"":""211103195812082329"",""IDTYPE"":""01"",""PROF"":""保育员/幼儿"",""QUERY_STRING"":""  辽宁省盘锦市兴隆台区   63 58 1958 "",""RNAME"":""王乃芳"",""SEX"":""女""}",a,0,"{""_index"":""person_address_label_info_master"",""_score"":null}"`,
  `AXtaK4rnFHhJ2Qrxsqvr,"{""AGE"":100,""BIRTHDAY"":""1921"",""BPLACE"":""湖北省随州市"",""HEIGHT"":""152"",""IDNO"":""42900119210906326x"",""IDTYPE"":""01"",""QUERY_STRING"":""  湖北省随州市   100 21 1921 "",""RNAME"":""李芳"",""SEX"":""女""}",a,0,"{""_index"":""person_address_label_info_master"",""_score"":null}"`,
  `AXtaVxb2Mo41myuBiE2Q,"{""AGE"":84,""BIRTHDAY"":""1937"",""IDNO"":""120723193704130019"",""IDTYPE"":""01"",""NATION"":""汉"",""QUERY_STRING"":""   汉  84 37 1937 "",""RNAME"":""兰吴"",""SEX"":""男""}",a,0,"{""_index"":""person_address_label_info_master"",""_score"":null}"`,
];

console.log("--- header line ---");
console.log(JSON.stringify(classifier.classify(headerLine, 0, headerLine.length)));

for (const line of dataLines)
{
  console.log("\n--- data line ---");
  const verdict = classifier.classify(line, 0, line.length);
  console.log("verdict:", verdict.verdict, "template_id:", verdict.template_id);
  console.log("row:", JSON.stringify(verdict.row, null, 2));
}
