import fs from "node:fs";
import { LineClassifier } from "../services/stream_parser/LineClassifier.js";
import {aiClassifierService} from "@service/ai_classifier/AiClassifierServiceHandler.js";
const INPUT_FILE = process.env.INPUT || "test.json";
const OUTPUT_FILE = process.env.OUTPUT || "test_json_output.csv";

const raw = fs.readFileSync(INPUT_FILE, "utf8");
const data = JSON.parse(raw);

function csvEscapeCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = typeof v === "object" ? JSON.stringify(v) : String(v);
  return /[",\r\n]/.test(s) ? "\"" + s.replace(/"/g, "\"\"") + "\"" : s;
}

const records: { label: string; line: string }[] = [];

function addRecord(label: string, value: unknown) {
  if (value !== null && value !== undefined) {
    records.push({ label, line: JSON.stringify(value) });
  }
}

if (data.export_metadata) addRecord("export_metadata", data.export_metadata);
if (data.profile) addRecord("profile", data.profile);
for (const item of data.connections || []) addRecord(`connection.${item?.id ?? item?.connection_id ?? "?"}`, item);
for (const [threadId, thread] of Object.entries(data.messages || {})) {
  addRecord(`messages.${threadId}`, thread);
}
for (const [i, item] of (data.activity_log || []).entries()) {
  addRecord(`activity_log[${i}]`, item);
}
for (const [i, item] of (data.education_multi_format_rows || []).entries()) {
  addRecord(`education[${i}]`, item);
}
if (data.quirky_edge_cases) addRecord("quirky_edge_cases", data.quirky_edge_cases);

async function classifyMode() {
  let fieldSpec = process.env.FIELD_SPEC
    ? process.env.FIELD_SPEC.split(",").map((s) => s.trim())
    : null;

  if (!fieldSpec) {
    // Always ask AI for a field spec first. If AI is unavailable, fall back to dynamic flattening.
    const samples: string[] = [];
    if (data.profile) samples.push(JSON.stringify(data.profile));
    if (data.connections?.[0]) samples.push(JSON.stringify(data.connections[0]));
    if (data.education_multi_format_rows?.[1]) samples.push(JSON.stringify(data.education_multi_format_rows[1]));
    try {
      const discovered = await aiClassifierService.discoverJsonFieldSpec(samples.length ? samples : [JSON.stringify(data)]);
      if (discovered.length > 0) {
        fieldSpec = discovered;
      }
    } catch (err) {
      console.warn("AI field discovery failed; falling back to dynamic flattening:", String(err));
    }
  }

  if (!fieldSpec) {
    dynamicFlattenMode();
    return;
  }

  const classifier = new LineClassifier("test-json-job", fieldSpec, [], [], null);
  const columns = [...fieldSpec.filter((f) => f !== "meta"), "meta"];
  const csvRows: string[] = [columns.map(csvEscapeCell).join(",")];

  const aiMode = process.env.AI_INLINE_MODE || "off";
  const aiEnabled = aiMode !== "off";
  for (const { label, line } of records) {
    let result = classifier.classify(line, 0, 0);
    // Local parser could not parse this record — ask AI if enabled.
    if (aiEnabled && result.verdict !== "parsed" && result.verdict !== "rubbish") {
      try {
        result = await classifier.classifyWithAI(line, []);
      } catch (err) {
        console.warn(`AI fallback failed for ${label}:`, String(err));
      }
    }
    if (result.verdict === "parsed" && result.row) {
      const row = result.row;
      const vals = columns.map((c) => csvEscapeCell(row[c]));
      csvRows.push(vals.join(","));
    } else {
      const vals = columns.map((c) => (c === "meta" ? csvEscapeCell(line) : ""));
      csvRows.push(vals.join(","));
    }
  }

  fs.writeFileSync(OUTPUT_FILE, csvRows.join("\n"), "utf8");
  console.log(`\nWrote ${OUTPUT_FILE}`);
  console.log(`  records: ${records.length}`);
  console.log(`  columns: ${columns.length} (${fieldSpec.join(", ")})`);
}

function dynamicFlattenMode() {
  function unwrapJsonString(v: unknown): unknown {
    if (typeof v !== "string") return v;
    const t = v.trim();
    if (t.length >= 2 && t[0] === "\"" && t[t.length - 1] === "\"") {
      try {
        const inner = JSON.parse(v);
        if (typeof inner === "string") return inner;
      } catch { /* not a wrapped string */ }
    }
    if (t.startsWith("{") || t.startsWith("[")) {
      try { return JSON.parse(v); } catch { /* keep raw */ }
    }
    return v;
  }

  function flatten(value: unknown, prefix = "", out: Record<string, unknown> = {}): Record<string, unknown> {
    if (value === null || value === undefined) return out;
    const unwrapped = unwrapJsonString(value);
    if (Array.isArray(unwrapped)) {
      if (unwrapped.length === 0) {
        out[prefix] = "[]";
      } else if (unwrapped.every((x) => x !== null && typeof x === "object" && !Array.isArray(x))) {
        for (let i = 0; i < unwrapped.length; i++) {
          flatten(unwrapped[i], prefix ? `${prefix}[${i}]` : `[${i}]`, out);
        }
      } else {
        out[prefix] = unwrapped;
      }
    } else if (typeof unwrapped === "object") {
      const obj = unwrapped as Record<string, unknown>;
      if (Object.keys(obj).length === 0) {
        out[prefix] = "{}";
      } else {
        for (const [k, v] of Object.entries(obj)) {
          const key = prefix ? `${prefix}.${k}` : k;
          flatten(v, key, out);
        }
      }
    } else {
      out[prefix] = unwrapped;
    }
    return out;
  }

  function cellValue(v: unknown): string {
    if (v === null || v === undefined) return "";
    if (typeof v === "string") return v;
    return JSON.stringify(v);
  }

  const rows: Record<string, unknown>[] = [];
  for (const { label, line } of records) {
    try {
      const parsed = JSON.parse(line);
      rows.push(flatten(parsed, label));
    } catch { /* bare string/number already stringified as line; use line as value */ }
  }

  const columnsSet = new Set<string>();
  for (const row of rows) {
    for (const k of Object.keys(row)) columnsSet.add(k);
  }
  const columns = Array.from(columnsSet);

  const csvRows: string[] = [columns.map(csvEscapeCell).join(",")];
  for (const row of rows) {
    const vals = columns.map((c) => csvEscapeCell(cellValue(row[c])));
    csvRows.push(vals.join(","));
  }

  fs.writeFileSync(OUTPUT_FILE, csvRows.join("\n"), "utf8");
  console.log(`\nWrote ${OUTPUT_FILE}`);
  console.log(`  records: ${rows.length}`);
  console.log(`  columns: ${columns.length}`);
  console.log(`  first columns: ${columns.slice(0, 10).join(", ")}${columns.length > 10 ? "..." : ""}`);
}

classifyMode().catch((err) => {
  console.error("test_json_parser failed:", err);
  process.exit(1);
});
