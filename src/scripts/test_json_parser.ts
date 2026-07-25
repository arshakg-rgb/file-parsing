import fs from "node:fs";

const INPUT_FILE = process.env.INPUT || "test.json";
const OUTPUT_FILE = process.env.OUTPUT || "test_json_output.csv";

const raw = fs.readFileSync(INPUT_FILE, "utf8");
const data = JSON.parse(raw);

// Unwrap a string that contains JSON (single or double encoded).
function unwrapJsonString(v: unknown): unknown {
  if (typeof v !== "string") return v;
  const t = v.trim();
  if (t.length >= 2 && t[0] === '"' && t[t.length - 1] === '"') {
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

// Flatten a value into dotted/array keys. Preserves leaf scalars, stringifies arrays.
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

function csvEscapeCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = typeof v === "object" ? JSON.stringify(v) : String(v);
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

function cellValue(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "string") return v;
  return JSON.stringify(v);
}

const records: { label: string; row: Record<string, unknown> }[] = [];

function addRecord(label: string, value: unknown, prefix = label) {
  if (value === null || value === undefined) return;
  const row = flatten(value, prefix);
  if (Object.keys(row).length > 0) {
    records.push({ label, row });
  }
}

if (data.export_metadata) addRecord("export_metadata", data.export_metadata);
if (data.profile) addRecord("profile", data.profile);
for (const item of data.connections || []) {
  const id = item?.id ?? item?.connection_id ?? "?";
  addRecord(`connection.${id}`, item, "connection");
}
for (const [threadId, thread] of Object.entries(data.messages || {})) {
  addRecord(`messages.${threadId}`, thread, `messages.${threadId}`);
}
for (const [i, item] of (data.activity_log || []).entries()) {
  addRecord(`activity_log[${i}]`, item, `activity_log[${i}]`);
}
for (const [i, item] of (data.education_multi_format_rows || []).entries()) {
  addRecord(`education[${i}]`, item, `education[${i}]`);
}
if (data.quirky_edge_cases) addRecord("quirky_edge_cases", data.quirky_edge_cases);

const columnsSet = new Set<string>();
for (const { row } of records) {
  for (const k of Object.keys(row)) columnsSet.add(k);
}
const columns = Array.from(columnsSet);

const csvRows: string[] = [columns.map(csvEscapeCell).join(",")];
for (const { label, row } of records) {
  const values = columns.map((c) => csvEscapeCell(cellValue(row[c])));
  csvRows.push(values.join(","));
}

fs.writeFileSync(OUTPUT_FILE, csvRows.join("\n"), "utf8");
console.log(`\nWrote ${OUTPUT_FILE}`);
console.log(`  records: ${records.length}`);
console.log(`  columns: ${columns.length}`);
console.log(`  first columns: ${columns.slice(0, 10).join(", ")}${columns.length > 10 ? "..." : ""}`);
