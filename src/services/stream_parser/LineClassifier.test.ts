import { describe, it, expect, vi, beforeEach } from "vitest";

// Stub heavy external dependencies before importing the module under test
vi.mock("@shared/Settings.js", () => ({
  settings: {
    AI_INLINE_MODE: "off",
    MAX_AI_CALLS_PER_JOB: 0,
    LOKI_HOST: "",
    LOKI_USERNAME: "",
    LOKI_PASSWORD: "",
  },
}));
vi.mock("@shared/TemplateRegistryService.js", () => ({
  templateRegistry: { getAllRecordTemplates: () => [], getAllRubbishTemplates: () => [] },
}));
vi.mock("@utils/logger/logger.js", () => ({
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
  Logger: class {},
}));
vi.mock("@utils/validator/safeRegex.js", () => ({
  safeRegex: (s: string) => new RegExp(s),
  safeRegexTest: (re: RegExp, s: string) => re.test(s),
}));

import { LineClassifier } from "./LineClassifier.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function make(fieldSpec: string[]) {
  return new LineClassifier("test-job", fieldSpec, [], [], null);
}

// ---------------------------------------------------------------------------
// Basic gates
// ---------------------------------------------------------------------------
describe("LineClassifier – gates", () => {
  it("drops empty lines as rubbish", () => {
    const c = make(["email", "name"]);
    expect(c.classify("", 0, 0).verdict).toBe("rubbish");
    expect(c.classify("   ", 0, 0).verdict).toBe("rubbish");
  });

  it("drops binary-heavy lines as rubbish", () => {
    const c = make(["email", "name"]);
    // line that is >30% non-printable C0 control chars
    const bin = "\x01\x02\x03\x04\x05\x06\x07\x08" + "a".repeat(10);
    expect(c.classify(bin, 0, 0).verdict).toBe("rubbish");
  });
});

// ---------------------------------------------------------------------------
// Header detection and meta extraction
// ---------------------------------------------------------------------------
describe("LineClassifier – CSV header + meta", () => {
  it("detects a TSV header and drops it", () => {
    const c = make(["email", "name", "phone"]);
    const result = c.classify("email\tname\tphone\tbirthday\tsnils", 0, 0);
    expect(result.verdict).toBe("rubbish");
    expect(result.template_id).toBe("header");
  });

  it("parses subsequent TSV rows and populates mapped fields", () => {
    const c = make(["email", "name", "phone"]);
    c.classify("email\tname\tphone\tbirthday", 0, 0); // consume header
    const r = c.classify("alice@example.com\tAlice\t+79001234567\t1990-01-01", 0, 0);
    expect(r.verdict).toBe("parsed");
    expect(r.row?.email).toBe("alice@example.com");
    expect(r.row?.name).toBe("Alice");
    expect(r.row?.phone).toBe("+79001234567");
  });

  it("puts unmapped columns into meta as JSON", () => {
    const c = make(["email", "name", "phone"]);
    c.classify("email\tname\tphone\tbirthday\tsnils", 0, 0); // header
    const r = c.classify("alice@example.com\tAlice\t+79001234567\t1990-01-01\t123-456-789 00", 0, 0);
    expect(r.verdict).toBe("parsed");
    const meta = JSON.parse(r.row?.meta as string);
    expect(meta.birthday).toBe("1990-01-01");
    expect(meta.snils).toBe("123-456-789 00");
  });

  it("sets meta to null when all columns are mapped", () => {
    const c = make(["email", "name", "phone"]);
    c.classify("email\tname\tphone", 0, 0); // header
    const r = c.classify("alice@example.com\tAlice\t+79001234567", 0, 0);
    expect(r.verdict).toBe("parsed");
    expect(r.row?.meta).toBeNull();
  });

  it("omits empty-string unmapped values from meta", () => {
    const c = make(["email", "name"]);
    c.classify("email\tname\textra", 0, 0); // header
    const r = c.classify("b@b.com\tBob\t", 0, 0); // extra is empty
    expect(r.verdict).toBe("parsed");
    expect(r.row?.meta).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Comma-separated files
// ---------------------------------------------------------------------------
describe("LineClassifier – CSV comma", () => {
  it("handles comma-delimited headers", () => {
    const c = make(["email", "name"]);
    const hdr = c.classify("email,name,extra_col", 0, 0);
    expect(hdr.verdict).toBe("rubbish");
    expect(hdr.template_id).toBe("header");

    const r = c.classify("x@x.com,Xavier,some_value", 0, 0);
    expect(r.verdict).toBe("parsed");
    expect(r.row?.email).toBe("x@x.com");
    const meta = JSON.parse(r.row?.meta as string);
    expect(meta.extra_col).toBe("some_value");
  });
});

// ---------------------------------------------------------------------------
// Alias matching
// ---------------------------------------------------------------------------
describe("LineClassifier – column aliases", () => {
  it("matches header alias 'mail' → email field", () => {
    const c = make(["email", "name"]);
    c.classify("mail,fullname,phone", 0, 0); // header with aliases
    const r = c.classify("y@y.com,Yves,+1555", 0, 0);
    expect(r.verdict).toBe("parsed");
    expect(r.row?.email).toBe("y@y.com");
  });
});

// ---------------------------------------------------------------------------
// Semicolon delimiter
// ---------------------------------------------------------------------------
describe("LineClassifier – semicolon delimiter", () => {
  it("handles semicolon-delimited data", () => {
    const c = make(["email", "name", "phone"]);
    c.classify("email;name;phone;dob", 0, 0);
    const r = c.classify("z@z.com;Zara;+49123;1985-06-15", 0, 0);
    expect(r.verdict).toBe("parsed");
    expect(r.row?.email).toBe("z@z.com");
    const meta = JSON.parse(r.row?.meta as string);
    expect(meta.dob).toBe("1985-06-15");
  });
});

// ---------------------------------------------------------------------------
// CsvOutputWriter integration smoke test
// ---------------------------------------------------------------------------
import { CsvOutputWriter } from "@shared/CsvOutputWriter.js";
import fs from "fs";
import os from "os";
import path from "path";

let capturedBody: Buffer | null = null;
const mockPutObject = vi.fn(async (_bucket: string, _key: string, body: Buffer) => {
  capturedBody = body;
});

vi.mock("@utils/cache/FirestoreCacheUtils.js", () => ({
  default: { getInstance: () => ({ putObject: mockPutObject }) },
}));
vi.mock("@config/system-config/Config.js", () => ({
  default: { getInstance: () => ({ settings: { DATA_BUCKET: "test-bucket" } }) },
}));

describe("CsvOutputWriter – meta column", () => {
  beforeEach(() => { capturedBody = null; });

  it("always writes meta as the last column header", async () => {
    const jobId = "unit-test-" + Date.now();
    const writer = new CsvOutputWriter(jobId, ["email", "name"]);
    writer.addRow({ email: "a@a.com", name: "Alice", meta: JSON.stringify({ birthday: "2000-01-01" }) }, 1);
    writer.addRow({ email: "b@b.com", name: "Bob", meta: null }, 2);
    const gsPath = await writer.flush();
    expect(gsPath).toBe(`gs://test-bucket/output/${jobId}.csv`);
    expect(capturedBody).not.toBeNull();
    const content = capturedBody!.toString("utf8").replace(/^\ufeff/, ""); // strip BOM
    const lines = content.split(/\r?\n/).filter(Boolean);
    expect(lines[0]).toBe("email,name,meta");
    expect(lines[1]).toContain("a@a.com");
    expect(lines[1]).toContain("birthday");
    expect(lines[2]).toContain("b@b.com");
  });

  it("includes meta field as last even when not in original fieldSpec", async () => {
    const jobId = "unit-test-spec-" + Date.now();
    const writer = new CsvOutputWriter(jobId, ["email", "phone"]);
    writer.addRow({ email: "c@c.com", phone: "+1", meta: JSON.stringify({ extra: "x" }) }, 1);
    await writer.flush();
    const content = capturedBody!.toString("utf8").replace(/^\ufeff/, "");
    const header = content.split(/\r?\n/)[0];
    expect(header).toBe("email,phone,meta");
  });
});
