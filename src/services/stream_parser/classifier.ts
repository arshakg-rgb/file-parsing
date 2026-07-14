import { settings } from "../../shared/config.js";
import { FailureClass } from "../../shared/models/job.js";
import { templateRegistry, RecordTemplate, RubbishTemplate } from "../../shared/templateRegistry.js";
import { safeRegex, safeRegexTest } from "../../shared/safeRegex.js";

interface ClassifyRequest {
  unknown_line: string;
  field_spec: string[];
  context_lines?: string[];
  job_id?: string;
}

interface ClassifyResponse {
  kind: "record-template" | "rubbish-signature" | "uncertain";
  template?: RecordTemplate | RubbishTemplate;
}

enum AIVerdict {
  RECORD_TEMPLATE = "record-template",
  RUBBISH_SIGNATURE = "rubbish-signature",
  UNCERTAIN = "uncertain"
}

export interface ClassifyResult {
  verdict: "parsed" | "rubbish" | "uncertain";
  row?: Record<string, any>;
  template_id?: string;
  template_version?: number;
  failure_class?: FailureClass;
}

export class LineClassifier {
  private jobId: string;
  private fieldSpec: string[];
  private recordTemplates: RecordTemplate[];
  private rubbishTemplates: RubbishTemplate[];
  private aiCache: Map<string, RecordTemplate | RubbishTemplate>;

  constructor(
    jobId: string,
    fieldSpec: string[],
    recordTemplates: RecordTemplate[],
    rubbishTemplates: RubbishTemplate[]
  ) {
    this.jobId = jobId;
    this.fieldSpec = fieldSpec;
    this.recordTemplates = recordTemplates;
    this.rubbishTemplates = rubbishTemplates;
    this.aiCache = new Map();
  }

  classify(line: string, _byteOffset: number, _byteLength: number): ClassifyResult {
    // 1. Length / empty gate.
    if (line.trim() === "") {
      return { verdict: "rubbish", template_id: "length-gate" };
    }
    if (line.length > 64 * 1024) {
      return { verdict: "uncertain", failure_class: FailureClass.TRANSFORM_ERROR };
    }

    const fp = quickFingerprint(line);
    const cached = this.aiCache.get(fp);

    // 2. Known record templates (records have priority over rubbish).
    let bestRecord: { row: Record<string, any>; template: RecordTemplate; score: number } | null = null;
    for (const t of this.recordTemplates) {
      if (t.length_hint !== undefined && line.length < t.length_hint) continue;
      try {
        const row = this.extractLine(line, t);
        if (row) {
          const meaningful = Object.values(row).filter((v) => v !== undefined && v !== null && v !== "").length;
          const present = Object.values(row).filter((v) => v !== undefined).length;
          const score = meaningful + present * 0.1;
          if (bestRecord === null || score > bestRecord.score) {
            bestRecord = { row, template: t, score };
          }
        }
      } catch {
        continue;
      }
    }
    if (bestRecord) {
      return { verdict: "parsed", row: this.coerce(bestRecord.row), template_id: bestRecord.template.template_id, template_version: bestRecord.template.version };
    }

    // 3. AI-cached record (learned in this job).
    if (cached && "field_map" in cached) {
      const row = this.extractLine(line, cached);
      if (row) return { verdict: "parsed", row: this.coerce(row), template_id: cached.template_id, template_version: cached.version };
    }

    // 4. Known high-confidence rubbish templates.
    for (const t of this.rubbishTemplates) {
      if ((t.confidence || 0) >= settings.RUBBISH_CONFIDENCE_MIN && safeRegexTest(t.signature, line)) {
        return { verdict: "rubbish", template_id: t.template_id };
      }
    }

    // 5. AI-cached rubbish (learned in this job).
    if (cached && "signature" in cached && (cached.confidence || 0) >= settings.RUBBISH_CONFIDENCE_MIN && safeRegexTest(cached.signature, line)) {
      return { verdict: "rubbish", template_id: cached.template_id };
    }

    // 6. Deterministic CSV/delimited fallback.
    const csvRow = this.parseCsvFallback(line);
    if (csvRow) return { verdict: "parsed", row: this.coerce(csvRow), template_id: "csv-auto" };

    return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
  }

  async classifyWithAI(line: string, contextLines: string[]): Promise<ClassifyResult> {
    const fp = quickFingerprint(line);
    const cached = this.aiCache.get(fp);
    if (cached) return this.toResult(line, cached);

    const req: ClassifyRequest = {
      unknown_line: line,
      field_spec: this.fieldSpec,
      context_lines: contextLines,
      job_id: this.jobId,
    };

    const { classifyAi } = await import("../ai_classifier/handler.js");
    const resp = await classifyAi(req);
    if (resp.kind === AIVerdict.UNCERTAIN || !resp.template) {
      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
    }
    this.aiCache.set(fp, resp.template);
    return this.toResult(line, resp.template);
  }

  /** Run classifier with a safety timeout to avoid hanging on pathological lines. */
  async classifyWithTimeout(line: string, contextLines: string[], timeoutMs: number): Promise<ClassifyResult> {
    return Promise.race([
      this.classifyWithAI(line, contextLines),
      new Promise<ClassifyResult>((resolve) =>
        setTimeout(() => resolve({ verdict: "uncertain", failure_class: FailureClass.UNCERTAIN }), timeoutMs)
      ),
    ]);
  }

  private toResult(line: string, tmpl: RecordTemplate | RubbishTemplate): ClassifyResult {
    if ("signature" in tmpl) {
      if ((tmpl.confidence || 0) >= settings.RUBBISH_CONFIDENCE_MIN && safeRegexTest(tmpl.signature, line)) {
        return { verdict: "rubbish", template_id: tmpl.template_id };
      }
      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
    }
    if ("field_map" in tmpl) {
      const row = this.extractLine(line, tmpl);
      if (row) return { verdict: "parsed", row: this.coerce(row), template_id: tmpl.template_id, template_version: tmpl.version };
    }
    return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
  }

  private extractLine(line: string, rec: RecordTemplate): Record<string, any> | null {
    const parsed = this.parseStructure(line, rec);
    if (!parsed) return null;

    const row: Record<string, any> = {};
    let presentCount = 0;
    for (const field of this.fieldSpec) {
      const loc = rec.field_map[field];
      if (!loc) {
        row[field] = undefined;
        continue;
      }
      const value = this.applyLocator(line, parsed, loc.locator);
      if (value !== undefined) presentCount++;
      row[field] = value;
    }
    return presentCount > 0 ? row : null;
  }

  private parseStructure(line: string, rec: RecordTemplate): string | any[] | Record<string, any> | null {
    if (rec.structure === "json") {
      if (line[0] !== "{" && line[0] !== "[") return null;
      try {
        const obj = JSON.parse(line);
        if (obj && typeof obj === "object" && !Array.isArray(obj)) return obj;
      } catch {
        return null;
      }
    }
    if (rec.structure === "kv") {
      const obj: Record<string, string> = {};
      for (const part of line.split(/[;\s]/)) {
        const [k, v] = part.split("=", 2);
        if (k && v !== undefined) obj[k.trim()] = v.trim();
      }
      return Object.keys(obj).length > 0 ? obj : null;
    }
    if (rec.structure === "csv") {
      const delim = rec.field_map && Object.values(rec.field_map)[0]?.locator?.startsWith("index:") 
        ? Object.values(rec.field_map)[0].locator.replace("index:", "") 
        : ",";
      const quote = '"';
      return parseCsvLine(line, delim, quote);
    }
    if (rec.structure === "regex" || rec.structure === "fixed") {
      return line;
    }
    return null;
  }

  private parseCsvFallback(line: string): Record<string, any> | null {
    if (this.fieldSpec.length === 0) return null;
    let best: { parts: string[]; delim: string } | null = null;
    for (const delim of [",", ";", "\t", "|"]) {
      const parts = parseCsvLine(line, delim, '"');
      if (parts.length < 2) continue;
      if (!best || parts.length > best.parts.length) best = { parts, delim };
    }
    if (!best || best.parts.length < Math.max(2, this.fieldSpec.length)) return null;

    const row: Record<string, any> = {};
    for (let i = 0; i < this.fieldSpec.length; i++) {
      const value = best.parts[i] !== undefined ? best.parts[i] : null;
      row[this.fieldSpec[i]] = value === "" ? null : value;
    }
    return row;
  }

  private applyLocator(line: string, parsed: string | any[] | Record<string, any>, loc: string): any {
    if (loc.startsWith("index:")) {
      const index = parseInt(loc.replace("index:", ""));
      if (Array.isArray(parsed) && index < parsed.length) return parsed[index];
      return undefined;
    }
    if (loc.startsWith("key:")) {
      const key = loc.replace("key:", "");
      if (parsed && !Array.isArray(parsed) && typeof parsed === "object") return (parsed as any)[key];
      return undefined;
    }
    if (loc.startsWith("regex:")) {
      const regexStr = loc.replace("regex:", "");
      const re = safeRegex(regexStr);
      if (!re) return undefined;
      const target = typeof parsed === "string" ? parsed : line;
      const match = re.exec(target);
      if (match) return match[1] ?? match[0];
      return undefined;
    }
    return undefined;
  }

  private coerce(row: Record<string, any>): Record<string, any> {
    const out: Record<string, any> = {};
    for (const [k, v] of Object.entries(row)) {
      if (v === null || v === undefined || v === "") {
        out[k] = null;
      } else if (typeof v === "boolean" || typeof v === "number") {
        out[k] = v;
      } else {
        const s = String(v).trim();
        out[k] = s;
      }
    }
    return out;
  }
}

function parseCsvLine(line: string, delim: string, quoteChar: string = '"'): string[] {
  const quote = quoteChar || null;
  const parts: string[] = [];
  let current = "";
  let inQuote = false;

  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    const next = line[i + 1];
    if (quote && c === quote) {
      if (inQuote && next === quote) {
        current += quote;
        i++; // skip escaped quote
      } else {
        inQuote = !inQuote;
      }
    } else if (c === delim && !inQuote) {
      parts.push(current.trim());
      current = "";
    } else {
      current += c;
    }
  }
  parts.push(current.trim());
  return parts;
}

function quickFingerprint(line: string): string {
  const trimmed = line.trim();
  if (trimmed.length === 0) return "empty";
  if (trimmed[0] === "{" || trimmed[0] === "[") {
    try {
      const parsed = JSON.parse(line);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return `json|${Object.keys(parsed).sort().join(",")}`;
      }
    } catch { /* ignore */ }
  }
  for (const delim of [",", ";", "\t", "|"]) {
    const parts = parseCsvLine(line, delim, '"');
    if (parts.length >= 3) return `csv|${delim}|${parts.length}`;
  }
  return `text|${trimmed.length}`;
}
