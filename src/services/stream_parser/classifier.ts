import { settings } from "../../shared/config.js";
import { FailureClass, ColumnMap } from "../../shared/models/job.js";
import { templateRegistry, RecordTemplate, RubbishTemplate } from "../../shared/templateRegistry.js";
import { safeRegex, safeRegexTest } from "../../utils/validator/safeRegex.js";

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
  private headerMap: Record<string, number> | null = null;
  private columnMap: ColumnMap | null = null;
  private firstLine = true;

  // Common column/key synonyms so field_spec names match real-world headers and JSON keys.
  private static readonly ALIASES: Record<string, string[]> = {
    email: ["email", "mail", "emailaddress", "e_mail"],
    name: ["name", "fullname", "full_name"],
    phone: ["phone", "mobile", "telephone", "phonenumber", "msisdn"],
    address: ["address", "addr", "streetaddress"],
  };

  constructor(
    jobId: string,
    fieldSpec: string[],
    recordTemplates: RecordTemplate[],
    rubbishTemplates: RubbishTemplate[],
    columnMap?: ColumnMap | null
  ) {
    this.jobId = jobId;
    this.fieldSpec = fieldSpec;
    this.recordTemplates = recordTemplates;
    this.rubbishTemplates = rubbishTemplates;
    this.columnMap = columnMap && Object.keys(columnMap).length > 0 ? columnMap : null;
    this.aiCache = new Map();
  }

  classify(line: string, _byteOffset: number, _byteLength: number): ClassifyResult {
    // 1. Length / empty / binary gate — cheapest first. Declined locally, never AI.
    const trimmed = line.trim();
    if (trimmed === "") {
      return { verdict: "rubbish", template_id: "length-gate" };
    }
    if (line.length > 64 * 1024) {
      return { verdict: "uncertain", failure_class: FailureClass.TRANSFORM_ERROR };
    }
    const nonPrintable = (trimmed.match(/[^\x09\x0A\x0D\x20-\x7E]/g) || []).length;
    if (nonPrintable / trimmed.length > 0.3) {
      return { verdict: "rubbish", template_id: "binary-gate" };
    }

    // 1b. First data line only: if it is a header row, capture a name->column map and
    // decline the header itself (dropped as rubbish, never emitted as a data row).
    if (this.firstLine) {
      this.firstLine = false;
      const hdr = this.detectHeader(line);
      if (hdr) {
        this.headerMap = hdr;
        return { verdict: "rubbish", template_id: "header" };
      }
    }

    // 1c. Client-supplied explicit column map (headerless fixed-column files). Authoritative
    // for delimited rows: it wins over learned templates. It only accepts a line whose mapped
    // email/phone column actually validates, so kv/JSON/binary lines decline here and fall
    // through to the structural/content recognizers below.
    if (this.columnMap) {
      const mapped = this.applyColumnMap(line);
      if (mapped) return { verdict: "parsed", row: this.coerce(mapped), template_id: "csv-column-map" };
    }

    const fp = quickFingerprint(line);
    const cached = this.aiCache.get(fp);

    // 2. Known learned record templates (records have priority over rubbish).
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

    // 3. AI-cached record (learned earlier in this job).
    if (cached && "field_map" in cached) {
      const row = this.extractLine(line, cached);
      if (row) return { verdict: "parsed", row: this.coerce(row), template_id: cached.template_id, template_version: cached.version };
    }

    // 4. Deterministic structural record recognizers (JSON object, or "Label: value"/
    // "k=v" key-value). These extract ONLY the client's field_spec fields, mapping by
    // key name — the design's "extract only what the client wants".
    const structural = this.parseJsonRecord(line) || this.parseKvRecord(line);
    if (structural) {
      return { verdict: "parsed", row: this.coerce(structural.row), template_id: structural.template_id };
    }

    // 5. Known high-confidence rubbish templates.
    for (const t of this.rubbishTemplates) {
      if ((t.confidence || 0) >= settings.RUBBISH_CONFIDENCE_MIN && safeRegexTest(t.signature, line)) {
        return { verdict: "rubbish", template_id: t.template_id };
      }
    }

    // 6. AI-cached rubbish (learned earlier in this job).
    if (cached && "signature" in cached && (cached.confidence || 0) >= settings.RUBBISH_CONFIDENCE_MIN && safeRegexTest(cached.signature, line)) {
      return { verdict: "rubbish", template_id: cached.template_id };
    }

    // 7. Validated delimited/CSV extraction: header-mapped when a header was seen, else
    // identify columns by CONTENT (email/phone). Returns null for junk/unmappable rows so
    // they fall through to AI/human review instead of being force-parsed into garbage.
    const delimited = this.parseDelimitedRecord(line);
    if (delimited) {
      return { verdict: "parsed", row: this.coerce(delimited), template_id: this.headerMap ? "csv-mapped" : "csv-auto" };
    }

    // 8. Nothing matched — keep-and-check. Caller escalates to AI, then human review.
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
    // Learn the template into the local stores so the NEXT matching line is recognized with no
    // AI call (design: "cached as a template and reused" — good OR junk each cost one AI call
    // in their lifetime). aiCache handles identical lines; the template lists generalize across
    // differently-fingerprinted lines of the same pattern.
    const t: any = resp.template;
    if ("field_map" in t && !this.recordTemplates.some((r) => r.template_id === t.template_id)) {
      this.recordTemplates.push(t);
    } else if ("signature" in t && !this.rubbishTemplates.some((r) => r.template_id === t.template_id)) {
      this.rubbishTemplates.push(t);
    }
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

  /**
   * Write-time guard: false if any strongly-typed field (email/phone) is populated but does not
   * validate — the fingerprint of a junk row produced by a mismapped learned/AI template. Called
   * at the emit point so no path (local template, AI, column map) can leak junk into email/phone.
   */
  rowStrongFieldsOk(row: Record<string, any>): boolean {
    for (const field of this.fieldSpec) {
      const nf = this.normalizeKey(field);
      if (nf !== "email" && nf !== "phone") continue;
      const v = row[field];
      if (v !== undefined && v !== null && String(v).trim() !== "" && !this.validateField(field, v)) return false;
    }
    return true;
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
    let strongPresent = 0; // strongly-typed fields (email/phone) that got a value
    let strongValid = 0;   // ...of those, how many actually validate
    for (const field of this.fieldSpec) {
      const loc = rec.field_map[field];
      if (!loc) {
        row[field] = undefined;
        continue;
      }
      const value = this.applyLocator(line, parsed, loc.locator);
      if (value !== undefined) presentCount++;
      const nf = this.normalizeKey(field);
      if ((nf === "email" || nf === "phone") && value !== undefined && value !== null && String(value).trim() !== "") {
        strongPresent++;
        if (this.validateField(field, value)) strongValid++;
      }
      row[field] = value;
    }
    if (presentCount === 0) return null;
    // Reject a template whose strongly-typed field(s) were populated but none validate — the
    // signature of a positional/mismapped template applied to the wrong line (e.g. a CSV
    // template reused across files that puts a bare id or a whole "Label: value" line into
    // email). Decline so the line falls through to the structural/content recognizers instead
    // of being force-parsed into garbage.
    if (strongPresent > 0 && strongValid === 0) return null;
    return row;
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
      // The delimiter is a property of the template, not something to reverse-engineer from a
      // field locator. The old code did `"index:0".replace("index:","")` -> "0", which split
      // every line on the digit 0. Use the template's stored delimiter, defaulting to comma.
      const delim = (rec as any).delimiter || ",";
      const quote = '"';
      return parseCsvLine(line, delim, quote);
    }
    if (rec.structure === "regex" || rec.structure === "fixed") {
      return line;
    }
    return null;
  }

  /** Normalize a field/column/key label for tolerant matching. */
  private normalizeKey(s: string): string {
    return s.toLowerCase().replace(/[^a-z0-9]/g, "");
  }

  /** Does a source key/column label correspond to a requested field (exact or alias)? */
  private keyMatchesField(key: string, field: string): boolean {
    const nk = this.normalizeKey(key);
    const nf = this.normalizeKey(field);
    if (!nk) return false;
    if (nk === nf) return true;
    const aliases = LineClassifier.ALIASES[nf] || [nf];
    return aliases.some((a) => this.normalizeKey(a) === nk);
  }

  /** Content validation, used to identify columns in a headerless CSV and to reject junk. */
  private validateField(field: string, value: any): boolean {
    if (value === null || value === undefined) return false;
    const v = String(value).trim();
    if (v === "") return false;
    const nf = this.normalizeKey(field);
    // Sane email chars only. The old `[^@\s]+@[^@\s]+\.[^@\s]+` accepted control/binary bytes,
    // so a garbage line containing an '@' and a '.' validated as an email. The local part allows
    // the common RFC punctuation (incl. '=', e.g. nabilah==6172@…) but no control/space/high bytes.
    if (nf === "email") return /^[A-Za-z0-9._%+=\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$/.test(v);
    if (nf === "phone") {
      if (v.includes("@")) return false;
      const digits = v.replace(/\D/g, "");
      // 10-15 digits reads as a phone. Shorter numbers (ZIP+4, year ranges, short IDs) are
      // too ambiguous to claim by content alone — those need a header or AI to map.
      // NOTE: a 10-15 digit pure-numeric ID can still false-positive here; the header-mapped
      // path (csv-mapped) is the reliable one, this content path is best-effort.
      return digits.length >= 10 && digits.length <= 15;
    }
    return true; // name/address/other: any non-empty value
  }

  /**
   * Extract only the field_spec fields from an object, matching by key name/alias.
   * requireStrong=true (fragile "Label: value" lines): accept only when a strongly-typed
   * field (email/phone) actually validates, or when ≥2 requested fields are present — so a
   * one-off "name: foo" log fragment is declined, not force-parsed. requireStrong=false
   * (a genuine JSON object, which is inherently structured): accept any single match.
   */
  private extractFromObject(
    obj: Record<string, any>,
    templateId: string,
    requireStrong: boolean
  ): { row: Record<string, any>; template_id: string } | null {
    const row: Record<string, any> = {};
    let matched = 0;
    let strong = 0;
    for (const field of this.fieldSpec) {
      let value: any = undefined;
      for (const [k, val] of Object.entries(obj)) {
        if (this.keyMatchesField(k, field)) { value = val; break; }
      }
      if (value !== undefined && value !== null && String(value).trim() !== "") {
        row[field] = value;
        matched++;
        const nf = this.normalizeKey(field);
        if ((nf === "email" || nf === "phone") && this.validateField(field, value)) strong++;
      } else {
        row[field] = null;
      }
    }
    const accept = requireStrong
      ? strong >= 1 || matched >= Math.min(2, this.fieldSpec.length)
      : matched >= 1;
    return accept ? { row, template_id: templateId } : null;
  }

  private parseJsonRecord(line: string): { row: Record<string, any>; template_id: string } | null {
    const t = line.trim();
    if (t[0] !== "{") return null;
    let obj: any;
    try { obj = JSON.parse(t); } catch { return null; }
    if (!obj || typeof obj !== "object" || Array.isArray(obj)) return null;
    return this.extractFromObject(obj, "json", false);
  }

  private parseKvRecord(line: string): { row: Record<string, any>; template_id: string } | null {
    // Only the "Label: value - Label: value" shape. The old "k=v" whitespace fallback split
    // values on spaces (truncating multi-word values), so it was removed.
    if (!line.includes(":")) return null;
    const obj: Record<string, string> = {};
    for (const seg of line.split(/\s+-\s+/)) {
      const m = seg.match(/^\s*([A-Za-z][A-Za-z0-9 _]*?)\s*:\s*(.*)$/);
      if (m) obj[m[1].trim()] = m[2].trim();
    }
    if (Object.keys(obj).length === 0) return null;
    return this.extractFromObject(obj, "kv", true);
  }

  private splitBestDelimited(line: string): string[] | null {
    let best: string[] | null = null;
    for (const delim of [",", ";", "\t", "|"]) {
      const parts = parseCsvLine(line, delim, '"');
      if (parts.length < 2) continue;
      if (!best || parts.length > best.length) best = parts;
    }
    return best;
  }

  /**
   * Treat the first line as a header only when it is unmistakably one: ≥2 cells, every cell
   * a bare label with NO data content (no '@', no ≥7-digit run), AND it locates a MAJORITY
   * (≥ half, and ≥2) of the requested fields. This prevents a plain words-only first DATA
   * row (e.g. "Cell,Berlin") from being misread as a header — which would both drop that
   * record and install a wrong column map that corrupts every following row.
   */
  private detectHeader(line: string): Record<string, number> | null {
    const parts = this.splitBestDelimited(line);
    if (!parts || parts.length < 2) return null;
    for (const c of parts) {
      const v = c.trim();
      if (v === "" || v.includes("@") || v.replace(/\D/g, "").length >= 7) return null; // data content, not a header
      if (!/^[A-Za-z][A-Za-z0-9 _.\-]*$/.test(v)) return null;
    }
    const map: Record<string, number> = {};
    let matched = 0;
    for (const field of this.fieldSpec) {
      for (let i = 0; i < parts.length; i++) {
        if (this.keyMatchesField(parts[i].trim(), field)) { map[field] = i; matched++; break; }
      }
    }
    const need = Math.max(2, Math.ceil(this.fieldSpec.length / 2));
    return matched >= need ? map : null;
  }

  /**
   * Extract fields from a delimited line using the client's explicit column map. A field maps
   * to a single 0-based column, or an array of columns whose non-empty cells are joined (e.g. a
   * multi-column address). Accepts only when a mapped strongly-typed field (email/phone) is
   * present AND validates — so this authoritative path fires on the intended fixed-column rows
   * and declines everything else (kv / JSON / binary), which then falls through to the normal flow.
   */
  private applyColumnMap(line: string): Record<string, any> | null {
    const map = this.columnMap!;
    const parts = this.splitBestDelimited(line);
    if (!parts) return null;

    const row: Record<string, any> = {};
    let present = 0;
    let strongPresent = 0;
    let strongValid = 0;
    for (const field of this.fieldSpec) {
      const spec = map[field];
      let value: any = null;
      if (typeof spec === "number") {
        value = spec < parts.length ? parts[spec] : null;
      } else if (Array.isArray(spec)) {
        const cells = spec.map((i) => (i < parts.length ? String(parts[i] ?? "").trim() : "")).filter((c) => c !== "");
        value = cells.length ? cells.join(", ") : null;
      }
      if (value !== null && String(value).trim() !== "") {
        row[field] = value;
        present++;
        const nf = this.normalizeKey(field);
        if (nf === "email" || nf === "phone") {
          strongPresent++;
          if (this.validateField(field, value)) strongValid++;
        }
      } else {
        row[field] = null;
      }
    }
    // Require a mapped email/phone column to be present and valid — the signal that this line
    // really is one of the fixed-column rows the map was written for.
    if (present === 0 || strongPresent === 0 || strongValid === 0) return null;
    return row;
  }

  private parseDelimitedRecord(line: string): Record<string, any> | null {
    if (this.fieldSpec.length === 0) return null;
    const parts = this.splitBestDelimited(line);
    if (!parts) return null;

    const row: Record<string, any> = {};
    let matched = 0;

    if (this.headerMap) {
      // Trust the header's column assignment.
      for (const field of this.fieldSpec) {
        const idx = this.headerMap[field];
        const value = idx !== undefined && idx < parts.length ? parts[idx] : "";
        row[field] = value === "" || value === undefined ? null : value;
        if (row[field] !== null) matched++;
      }
      return matched > 0 ? row : null;
    }

    // No header: identify columns by CONTENT for strongly-validatable fields (email/phone).
    // Weak fields (name/address) can't be reliably located without a header, so leave null.
    const claimed = new Set<number>();
    for (const field of this.fieldSpec) {
      const nf = this.normalizeKey(field);
      let value: any = null;
      if (nf === "email" || nf === "phone") {
        for (let i = 0; i < parts.length; i++) {
          if (claimed.has(i)) continue;
          if (this.validateField(field, parts[i])) { value = parts[i]; claimed.add(i); break; }
        }
      }
      row[field] = value;
      if (value !== null) matched++;
    }
    // Decline junk / headerless-unidentifiable rows: require at least one confident field.
    return matched > 0 ? row : null;
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
