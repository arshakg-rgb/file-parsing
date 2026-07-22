import { settings } from "@shared/Settings.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
import { FailureClass, ColumnMap } from "@shared/models/job.js";
import { templateRegistry, RecordTemplate, RubbishTemplate } from "@shared/TemplateRegistryService.js";
import { safeRegex, safeRegexTest } from "@utils/validator/safeRegex.js";
import { AIVerdict, ClassifyRequest, ClassifyResponse } from "@service/ai_classifier/io/IAiClassifier.js";
import { ClassifyResult, IClassifier } from "@service/stream_parser/io/IClassifier.js";

export type { ClassifyResult } from "@service/stream_parser/io/IClassifier.js";

/**
 * LineClassifier is responsible for line classifier operations.
 */
export class LineClassifier implements IClassifier {
    /**
   * Job Id
   * @private
   */
  private jobId: string;
    /**
   * Field Spec
   * @private
   */
  private fieldSpec: string[];
    /**
   * Record Templates
   * @private
   */
  private recordTemplates: RecordTemplate[];
    /**
   * Rubbish Templates
   * @private
   */
  private rubbishTemplates: RubbishTemplate[];
    /**
   * Ai Cache
   * @private
   */
  private aiCache: Map<string, RecordTemplate | RubbishTemplate>;
    /**
   * Header Map
   * @private
   */
  private headerMap: Record<string, number> | null = null;
  private headerParts: string[] | null = null;
    /**
   * Column Map
   * @private
   */
  private columnMap: ColumnMap | null = null;
    /**
   * First Line
   * @private
   */
  private firstLine = true;
  private logger: Logger;
  private normalizedFieldSpec: string[];
  private aliasMap: Map<string, Set<string>>;

  // Common column/key synonyms so field_spec names match real-world headers and JSON keys.
  private static readonly ALIASES: Record<string, string[]> = {
    email: ["email", "mail", "emailaddress", "e_mail", "emails"],
    name: ["name", "fullname", "full_name"],
    phone: ["phone", "mobile", "telephone", "phonenumber", "msisdn", "phones"],
    address: ["address", "addr", "streetaddress", "addresses"],
  };

  // Static reusable regexes (compiled once).
  private static readonly EMAIL_RE = /^[A-Za-z0-9._%+=\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$/;
  private static readonly HEADER_LABEL_RE = /^[A-Za-z][A-Za-z0-9 _.\-]*$/;
  private static readonly KV_SEG_RE = /^\s*([A-Za-z][A-Za-z0-9 _]*?)\s*:\s*(.*)$/;

    /**
   * Constructs a new LineClassifier instance.
   * @param jobId - The job identifier
   * @param fieldSpec - The field spec
   * @param recordTemplates - The record templates
   * @param rubbishTemplates - The rubbish templates
   * @param columnMap - The column map
   */
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
    this.logger = createLogger(`LineClassifier:${this.jobId}`);
    this.normalizedFieldSpec = fieldSpec.map((f) => this.normalizeKey(f));
    this.aliasMap = new Map<string, Set<string>>();
    for (const [base, aliases] of Object.entries(LineClassifier.ALIASES)) {
      const set = new Set<string>();
      for (const a of aliases) set.add(this.normalizeKey(a));
      this.aliasMap.set(this.normalizeKey(base), set);
    }
  }

    /**
   * Classifies the operation
   * @param line - The line to process
   * @param _byteOffset - The _byte offset
   * @param _byteLength - The _byte length
   * @returns The classify result result
   */
  classify(line: string, _byteOffset: number, _byteLength: number): ClassifyResult {
    // 1. Length / empty / binary gate — cheapest first. Declined locally, never AI.
    const trimmed = line.trim();
    if (trimmed === "") {
      return { verdict: "rubbish", template_id: "length-gate" };
    }
    if (line.length > 64 * 1024) {
      return { verdict: "uncertain", failure_class: FailureClass.TRANSFORM_ERROR };
    }
    // Count only true control/ non-printable characters (C0 + C1 blocks). Cyrillic and other
    // Unicode letters/digits/punctuation are printable text, not binary.
    let nonPrintable = 0;
    for (let i = 0; i < trimmed.length; i++) {
      const c = trimmed.charCodeAt(i);
      if ((c <= 0x08) || (c >= 0x0B && c <= 0x0C) || (c >= 0x0E && c <= 0x1F) || (c >= 0x7F && c <= 0x9F)) {
        nonPrintable++;
      }
    }
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

    // 1d. Header-detected fast path: if a header row was seen, we already know the column
    // layout — skip template/AI matching entirely and go straight to delimited extraction.
    if (this.headerMap) {
      const delimited = this.parseDelimitedRecord(line);
      if (delimited) return { verdict: "parsed", row: this.coerce(delimited), template_id: "csv-mapped" };
    }

    // 2. Known learned record templates (records have priority over rubbish).
    // AI cache is only consumed in steps 3 and 6, so compute the fingerprint lazily.
    let computedCache = false;
    let cached: RecordTemplate | RubbishTemplate | undefined;
    const getCached = () => {
      if (!computedCache) {
        computedCache = true;
        const fp = quickFingerprint(line);
        cached = this.aiCache.get(fp);
      }
      return cached;
    };

    let bestRecord: { row: Record<string, unknown>; template: RecordTemplate; score: number } | null = null;
    for (const t of this.recordTemplates) {
      if (t.length_hint !== undefined && line.length < t.length_hint) continue;
      try {
        const row = this.extractLine(line, t);
        if (row) {
          let meaningful = 0;
          let present = 0;
          for (const v of Object.values(row)) {
            if (v !== undefined) {
              present++;
              if (v !== null && v !== "") meaningful++;
            }
          }
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
    const c3 = getCached();
    if (c3 && "field_map" in c3) {
      const row = this.extractLine(line, c3);
      if (row) return { verdict: "parsed", row: this.coerce(row), template_id: c3.template_id, template_version: c3.version };
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
    const c6 = getCached();
    if (c6 && "signature" in c6 && (c6.confidence || 0) >= settings.RUBBISH_CONFIDENCE_MIN && safeRegexTest(c6.signature, line)) {
      return { verdict: "rubbish", template_id: c6.template_id };
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

    /**
   * Classifies with a i
   * @param line - The line to process
   * @param contextLines - The context lines
   * @returns A promise that resolves to the result
   */
  async classifyWithAI(line: string, contextLines: string[]): Promise<ClassifyResult> {
    const fp = quickFingerprint(line);
    const cached = this.aiCache.get(fp);
    if (cached) {
      this.logger.info("ai_cache_hit", { fingerprint: fp, template_id: cached.template_id });
      return this.toResult(line, cached);
    }
    this.logger.info("ai_cache_miss", { fingerprint: fp, line_length: line.length, context_lines: contextLines.length });

    const req: ClassifyRequest = {
      unknown_line: line,
      field_spec: this.fieldSpec,
      context_lines: contextLines,
      job_id: this.jobId,
    };

    const { classifyAi } = await import("@service/ai_classifier/AiClassifierServiceHandler.js");
    const resp = await classifyAi(req);
    if (resp.kind === AIVerdict.UNCERTAIN || !resp.template) {
      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
    }

    this.aiCache.set(fp, resp.template);
    this.logger.info("ai_cache_saved", { fingerprint: fp, template_id: resp.template.template_id });
    // Learn the template into the local stores so the NEXT matching line is recognized with no
    // AI call (design: "cached as a template and reused" — good OR junk each cost one AI call
    // in their lifetime). aiCache handles identical lines; the template lists generalize across
    // differently-fingerprinted lines of the same pattern.
    const t: RecordTemplate | RubbishTemplate = resp.template;
    if ("field_map" in t && !this.recordTemplates.some((r) => r.template_id === t.template_id)) {
      this.recordTemplates.push(t as RecordTemplate);
      this.logger.info("ai_template_learned", { template_id: t.template_id, kind: "record", source: "ai_call" });
    } else if ("signature" in t && !this.rubbishTemplates.some((r) => r.template_id === t.template_id)) {
      this.rubbishTemplates.push(t as RubbishTemplate);
      this.logger.info("ai_template_learned", { template_id: t.template_id, kind: "rubbish", source: "ai_call" });
    }
    return this.toResult(line, t);
  }

  /** Run classifier with a safety timeout to avoid hanging on pathological lines. */
  async classifyWithTimeout(line: string, contextLines: string[], timeoutMs: number): Promise<ClassifyResult> {
    this.logger.info("ai_call_timeout_scheduled", { line_length: line.length, timeout_ms: timeoutMs });
    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    const timeoutPromise = new Promise<ClassifyResult>((resolve) => {
      timeoutHandle = setTimeout(() => {
        this.logger.warn("ai_call_timeout_reached", { line_length: line.length, timeout_ms: timeoutMs });
        resolve({ verdict: "uncertain", failure_class: FailureClass.UNCERTAIN });
      }, timeoutMs);
    });
    try {
      return await Promise.race([this.classifyWithAI(line, contextLines), timeoutPromise]);
    } finally {
      clearTimeout(timeoutHandle);
    }
  }

  /**
   * Write-time guard: false if a strongly-typed field (email/phone) is populated but does not
   * validate — the fingerprint of a junk row produced by a mismapped learned/AI template. Called
   * at the emit point so no path (local template, AI, column map) can leak junk into email/phone.
   */
  rowStrongFieldsOk(row: Record<string, unknown>): boolean {
    for (let i = 0; i < this.fieldSpec.length; i++) {
      const field = this.fieldSpec[i];
      const nf = this.normalizedFieldSpec[i];
      if (nf !== "email" && nf !== "phone") continue;
      const v = row[field];
      if (v !== undefined && v !== null && String(v).trim() !== "" && !this.validateField(field, v)) return false;
    }
    return true;
  }

    /**
   * Performs the to result operation.
   * @param line - The line to process
   * @param tmpl - The tmpl
   * @returns The classify result result
   */
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

    /**
   * Extracts line
   * @param line - The line to process
   * @param rec - The rec
   * @returns The record<string, unknown> | null result
   */
  private extractLine(line: string, rec: RecordTemplate): Record<string, unknown> | null {
    const parsed = this.parseStructure(line, rec);
    if (!parsed) return null;

    const row: Record<string, unknown> = {};
    let presentCount = 0;
    let strongPresent = 0; // strongly-typed fields (email/phone) that got a value
    let strongValid = 0;   // ...of those, how many actually validate
    for (let i = 0; i < this.fieldSpec.length; i++) {
      const field = this.fieldSpec[i];
      const loc = rec.field_map[field];
      if (!loc) {
        row[field] = undefined;
        continue;
      }
      // Defensive: accept current { locator: string } shape, legacy raw locator string,
      // or older { index | key | regex } objects that may still exist in cache/DB.
      let locator: string | undefined;
      const rawLoc = loc as unknown as Record<string, unknown>;
      if (typeof rawLoc === "string") {
        locator = rawLoc;
      } else if (typeof rawLoc.locator === "string") {
        locator = rawLoc.locator;
      } else if (typeof rawLoc.index === "number") {
        locator = `index:${rawLoc.index}`;
      } else if (typeof rawLoc.key === "string") {
        locator = `key:${rawLoc.key}`;
      } else if (typeof rawLoc.regex === "string") {
        locator = `regex:${rawLoc.regex}`;
      }
      const value = locator ? this.applyLocator(line, parsed, locator) : undefined;
      if (value !== undefined) presentCount++;
      const nf = this.normalizedFieldSpec[i];
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

    /**
   * Parses structure
   * @param line - The line to process
   * @param rec - The rec
   * @returns The string | unknown[] |  record<string, unknown> | null result
   */
  private parseStructure(line: string, rec: RecordTemplate): string | unknown[] | Record<string, unknown> | null {
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
      const delim = rec.delimiter ?? ",";
      const quote = "\"";
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
  private validateField(field: string, value: unknown): boolean {
    if (value === null || value === undefined) return false;
    const v = String(value).trim();
    if (v === "") return false;
    const nf = this.normalizeKey(field);
    // Sane email chars only. The old `[^@\s]+@[^@\s]+\.[^@\s]+` accepted control/binary bytes,
    // so a garbage line containing an '@' and a '.' validated as an email. The local part allows
    // the common RFC punctuation (incl. '=', e.g. nabilah==6172@…) but no control/space/high bytes.
    if (nf === "email") return LineClassifier.EMAIL_RE.test(v);
    if (nf === "phone") {
      if (v.includes("@")) return false;
      const digits = v.replace(/\D/g, "");
      // 10-15 digits reads as a phone. Shorter numbers (ZIP+4, year ranges, short IDs) are
      // too ambiguous to claim by content alone — those need a header or AI to map.
      // NOTE: a 10-15 digit pure-numeric ID can still false-positive here; the header-mapped
      // path (csv-mapped) is the reliable one, this content path is best-effort.
      return digits.length >= 10 && digits.length <= 15;
    }
    return true; // name/address/other: every non-empty value
  }

  /**
   * Extract only the field_spec fields from an object, matching by key name/alias.
   * requireStrong=true (fragile "Label: value" lines): accept only when a strongly-typed
   * field (email/phone) actually validates, or when ≥2 requested fields are present — so a
   * one-off "name: foo" log fragment is declined, not force-parsed. requireStrong=false
   * (a genuine JSON object, which is inherently structured): accept a single match.
   */
  private extractFromObject(
    obj: Record<string, unknown>,
    templateId: string,
    requireStrong: boolean
  ): { row: Record<string, unknown>; template_id: string } | null {
    const row: Record<string, unknown> = {};
    let matched = 0;
    let strong = 0;
    const normalizedObjKeys = new Map<string, unknown>();
    for (const [k, val] of Object.entries(obj)) {
      const nk = this.normalizeKey(k);
      if (!normalizedObjKeys.has(nk)) normalizedObjKeys.set(nk, val); // first wins
    }
    for (let i = 0; i < this.fieldSpec.length; i++) {
      const field = this.fieldSpec[i];
      const nf = this.normalizedFieldSpec[i];
      let value = normalizedObjKeys.get(nf);
      if (value === undefined) {
        const aliases = this.aliasMap.get(nf);
        if (aliases) {
          for (const a of aliases) {
            value = normalizedObjKeys.get(a);
            if (value !== undefined) break;
          }
        }
      }
      if (value !== undefined && value !== null && String(value).trim() !== "") {
        row[field] = value;
        matched++;
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

    /**
   * Parses json record
   * @param line - The line to process
   * @returns The { row:  record<string, unknown>; template_id: string } | null result
   */
  private parseJsonRecord(line: string): { row: Record<string, unknown>; template_id: string } | null {
    const t = line.trim();
    if (t[0] !== "{") return null;
    let obj: unknown;
    try { obj = JSON.parse(t); } catch { return null; }
    if (!obj || typeof obj !== "object" || Array.isArray(obj)) return null;
    return this.extractFromObject(obj as Record<string, unknown>, "json", false);
  }

    /**
   * Parses kv record
   * @param line - The line to process
   * @returns The { row:  record<string, unknown>; template_id: string } | null result
   */
  private parseKvRecord(line: string): { row: Record<string, unknown>; template_id: string } | null {
    // Only the "Label: value - Label: value" shape. The old "k=v" whitespace fallback split
    // values on spaces (truncating multi-word values), so it was removed.
    if (!line.includes(":")) return null;
    const obj: Record<string, string> = {};
    for (const seg of line.split(/\s+-\s+/)) {
      const m = LineClassifier.KV_SEG_RE.exec(seg);
      if (m) obj[m[1].trim()] = m[2].trim();
    }
    if (Object.keys(obj).length === 0) return null;
    return this.extractFromObject(obj, "kv", true);
  }

    /**
   * Splits best delimited
   * @param line - The line to process
   * @returns The string[] | null result
   */
  private splitBestDelimited(line: string): string[] | null {
    let best: string[] | null = null;
    for (const delim of [",", ";", "\t", "|"]) {
      const parts = parseCsvLine(line, delim, "\"");
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
      if (!LineClassifier.HEADER_LABEL_RE.test(v)) return null;
    }
    const map: Record<string, number> = {};
    let matched = 0;
    for (const field of this.fieldSpec) {
      if (field === "meta") continue;
      for (let i = 0; i < parts.length; i++) {
        if (this.keyMatchesField(parts[i].trim(), field)) { map[field] = i; matched++; break; }
      }
    }
    const nonMetaFields = this.fieldSpec.filter((f) => f !== "meta");
    // When the source header has MORE columns than fieldSpec (extra columns go to meta),
    // require only 1 match — the label-only check is already strict enough to avoid false positives.
    // When fieldSpec is the same size as (or larger than) the source, keep the stricter majority rule.
    const need = parts.length > nonMetaFields.length
      ? Math.max(1, Math.ceil(nonMetaFields.length / 4))
      : Math.max(2, Math.ceil(nonMetaFields.length / 2));
    if (matched < need) return null;
    this.headerParts = parts.map((p) => p.trim());
    return map;
  }

  /**
   * Extract fields from a delimited line using the client's explicit column map. A field maps
   * to a single 0-based column, or an array of columns whose non-empty cells are joined (e.g. a
   * multi-column address). Accepts only when a mapped strongly-typed field (email/phone) is
   * present AND validates — so this authoritative path fires on the intended fixed-column rows
   * and declines everything else (kv / JSON / binary), which then falls through to the normal flow.
   */
  private applyColumnMap(line: string): Record<string, unknown> | null {
    const map = this.columnMap!;
    const parts = this.splitBestDelimited(line);
    if (!parts) return null;

    const row: Record<string, unknown> = {};
    let present = 0;
    let strongPresent = 0;
    let strongValid = 0;
    for (let i = 0; i < this.fieldSpec.length; i++) {
      const field = this.fieldSpec[i];
      const nf = this.normalizedFieldSpec[i];
      const spec = map[field];
      let value: unknown = null;
      if (typeof spec === "number") {
        value = spec < parts.length ? parts[spec] : null;
      } else if (Array.isArray(spec)) {
        const cells = spec.map((i) => (i < parts.length ? String(parts[i] ?? "").trim() : "")).filter((c) => c !== "");
        value = cells.length ? cells.join(", ") : null;
      }
      if (value !== null && String(value).trim() !== "") {
        row[field] = value;
        present++;
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

    /**
   * Parses delimited record
   * @param line - The line to process
   * @returns The record<string, unknown> | null result
   */
  private parseDelimitedRecord(line: string): Record<string, unknown> | null {
    if (this.fieldSpec.length === 0) return null;
    const parts = this.splitBestDelimited(line);
    if (!parts) return null;

    const row: Record<string, unknown> = {};
    let matched = 0;

    if (this.headerMap) {
      // Trust the header's column assignment.
      const mappedIndices = new Set<number>(Object.values(this.headerMap));
      for (let i = 0; i < this.fieldSpec.length; i++) {
        const field = this.fieldSpec[i];
        if (field === "meta") continue; // handled unconditionally below
        const idx = this.headerMap[field];
        const value = idx !== undefined && idx < parts.length ? parts[idx] : "";
        row[field] = value === "" || value === undefined ? null : value;
        if (row[field] !== null) matched++;
      }
      // Always collect ALL unmapped source columns into meta when the header is known,
      // regardless of whether "meta" is present in fieldSpec. This surfaces extra
      // columns (birthday, snils, passport_numbers, etc.) without requiring the caller
      // to enumerate every source column in field_spec.
      if (this.headerParts) {
        const metaObj: Record<string, string> = {};
        for (let j = 0; j < this.headerParts.length; j++) {
          if (!mappedIndices.has(j)) {
            const v = j < parts.length ? String(parts[j] ?? "").trim() : "";
            if (v !== "") metaObj[this.headerParts[j]] = v;
          }
        }
        row["meta"] = Object.keys(metaObj).length ? JSON.stringify(metaObj) : null;
        if (row["meta"] !== null) matched++;
      }
      return matched > 0 ? row : null;
    }

    // No header: identify columns by CONTENT for strongly-validatable fields (email/phone).
    // Weak fields (name/address) can't be reliably located without a header, so leave null.
    const claimed = new Set<number>();
    for (let i = 0; i < this.fieldSpec.length; i++) {
      const field = this.fieldSpec[i];
      const nf = this.normalizedFieldSpec[i];
      let value: unknown = null;
      if (nf === "email" || nf === "phone") {
        for (let j = 0; j < parts.length; j++) {
          if (claimed.has(j)) continue;
          if (this.validateField(field, parts[j])) { value = parts[j]; claimed.add(j); break; }
        }
      }
      row[field] = value;
      if (value !== null) matched++;
    }
    // Decline junk / headerless-unidentifiable rows: require at least one confident field.
    return matched > 0 ? row : null;
  }

    /**
   * Performs the apply locator operation.
   * @param line - The line to process
   * @param parsed - The parsed
   * @param loc - The loc
   * @returns The unknown result
   */
  private applyLocator(line: string, parsed: string | unknown[] | Record<string, unknown>, loc: string): unknown {
    if (typeof loc !== "string" || !loc) return undefined;
    if (loc.startsWith("index:")) {
      const index = parseInt(loc.replace("index:", ""));
      if (Array.isArray(parsed) && index < parsed.length) return parsed[index];
      return undefined;
    }
    if (loc.startsWith("key:")) {
      const key = loc.replace("key:", "");
      if (parsed && !Array.isArray(parsed) && typeof parsed === "object") return (parsed as Record<string, unknown>)[key];
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

    /**
   * Performs the coerce operation.
   * @param row - The row
   * @returns The record<string, unknown> result
   */
  private coerce(row: Record<string, unknown>): Record<string, unknown> {
    const out: Record<string, unknown> = {};
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

/**
 * Parses csv line
 * @param line - The line to process
 * @param delim - The delim
 * @param quoteChar - The quote char
 * @returns The list of results
 */
function parseCsvLine(line: string, delim: string, quoteChar: string = "\""): string[] {
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

/**
 * Performs the quick fingerprint operation.
 * @param line - The line to process
 * @returns The string result
 */
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
    const parts = parseCsvLine(line, delim, "\"");
    if (parts.length >= 3) return `csv|${delim}|${parts.length}`;
  }
  return `text|${trimmed.length}`;
}
