import { settings } from "@shared/Settings.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
import { FailureClass } from "@shared/models/job.js";
import { templateRegistry, RecordTemplate, RubbishTemplate } from "@shared/TemplateRegistryService.js";
import { safeRegex, safeRegexTest } from "@utils/validator/safeRegex.js";
import ServiceManager from "@config/ServiceManager.js";
import { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { ClassifierService } from "@service/stream_parser/ClassifierService.js";
import { IClassifier, ClassifyRequest, ClassifyResponse, ClassifyResult } from "@service/stream_parser/io/IClassifier.js";
import { FIELD_ALIASES, DELIMITERS, MAX_LINE_LENGTH, BINARY_THRESHOLD, MIN_HEADER_FIELDS, HEADER_MATCH_RATIO, PHONE_MIN_DIGITS, PHONE_MAX_DIGITS, TEMPLATE_IDS } from "@service/stream_parser/io/ClassifierConstants.js";

enum AIVerdict {
  RECORD_TEMPLATE = "record-template",
  RUBBISH_SIGNATURE = "rubbish-signature",
  UNCERTAIN = "uncertain"
}

/**
 * ClassifierServiceImpl is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class ClassifierServiceImpl extends ServiceManager implements ClassifierService {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: ClassifierServiceImpl;
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
   * First Line
   * @private
   */
  private firstLine = true;
  private logger: Logger;

    /**
   * Constructs a new ClassifierServiceImpl instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate ClassifierServiceImpl directly. Use getInstance()");
    }
    super(enforce);
    this.logger = createLogger("ClassifierServiceImpl");
    this.jobId = "";
    this.fieldSpec = [];
    this.recordTemplates = [];
    this.rubbishTemplates = [];
    this.aiCache = new Map();
    this.headerMap = null;
    this.firstLine = true;
  }

    /**
   * Gets the single instance of the ClassifierServiceImpl class.
   * @returns The single instance of the class
   */
  public static getInstance(): ClassifierServiceImpl {
    if (!ClassifierServiceImpl.instance) {
      ClassifierServiceImpl.instance = new ClassifierServiceImpl(Enforce);
    }
    return ClassifierServiceImpl.instance;
  }

    /**
   * Resets the operation
   * @param jobId - The job identifier
   * @param fieldSpec - The field spec
   * @param recordTemplates - The record templates
   * @param rubbishTemplates - The rubbish templates
   */
  public reset(jobId: string, fieldSpec: string[], recordTemplates: RecordTemplate[], rubbishTemplates: RubbishTemplate[]): void {
    this.jobId = jobId;
    this.fieldSpec = fieldSpec;
    this.recordTemplates = recordTemplates;
    this.rubbishTemplates = rubbishTemplates;
    this.aiCache = new Map();
    this.headerMap = null;
    this.headerParts = null;
    this.firstLine = true;
    this.logger = createLogger(`ClassifierServiceImpl:${jobId}`);
  }

    /**
   * Gets header map
   * @returns The record<string, number> | null result
   */
  public getHeaderMap(): Record<string, number> | null {
    return this.headerMap;
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
      return { verdict: "rubbish", template_id: TEMPLATE_IDS.LENGTH_GATE };
    }
    if (line.length > MAX_LINE_LENGTH) {
      return { verdict: "uncertain", failure_class: FailureClass.TRANSFORM_ERROR };
    }
    const nonPrintable = (trimmed.match(/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/g) || []).length;
    // Lower threshold for CSV-like lines (lines with delimiters) to catch binary data
    // that happens to have commas/quotes making it look like CSV
    const hasDelimiters = /[,\t;|]/.test(trimmed);
    const threshold = hasDelimiters ? 0.15 : BINARY_THRESHOLD;
    if (nonPrintable / trimmed.length > threshold) {
      return { verdict: "rubbish", template_id: TEMPLATE_IDS.BINARY_GATE };
    }

    // 1b. First data line only: if it is a header row, capture a name->column map and
    // decline the header itself (dropped as rubbish, never emitted as a data row).
    if (this.firstLine) {
      this.firstLine = false;
      const hdr = this.detectHeader(line);
      if (hdr) {
        this.headerMap = hdr;
        return { verdict: "rubbish", template_id: TEMPLATE_IDS.HEADER };
      }
    }

    const fp = quickFingerprint(line);
    const cached = this.aiCache.get(fp);

    // 2. Known learned record templates (records have priority over rubbish).
    let bestRecord: { row: Record<string, unknown>; template: RecordTemplate; score: number } | null = null;
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
    const structural = this.parseJsonRecord(line) || this.parseKvRecord(line) || this.parseXmlRecord(line) || this.parseYamlRecord(line);
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
      return { verdict: "parsed", row: this.coerce(delimited), template_id: this.headerMap ? TEMPLATE_IDS.CSV_MAPPED : TEMPLATE_IDS.CSV_AUTO };
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

    this.logger.info("ai_call_initiated", { fingerprint: fp, line_length: line.length, context_lines: contextLines.length });
    const handler = await import("@service/ai_classifier/AiClassifierServiceHandler.js");
    const resp = await handler.classifyAi(req);
    if (resp.kind === AIVerdict.UNCERTAIN || !resp.template) {
      this.logger.info("ai_call_uncertain", { fingerprint: fp, kind: resp.kind });
      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
    }
    this.aiCache.set(fp, resp.template);
    this.logger.info("ai_cache_saved", { fingerprint: fp, template_id: resp.template.template_id });
    this.logger.info("ai_call_completed", { fingerprint: fp, template_id: resp.template.template_id, verdict: "field_map" in resp.template ? "parsed" : "rubbish" });
    return this.toResult(line, resp.template);
  }

    /**
   * Classifies with timeout
   * @param line - The line to process
   * @param contextLines - The context lines
   * @param timeoutMs - The timeout in milliseconds
   * @returns A promise that resolves to the result
   */
  async classifyWithTimeout(line: string, contextLines: string[], timeoutMs: number): Promise<ClassifyResult> {
    this.logger.info("ai_call_timeout_scheduled", { line_length: line.length, timeout_ms: timeoutMs });
    return Promise.race([
      this.classifyWithAI(line, contextLines),
      new Promise<ClassifyResult>((resolve) =>
        setTimeout(() => {
          this.logger.warn("ai_call_timeout_reached", { line_length: line.length, timeout_ms: timeoutMs });
          resolve({ verdict: "uncertain", failure_class: FailureClass.UNCERTAIN });
        }, timeoutMs)
      ),
    ]);
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
      // Try different key-value separators: =, :, or -
      // Try different pair separators: ;, or " - "
      let parts: string[] = [];
      
      // First try " - " as pair separator (common in key-value logs)
      if (line.includes(" - ")) {
        parts = line.split(" - ");
      } else if (line.includes(";")) {
        parts = line.split(";");
      } else {
        // Fallback to whitespace
        parts = line.split(/\s+/);
      }
      
      for (const part of parts) {
        // Try different key-value separators
        let k: string | undefined, v: string | undefined;
        
        if (part.includes("=")) {
          [k, v] = part.split("=", 2);
        } else if (part.includes(":")) {
          [k, v] = part.split(":", 2);
        } else if (part.includes("-")) {
          [k, v] = part.split("-", 2);
        }
        
        if (k && v !== undefined) {
          obj[k.trim()] = v.trim();
        }
      }
      return Object.keys(obj).length > 0 ? obj : null;
    }
    if (rec.structure === "csv") {
      const delim = rec.field_map && Object.values(rec.field_map)[0]?.locator?.startsWith("index:")
        ? Object.values(rec.field_map)[0].locator.replace("index:", "")
        : ",";
      const quote = "\"";
      return parseCsvLine(line, delim, quote);
    }
    if (rec.structure === "xml") {
      return this.parseXml(line);
    }
    if (rec.structure === "yaml") {
      return this.parseYaml(line);
    }
    if (rec.structure === "regex" || rec.structure === "fixed") {
      return line;
    }
    return null;
  }

    /**
   * Parses xml
   * @param line - The line to process
   * @returns The record<string, unknown> | null result
   */
  private parseXml(line: string): Record<string, unknown> | null {
    // Simple XML parsing for basic structures
    // For production, use a proper XML parser like xml2js
    if (!line.includes("<") || !line.includes(">")) return null;
    
    const obj: Record<string, unknown> = {};
    const tagRegex = /<(\w+)>([^<]*)<\/\1>/g;
    let match;
    while ((match = tagRegex.exec(line)) !== null) {
      const [, tag, value] = match;
      obj[tag] = value.trim();
    }
    
    return Object.keys(obj).length > 0 ? obj : null;
  }

    /**
   * Parses yaml
   * @param line - The line to process
   * @returns The record<string, unknown> | null result
   */
  private parseYaml(line: string): Record<string, unknown> | null {
    // Simple YAML parsing for key-value pairs
    // For production, use a proper YAML parser like js-yaml
    if (!line.includes(":")) return null;
    
    const obj: Record<string, string> = {};
    for (const part of line.split("\n")) {
      const [k, v] = part.split(":", 2);
      if (k && v !== undefined) {
        obj[k.trim()] = v.trim();
      }
    }
    
    return Object.keys(obj).length > 0 ? obj : null;
  }

    /**
   * Parses xml record
   * @param line - The line to process
   * @returns The { row:  record<string, unknown>; template_id: string } | null result
   */
  private parseXmlRecord(line: string): { row: Record<string, unknown>; template_id: string } | null {
    const obj = this.parseXml(line);
    if (!obj) return null;
    return this.extractFromObject(obj, "xml", false);
  }

    /**
   * Parses yaml record
   * @param line - The line to process
   * @returns The { row:  record<string, unknown>; template_id: string } | null result
   */
  private parseYamlRecord(line: string): { row: Record<string, unknown>; template_id: string } | null {
    const obj = this.parseYaml(line);
    if (!obj) return null;
    return this.extractFromObject(obj, "yaml", false);
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
    const aliases = FIELD_ALIASES[nf] || [nf];
    return aliases.some((a) => this.normalizeKey(a) === nk);
  }

  /** Content validation, used to identify columns in a headerless CSV and to reject junk. */
  private validateField(field: string, value: unknown): boolean {
    if (value === null || value === undefined) return false;
    const v = String(value).trim();
    if (v === "") return false;
    const nf = this.normalizeKey(field);
    
    if (nf === "email") return /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(v);
    if (nf === "phone") {
      if (v.includes("@")) return false;
      const digits = v.replace(/\D/g, "");
      return digits.length >= PHONE_MIN_DIGITS && digits.length <= PHONE_MAX_DIGITS;
    }
    if (nf === "zip" || nf === "zipcode" || nf === "postalcode") {
      // US ZIP: 5 digits or 5+4, or international formats
      return /^\d{5}(-\d{4})?$/.test(v) || /^[A-Za-z]\d[A-Za-z] \d[A-Za-z]\d$/.test(v);
    }
    if (nf === "date" || nf === "datetime" || nf === "timestamp") {
      // ISO 8601 date format
      return /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?)?$/.test(v);
    }
    if (nf === "url" || nf === "website" || nf === "link") {
      return /^https?:\/\/.+\..+/.test(v);
    }
    return true; // name/address/other: every non-empty value
  }

  /**
   * Extract only the field_spec fields from an object, matching by key name/alias.
   * requireStrong=true (fragile "Label: value" lines): accept only when a strongly-typed
   * field (email/phone/date/zip/url) actually validates, or when ≥2 requested fields are present.
   * requireStrong=false (a genuine JSON/XML/YAML object, which is inherently structured): accept a single match.
   */
  private extractFromObject(
    obj: Record<string, unknown>,
    templateId: string,
    requireStrong: boolean
  ): { row: Record<string, unknown>; template_id: string } | null {
    const row: Record<string, unknown> = {};
    let matched = 0;
    let strong = 0;
    for (const field of this.fieldSpec) {
      let value: unknown = undefined;
      for (const [k, val] of Object.entries(obj)) {
        if (this.keyMatchesField(k, field)) { value = val; break; }
      }
      if (value !== undefined && value !== null && String(value).trim() !== "") {
        row[field] = value;
        matched++;
        const nf = this.normalizeKey(field);
        if ((nf === "email" || nf === "phone" || nf === "zip" || nf === "date" || nf === "url") && this.validateField(field, value)) {
          strong++;
        }
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
    return this.extractFromObject(obj as Record<string, unknown>, TEMPLATE_IDS.JSON, false);
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
      const m = seg.match(/^\s*([A-Za-z][A-Za-z0-9 _]*?)\s*:\s*(.*)$/);
      if (m) obj[m[1].trim()] = m[2].trim();
    }
    if (Object.keys(obj).length === 0) return null;
    return this.extractFromObject(obj, TEMPLATE_IDS.KV, true);
  }

    /**
   * Splits best delimited
   * @param line - The line to process
   * @returns The string[] | null result
   */
  private splitBestDelimited(line: string): string[] | null {
    let best: string[] | null = null;
    for (const delim of DELIMITERS) {
      const parts = parseCsvLine(line, delim, "\"");
      if (parts.length < 2) continue;
      if (!best || parts.length > best.length) best = parts;
    }
    return best;
  }

  /**
   * Treat the first line as a header only when it is unmistakably one: ≥2 cells, every cell
   * a bare label with NO data content (no '@', no ≥7-digit run), AND it locates a MAJORITY
   * (≥ half, and ≥2) of the requested fields.
   */
  private detectHeader(line: string): Record<string, number> | null {
    const parts = this.splitBestDelimited(line);
    if (!parts || parts.length < MIN_HEADER_FIELDS) return null;
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
    // When source has MORE columns than fieldSpec (extra go to meta), require only 1 match.
    const nonMetaFields = this.fieldSpec.filter((f) => f !== "meta");
    const need = parts.length > nonMetaFields.length
      ? Math.max(1, Math.ceil(nonMetaFields.length / 4))
      : Math.max(MIN_HEADER_FIELDS, Math.ceil(nonMetaFields.length * HEADER_MATCH_RATIO));
    if (matched < need) return null;
    this.headerParts = parts.map((p) => p.trim());
    return map;
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
      for (const field of this.fieldSpec) {
        if (field === "meta") continue; // handled unconditionally below
        const idx = this.headerMap[field];
        const value = idx !== undefined && idx < parts.length ? parts[idx] : "";
        row[field] = value === "" || value === undefined ? null : value;
        if (row[field] !== null) matched++;
      }
      // Always collect ALL unmapped source columns into meta when the header is known.
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

    // No header: identify columns by CONTENT for strongly-validatable fields (email/phone/date/zip/url).
    // Weak fields (name/address) can't be reliably located without a header, so leave null.
    const claimed = new Set<number>();
    for (const field of this.fieldSpec) {
      const nf = this.normalizeKey(field);
      let value: unknown = null;
      if (nf === "email" || nf === "phone" || nf === "zip" || nf === "date" || nf === "url") {
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

    /**
   * Performs the apply locator operation.
   * @param line - The line to process
   * @param parsed - The parsed
   * @param loc - The loc
   * @returns The unknown result
   */
  private applyLocator(line: string, parsed: string | unknown[] | Record<string, unknown>, loc: string): unknown {
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
        i++;
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
  for (const delim of DELIMITERS) {
    const parts = parseCsvLine(line, delim, "\"");
    if (parts.length >= 3) return `csv|${delim}|${parts.length}`;
  }
  return `text|${trimmed.length}`;
}

export default ClassifierServiceImpl;
