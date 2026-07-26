import { settings } from "@shared/Settings.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
import { FailureClass, ColumnMap } from "@shared/models/job.js";
import { templateRegistry, RecordTemplate, RubbishTemplate } from "@shared/TemplateRegistryService.js";
import { safeRegex, safeRegexTest } from "@utils/validator/safeRegex.js";
import { AIVerdict, ClassifyRequest, ClassifyResponse } from "@service/ai_classifier/io/IAiClassifier.js";
import { ClassifyResult, IClassifier } from "@service/stream_parser/io/IClassifier.js";
import {aiClassifierService} from "@service/ai_classifier/AiClassifierServiceHandler.js";

export type { ClassifyResult } from "@service/stream_parser/io/IClassifier.js";

/**
 * LineClassifier turns a single raw input line into a `ClassifyResult`.
 *
 * `classify()` runs the line through a fixed pipeline of cheap, deterministic stages
 * (length/binary gate → header capture → client column map → structural JSON/KV →
 * delimited/CSV → unmapped-JSON flatten → learned templates → AI-cached template →
 * rubbish templates), stopping at the first stage that produces a verdict. Anything
 * that survives every stage is "uncertain" and left for the caller to escalate to
 * `classifyWithAI`.
 *
 * Stage order matters and is deliberate: structural/delimited parsing runs before
 * learned/AI-cached templates so a stale template (learned from a different file
 * shape) can never hijack a line that parses cleanly on its own.
 */
export class LineClassifier implements IClassifier {
  private jobId: string;
  private fieldSpec: string[];
  private recordTemplates: RecordTemplate[];
  private rubbishTemplates: RubbishTemplate[];
  private aiCache: Map<string, RecordTemplate | RubbishTemplate>;
  private headerMap: Record<string, number> | null = null;
  private headerParts: string[] | null = null;
  private columnMap: ColumnMap | null = null;
  private firstLine = true;
  private logger: Logger;
  private normalizedFieldSpec: string[];
  private aliasMap: Map<string, Set<string>>;
  private aiRateLimiter?: { acquire(): Promise<void> };

  // Common column/key synonyms so field_spec names match real-world headers and JSON keys.
  private static readonly ALIASES: Record<string, string[]> = {
    email: ["email", "mail", "emailaddress", "e_mail", "emails"],
    name: ["name", "fullname", "full_name"],
    phone: ["phone", "mobile", "telephone", "phonenumber", "msisdn", "phones"],
    address: ["address", "addr", "streetaddress", "addresses", "city", "country", "street"],
  };

  // --- Compiled-once regexes and thresholds. Grouped here (rather than declared inline
  // inside the methods that use them) so every magic number/pattern in the classifier is
  // discoverable and documented in one place. ---
  private static readonly EMAIL_RE = /^[A-Za-z0-9._%+=\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$/;
  private static readonly HEADER_LABEL_RE = /^[A-Za-z][A-Za-z0-9 _.\-]*$/;
  private static readonly KV_SEG_RE = /^\s*([A-Za-z][A-Za-z0-9 _]*?)\s*:\s*(.*)$/;
  /** Matches control/private-use/unassigned/surrogate code points — true binary corruption.
   *  Deliberately excludes emoji/symbol categories (So/Sm/Sk), which are normal in real
   *  names/usernames and must not reject an otherwise-valid line. Reused by both the
   *  line-level binary gate and the field-level check in `coerce()`. */
  private static readonly BINARY_RE = /[\p{Cc}\p{Co}\p{Cn}\p{Cs}]/gu;
  private static readonly MAX_LINE_LENGTH = 64 * 1024;
  private static readonly NON_PRINTABLE_RATIO_MAX = 0.15;
  private static readonly BINARY_RATIO_MAX = 0.05;
  /** Columns that look like a synthetic/opaque ID rather than free text, so they're
   *  excluded from best-effort weak-field (name/address) grouping in headerless CSVs. */
  private static readonly ID_LIKE_RE = /^\d{7,}$|^OD\d+$/i;
  private static readonly SALUTATION_RE = /^(Mr|Mrs|Ms|Master|Miss)\.?$/i;
  private static readonly DELIMITER_ONLY_RE = /^[,;]+$/;
  private static readonly ZIP_RE = /^\d{4,6}$/;
  /** Delimiters tried, in order, when guessing a line's column separator. Shared by the
   *  column splitter and the fingerprinting helper. */
  private static readonly DELIMITER_CANDIDATES = [",", ";", "\t", "|"] as const;

  constructor(
      jobId: string,
      fieldSpec: string[],
      recordTemplates: RecordTemplate[],
      rubbishTemplates: RubbishTemplate[],
      columnMap?: ColumnMap | null,
      aiRateLimiter?: { acquire(): Promise<void> } | null
  ) {
    this.jobId = jobId;
    this.fieldSpec = fieldSpec;
    this.recordTemplates = recordTemplates;
    this.rubbishTemplates = rubbishTemplates;
    this.columnMap = columnMap && Object.keys(columnMap).length > 0 ? columnMap : null;
    this.aiRateLimiter = aiRateLimiter ?? undefined;
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

  // ===========================================================================================
  // Pipeline orchestration
  // ===========================================================================================

  /**
   * Classifies one line. Stages run in order; the first stage to return a non-null
   * result wins. See the class-level doc comment for the rationale behind the order.
   */
  classify(line: string, _byteOffset: number, _byteLength: number): ClassifyResult {
    const trimmed = line.trim();

    const gated = this.applyLengthAndBinaryGate(line, trimmed);
    if (gated) return gated;

    // First data line only: capture a header→column map, if this line looks like one,
    // and drop it (never emitted as a data row).
    if (this.firstLine) {
      this.firstLine = false;
      const header = this.detectHeader(line);
      if (header) {
        this.headerMap = header;
        return { verdict: "rubbish", template_id: "header" };
      }
    }

    const columnMapped = this.classifyViaColumnMap(line);
    if (columnMapped) return columnMapped;

    // Deterministic structural recognizers (JSON object, "Label: value"/"k=v" KV) win over
    // learned/AI templates: stale templates can otherwise force junk values onto lines that
    // actually parse exactly.
    const structural = this.parseJsonRecord(line) || this.parseKvRecord(line);
    if (structural) return this.finalizeParsedOrReject(structural.row, structural.template_id);

    // Validated delimited/CSV extraction, header-mapped when a header was seen, else by
    // content (email/phone). Also runs before learned templates for the same reason.
    const delimited = this.parseDelimitedRecord(line);
    if (delimited) {
      return this.finalizeParsedOrReject(delimited.row, delimited.usedHeader ? "csv-mapped" : "csv-auto");
    }

    // Unmapped JSON: still valid JSON, so every key must be preserved (in field_spec
    // columns or folded into meta) rather than becoming uncertain/DLQ.
    const looksLikeJson = trimmed[0] === "{" || trimmed[0] === "[";
    if (looksLikeJson) return this.classifyUnmappedJson(trimmed);

    // From here on, template matching may need the line's fingerprint for AI-cache lookups.
    // Computed lazily since most lines resolve via the structural/delimited stages above.
    let cacheComputed = false;
    let cached: RecordTemplate | RubbishTemplate | undefined;
    const getCached = (): RecordTemplate | RubbishTemplate | undefined => {
      if (!cacheComputed) {
        cacheComputed = true;
        cached = this.aiCache.get(LineClassifier.quickFingerprint(line));
      }
      return cached;
    };

    const learned = this.classifyViaLearnedRecordTemplates(line);
    if (learned) return learned;

    const aiCachedRecord = this.classifyViaCachedRecordTemplate(line, getCached());
    if (aiCachedRecord) return aiCachedRecord;

    const rubbish = this.classifyViaRubbishTemplates(line, getCached());
    if (rubbish) return rubbish;

    // Nothing matched — keep-and-check. Caller escalates to AI, then human review.
    return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
  }

  /** Stage: length/empty/binary gate. Cheapest checks first; declined locally, never AI. */
  private applyLengthAndBinaryGate(line: string, trimmed: string): ClassifyResult | null {
    if (trimmed === "") return { verdict: "rubbish", template_id: "length-gate" };
    if (line.length > LineClassifier.MAX_LINE_LENGTH) {
      return { verdict: "uncertain", failure_class: FailureClass.TRANSFORM_ERROR };
    }

    // Count only true control/non-printable characters (C0 + C1 blocks). Cyrillic and other
    // Unicode letters/digits/punctuation are printable text, not binary.
    let nonPrintable = 0;
    for (let i = 0; i < trimmed.length; i++) {
      const c = trimmed.charCodeAt(i);
      if ((c <= 0x08) || (c >= 0x0b && c <= 0x0c) || (c >= 0x0e && c <= 0x1f) || (c >= 0x7f && c <= 0x9f)) {
        nonPrintable++;
      }
    }
    if (nonPrintable / trimmed.length > LineClassifier.NON_PRINTABLE_RATIO_MAX) {
      return { verdict: "rubbish", template_id: "binary-gate" };
    }

    const binaryCount = (trimmed.match(LineClassifier.BINARY_RE) || []).length;
    if (binaryCount / trimmed.length > LineClassifier.BINARY_RATIO_MAX) {
      return { verdict: "rubbish", template_id: "binary-gate" };
    }
    return null;
  }

  /**
   * Stage: client-supplied explicit column map (headerless fixed-column files).
   * Authoritative for delimited rows — wins over learned templates — but only accepts a
   * line whose mapped email/phone column actually validates, so kv/JSON/binary lines
   * decline here and fall through to the structural recognizers.
   */
  private classifyViaColumnMap(line: string): ClassifyResult | null {
    if (!this.columnMap) return null;
    const mapped = this.applyColumnMap(line);
    if (!mapped) return null;
    return this.finalizeParsedOrReject(mapped, "csv-column-map");
  }

  /** Stage: unmapped-but-valid JSON. Flattens and folds every key into field_spec + meta. */
  private classifyUnmappedJson(trimmed: string): ClassifyResult {
    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
    }
    if (Array.isArray(parsed)) {
      const first = parsed.find((x) => x && typeof x === "object" && !Array.isArray(x)) as
          | Record<string, unknown>
          | undefined;
      if (!first) return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
      parsed = first;
    }
    if (!parsed || typeof parsed !== "object") {
      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
    }

    const extracted = this.extractFromObject(parsed as Record<string, unknown>, "json", undefined, true);
    if (!extracted) return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
    // Note: unlike finalizeParsedOrReject, a coerce failure here yields "uncertain" (not
    // "rubbish") — this branch is a best-effort fallback for already-valid JSON, so a
    // binary-corrupted field is treated as ambiguous rather than confidently junk.
    const coerced = this.coerce(extracted.row);
    if (coerced) return { verdict: "parsed", row: coerced, template_id: "json" };
    return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
  }

  /** Stage: known learned record templates. Records have priority over rubbish; best-scoring
   *  template (most present + non-empty fields) wins when several match. */
  private classifyViaLearnedRecordTemplates(line: string): ClassifyResult | null {
    let best: { row: Record<string, unknown>; template: RecordTemplate; score: number } | null = null;
    for (const t of this.recordTemplates) {
      if (t.length_hint !== undefined && line.length < t.length_hint) continue;
      try {
        const row = this.extractLine(line, t);
        if (!row) continue;
        const score = this.scoreExtractedRow(row);
        if (!best || score > best.score) best = { row, template: t, score };
      } catch {
        continue;
      }
    }
    if (!best) return null;
    return this.finalizeParsedOrReject(best.row, best.template.template_id, best.template.version);
  }

  /** Scores an extracted row: more present fields, and especially non-empty ones, win. */
  private scoreExtractedRow(row: Record<string, unknown>): number {
    let meaningful = 0;
    let present = 0;
    for (const v of Object.values(row)) {
      if (v !== undefined) {
        present++;
        if (v !== null && v !== "") meaningful++;
      }
    }
    return meaningful + present * 0.1;
  }

  /** Stage: AI-cached record template learned earlier in this job. */
  private classifyViaCachedRecordTemplate(
      line: string,
      cached: RecordTemplate | RubbishTemplate | undefined
  ): ClassifyResult | null {
    if (!cached || !("field_map" in cached)) return null;
    const row = this.extractLine(line, cached);
    if (!row) return null;
    return this.finalizeParsedOrReject(row, cached.template_id, cached.version);
  }

  /** Stage: known high-confidence rubbish templates, then AI-cached rubbish. */
  private classifyViaRubbishTemplates(
      line: string,
      cached: RecordTemplate | RubbishTemplate | undefined
  ): ClassifyResult | null {
    for (const t of this.rubbishTemplates) {
      if ((t.confidence || 0) >= settings.RUBBISH_CONFIDENCE_MIN && safeRegexTest(t.signature, line)) {
        return { verdict: "rubbish", template_id: t.template_id };
      }
    }
    if (
        cached &&
        "signature" in cached &&
        (cached.confidence || 0) >= settings.RUBBISH_CONFIDENCE_MIN &&
        safeRegexTest(cached.signature, line)
    ) {
      return { verdict: "rubbish", template_id: cached.template_id };
    }
    return null;
  }

  /** Coerces a row and turns it into a `parsed` result, or a `rubbish`/binary-field result
   *  if coercion rejects it (field-level binary content). Centralizes the
   *  coerce-then-branch pattern repeated across every "found a match" stage above. */
  private finalizeParsedOrReject(
      row: Record<string, unknown>,
      templateId: string,
      templateVersion?: number
  ): ClassifyResult {
    const coerced = this.coerce(row);
    if (!coerced) return { verdict: "rubbish", template_id: "binary-field" };
    return templateVersion !== undefined
        ? { verdict: "parsed", row: coerced, template_id: templateId, template_version: templateVersion }
        : { verdict: "parsed", row: coerced, template_id: templateId };
  }

  // ===========================================================================================
  // AI escalation
  // ===========================================================================================

  async classifyWithAI(line: string, contextLines: string[], remainingBudget?: number): Promise<ClassifyResult> {
    const fp = LineClassifier.quickFingerprint(line);
    const cached = this.aiCache.get(fp);
    if (cached) {
      this.logger.info("ai_cache_hit", { fingerprint: fp, template_id: cached.template_id });
      return { ...this.toResult(line, cached), ai_calls_used: 0 };
    }
    this.logger.info("ai_cache_miss", { fingerprint: fp, line_length: line.length, context_lines: contextLines.length });

    if (remainingBudget !== undefined && remainingBudget <= 0) {
      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN, ai_calls_used: 0 };
    }

    const req: ClassifyRequest = {
      unknown_line: line,
      field_spec: this.fieldSpec,
      context_lines: contextLines,
      job_id: this.jobId,
    };

    const { aiClassifierService } = await import("@service/ai_classifier/AiClassifierServiceHandler.js");
    const trimmed = line.trim();
    const isJsonLine = trimmed[0] === "{" || trimmed[0] === "[";

    // JSON-shaped lines should not be parsed by generic regex/delimited record templates.
    // Ask the model to parse the JSON directly into the target field_spec + meta.
    if (isJsonLine) {
      this.logger.info("ai_call_initiated", { fingerprint: fp, line_length: line.length, context_lines: contextLines.length, reason: "json_parse" });
      const { result, ai_calls_used } = await this.tryDiscoverJsonFields(line, aiClassifierService.parseJsonLine);
      if (result) return { ...result, ai_calls_used };
      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN, ai_calls_used };
    }

    this.logger.info("ai_call_initiated", { fingerprint: fp, line_length: line.length, context_lines: contextLines.length });
    if (this.aiRateLimiter) await this.aiRateLimiter.acquire();
    const resp = await aiClassifierService.classifyAi(req);
    const ai_calls_used = 1;
    if (resp.kind === AIVerdict.UNCERTAIN || !resp.template) {
      this.logger.info("ai_call_uncertain", { fingerprint: fp, kind: resp.kind });
      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN, ai_calls_used };
    }

    this.aiCache.set(fp, resp.template);
    this.logger.info("ai_cache_saved", { fingerprint: fp, template_id: resp.template.template_id });
    // Learn the template into the local stores so the NEXT matching line is recognized with
    // no AI call. aiCache handles identical lines; these lists generalize across differently
    // fingerprinted lines of the same pattern.
    const t: RecordTemplate | RubbishTemplate = resp.template;
    if ("field_map" in t && !this.recordTemplates.some((r) => r.template_id === t.template_id)) {
      this.recordTemplates.push(t as RecordTemplate);
      this.logger.info("ai_template_learned", { template_id: t.template_id, kind: "record", source: "ai_call" });
    } else if ("signature" in t && !this.rubbishTemplates.some((r) => r.template_id === t.template_id)) {
      this.rubbishTemplates.push(t as RubbishTemplate);
      this.logger.info("ai_template_learned", { template_id: t.template_id, kind: "rubbish", source: "ai_call" });
    }
    this.logger.info("ai_call_completed", { fingerprint: fp, template_id: t.template_id, verdict: "field_map" in t ? "parsed" : "rubbish" });
    return { ...this.toResult(line, t), ai_calls_used };
  }

  /**
   * AI fallback for structurally-valid JSON that local parsing could not map.
   * Asks the model to parse the JSON record directly into target columns + meta.
   * If the model fails, falls back to a flat local extraction so no data is lost.
   */
  private async tryDiscoverJsonFields(
      line: string,
      parseJsonLine: (jsonLine: string, targets: string[]) => Promise<Record<string, unknown> | null>
  ): Promise<{ result: ClassifyResult | null; ai_calls_used: number }> {
    const t = line.trim();
    if (t[0] !== "{" && t[0] !== "[" && !(t.length >= 2 && t[0] === "\"" && t[t.length - 1] === "\"")) {
      return { result: null, ai_calls_used: 0 };
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(t);
    } catch {
      return { result: null, ai_calls_used: 0 };
    }
    if (!parsed || typeof parsed !== "object") return { result: null, ai_calls_used: 0 };
    let obj = parsed as Record<string, unknown>;
    if (Array.isArray(parsed)) {
      const first = parsed.find((x) => x && typeof x === "object" && !Array.isArray(x)) as
          | Record<string, unknown>
          | undefined;
      if (!first) return { result: null, ai_calls_used: 0 };
      obj = first;
    }
    // JSON-in-JSON string
    if (typeof obj === "string") {
      return this.tryDiscoverJsonFields(obj, parseJsonLine);
    }
    try {
      if (this.aiRateLimiter) await this.aiRateLimiter.acquire();
      const aiRow = await parseJsonLine(line, this.fieldSpec);
      if (aiRow && typeof aiRow === "object" && !Array.isArray(aiRow)) {
        const coerced = this.coerce(aiRow);
        if (coerced) {
          this.logger.info("ai_json_parse_succeeded", { fingerprint: LineClassifier.quickFingerprint(line), keys: Object.keys(coerced).length });
          return { result: { verdict: "parsed", row: coerced, template_id: "ai-json" }, ai_calls_used: 1 };
        }
      }
    } catch (err) {
      this.logger.warn("ai_json_parse_failed", { error: String(err) });
    }
    // Local flatten fallback: always preserve the full JSON in meta.
    const extracted = this.extractFromObject(obj, "json", this.fieldSpec, true);
    if (extracted) {
      const coerced = this.coerce(extracted.row);
      if (coerced) return { result: { verdict: "parsed", row: coerced, template_id: "json" }, ai_calls_used: 1 };
    }
    return { result: null, ai_calls_used: 1 };
  }

  /** Runs classification with a safety timeout so pathological lines can't hang the pipeline. */
  async classifyWithTimeout(
      line: string,
      contextLines: string[],
      timeoutMs: number,
      remainingBudget?: number
  ): Promise<ClassifyResult> {
    this.logger.info("ai_call_timeout_scheduled", { line_length: line.length, timeout_ms: timeoutMs });
    return Promise.race([
      this.classifyWithAI(line, contextLines, remainingBudget),
      new Promise<ClassifyResult>((resolve) =>
          setTimeout(() => {
            this.logger.warn("ai_call_timeout_reached", { line_length: line.length, timeout_ms: timeoutMs });
            resolve({ verdict: "uncertain", failure_class: FailureClass.UNCERTAIN });
          }, timeoutMs)
      ),
    ]);
  }

  /**
   * Write-time guard: false if a strongly-typed field (email/phone) is populated but does not
   * validate — the fingerprint of a junk row produced by a mismapped learned/AI template.
   * Called at the emit point so no path (local template, AI, column map) can leak junk into
   * email/phone.
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

  /** Resolves a learned/AI template (record or rubbish) against a line, used by the AI-cache
   *  hit path in `classifyWithAI`. */
  private toResult(line: string, tmpl: RecordTemplate | RubbishTemplate): ClassifyResult {
    if ("signature" in tmpl) {
      if ((tmpl.confidence || 0) >= settings.RUBBISH_CONFIDENCE_MIN && safeRegexTest(tmpl.signature, line)) {
        return { verdict: "rubbish", template_id: tmpl.template_id };
      }
      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
    }
    const row = this.extractLine(line, tmpl);
    if (!row) return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
    return this.finalizeParsedOrReject(row, tmpl.template_id, tmpl.version);
  }

  // ===========================================================================================
  // Record-template extraction
  // ===========================================================================================

  private extractLine(line: string, rec: RecordTemplate): Record<string, unknown> | null {
    const parsed = this.parseStructure(line, rec);
    if (!parsed) return null;

    const row: Record<string, unknown> = {};
    let presentCount = 0;
    let strongPresent = 0; // strongly-typed fields (email/phone) that got a value
    let strongValid = 0; // ...of those, how many actually validate
    for (let i = 0; i < this.fieldSpec.length; i++) {
      const field = this.fieldSpec[i];
      const loc = rec.field_map[field];
      if (!loc) {
        row[field] = undefined;
        continue;
      }
      const locator = this.resolveLocatorString(loc);
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

  /** Normalizes a field_map locator entry to its canonical `"kind:value"` string form.
   *  Accepts the current `{ locator: string }` shape, a legacy raw locator string, or
   *  older `{ index | key | regex }` objects that may still exist in cache/DB. */
  private resolveLocatorString(loc: unknown): string | undefined {
    if (typeof loc === "string") return loc;
    const rawLoc = loc as Record<string, unknown>;
    if (typeof rawLoc.locator === "string") return rawLoc.locator;
    if (typeof rawLoc.index === "number") return `index:${rawLoc.index}`;
    if (typeof rawLoc.key === "string") return `key:${rawLoc.key}`;
    if (typeof rawLoc.regex === "string") return `regex:${rawLoc.regex}`;
    return undefined;
  }

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
      // Try different key-value separators: =, :, or -. Try different pair separators:
      // ";" or " - ".
      let parts: string[] = [];
      if (line.includes(" - ")) {
        parts = line.split(" - ");
      } else if (line.includes(";")) {
        parts = line.split(";");
      } else {
        parts = line.split(/\s+/);
      }
      for (const part of parts) {
        let k: string | undefined, v: string | undefined;
        if (part.includes("=")) {
          [k, v] = part.split("=", 2);
        } else if (part.includes(":")) {
          [k, v] = part.split(":", 2);
        } else if (part.includes("-")) {
          [k, v] = part.split("-", 2);
        }
        if (k && v !== undefined) obj[k.trim()] = v.trim();
      }
      return Object.keys(obj).length > 0 ? obj : null;
    }
    if (rec.structure === "csv") {
      // The delimiter is a property of the template, not something to reverse-engineer from a
      // field locator (the old code did `"index:0".replace("index:","")` -> "0", which split
      // every line on the digit 0). Use the template's stored delimiter, defaulting to comma.
      const delim = rec.delimiter ?? ",";
      return LineClassifier.parseCsvLine(line, delim, "\"");
    }
    if (rec.structure === "regex" || rec.structure === "fixed") {
      return line;
    }
    return null;
  }

  // ===========================================================================================
  // Key/field matching helpers
  // ===========================================================================================

  /** Normalizes a field/column/key label for tolerant matching. */
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
    // so a garbage line containing an '@' and a '.' validated as an email. The local part
    // allows common RFC punctuation (incl. '=', e.g. nabilah==6172@…) but no control/space/high
    // bytes.
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

  // ===========================================================================================
  // Object flattening / extraction (JSON, KV)
  // ===========================================================================================

  /**
   * Recursively flattens a nested object into dot-notation keys. Arrays of objects are
   * flattened with numeric indexes (e.g. `messages[0].body`); scalar arrays are kept as
   * arrays so they are JSON-stringified in meta/output rather than mangled to
   * "[object Object]".
   */
  private flattenObject(obj: Record<string, unknown>, prefix = ""): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj)) {
      const key = prefix ? `${prefix}.${k}` : k;
      if (v !== null && typeof v === "object" && !Array.isArray(v)) {
        Object.assign(out, this.flattenObject(v as Record<string, unknown>, key));
        continue;
      }
      if (Array.isArray(v)) {
        const allObjects = v.length > 0 && v.every((x) => x !== null && typeof x === "object" && !Array.isArray(x));
        if (allObjects) {
          for (let i = 0; i < v.length; i++) {
            Object.assign(out, this.flattenObject(v[i] as Record<string, unknown>, `${key}[${i}]`));
          }
        } else {
          out[key] = v;
        }
        continue;
      }
      let sv: unknown = v;
      // Double-encoded JSON strings: unwrap a JSON string that contains another JSON
      // string/object/array.
      if (typeof sv === "string") {
        const t = sv.trim();
        if (t.length >= 2 && t[0] === "\"" && t[t.length - 1] === "\"") {
          try {
            const inner = JSON.parse(sv);
            if (typeof inner === "string") sv = inner;
          } catch {
            /* not a JSON string */
          }
        }
      }
      if (typeof sv === "string" && (sv.trim().startsWith("{") || sv.trim().startsWith("["))) {
        try {
          const parsed = JSON.parse(sv);
          if (parsed && typeof parsed === "object") {
            if (Array.isArray(parsed)) {
              const allObjects = parsed.length > 0 && parsed.every((x) => x !== null && typeof x === "object" && !Array.isArray(x));
              if (allObjects) {
                for (let i = 0; i < parsed.length; i++) {
                  Object.assign(out, this.flattenObject(parsed[i] as Record<string, unknown>, `${key}[${i}]`));
                }
              } else {
                out[key] = parsed;
              }
              continue;
            }
            Object.assign(out, this.flattenObject(parsed as Record<string, unknown>, key));
            continue;
          }
        } catch {
          /* fall through to keep raw string */
        }
      }
      out[key] = sv;
    }
    return out;
  }

  /**
   * Extracts only the field_spec fields from a (possibly nested) object, matching by key
   * name/alias. Nested objects are flattened first so `contact.email` maps to the `email`
   * field_spec field.
   */
  private extractFromObject(
      rawObj: Record<string, unknown>,
      templateId: string,
      fieldSpecOverride?: string[],
      loose = false
  ): { row: Record<string, unknown>; template_id: string } | null {
    const obj = this.flattenObject(rawObj);
    const spec = fieldSpecOverride ?? this.fieldSpec;
    const normalizedSpec = fieldSpecOverride ? fieldSpecOverride.map((f) => this.normalizeKey(f)) : this.normalizedFieldSpec;
    const row: Record<string, unknown> = {};
    let matched = 0;
    let strong = 0;
    const normalizedObjKeys = new Map<string, unknown>();
    const leafToFull = new Map<string, string>(); // leaf → full normalized key
    const consumedKeys = new Set<string>(); // Track which raw keys got mapped

    for (const [k, val] of Object.entries(obj)) {
      const nk = this.normalizeKey(k);
      if (!normalizedObjKeys.has(nk)) normalizedObjKeys.set(nk, val); // first wins
      // Also map the leaf key so "contact.email" can match the "email" field_spec field.
      if (k.includes(".")) {
        const leaf = k.slice(k.lastIndexOf(".") + 1);
        const nLeaf = this.normalizeKey(leaf);
        if (!normalizedObjKeys.has(nLeaf)) {
          normalizedObjKeys.set(nLeaf, val);
          leafToFull.set(nLeaf, nk);
        }
      }
    }

    for (let i = 0; i < spec.length; i++) {
      const field = spec[i];
      if (field === "meta") continue; // handled below
      const nf = normalizedSpec[i];
      let value = normalizedObjKeys.get(nf);
      let matchedKey: string | undefined = nf;
      if (value !== undefined) {
        const vFull = leafToFull.get(nf);
        if (consumedKeys.has(nf) || (vFull && consumedKeys.has(vFull))) value = undefined;
      }
      if (value === undefined) {
        const aliases = this.aliasMap.get(nf);
        if (aliases) {
          for (const a of aliases) {
            const av = normalizedObjKeys.get(a);
            if (av !== undefined) {
              const aFull = leafToFull.get(a);
              if (consumedKeys.has(a) || (aFull && consumedKeys.has(aFull))) continue;
              value = av;
              matchedKey = a;
              break;
            }
          }
        }
      }
      // Nested path fallback: a field like "phone" can live under "contact_info.phone.raw"
      // and "location" under "person.location.city". Also tolerate renamed/numbered keys
      // such as "first name", "email_address", "mobile_2" via substring matching.
      if (value === undefined) {
        const found = this.locateNestedFieldValue(obj, field, nf, consumedKeys);
        if (found) {
          value = found.value;
          matchedKey = found.key;
        }
      }

      // Semantic inference for name: if first and last exist, the full name should contain
      // both. This overrides a fallback that accidentally landed on "first".
      if (nf === "name") {
        const inferred = this.inferFullNameFromParts(obj, normalizedObjKeys, value);
        if (inferred) {
          value = inferred.value;
          matchedKey = inferred.key;
        }
      }

      if (value !== undefined && value !== null && String(value).trim() !== "") {
        row[field] = value;
        matched++;
        consumedKeys.add(matchedKey!);
        const fullKey = leafToFull.get(matchedKey!);
        if (fullKey) consumedKeys.add(fullKey); // do not duplicate into meta
        if ((nf === "email" || nf === "phone") && this.validateField(field, value)) strong++;
      } else {
        row[field] = null;
      }
    }

    // Fold every unmapped source key into meta. This runs unconditionally — even when the
    // caller's field_spec omits "meta" — because CsvOutputWriter always appends a meta column
    // to the output CSV, so any extra source fields must land there or be lost.
    const metaObj: Record<string, string> = {};
    for (const [k, v] of Object.entries(obj)) {
      if (!consumedKeys.has(this.normalizeKey(k)) && v !== undefined && v !== null) {
        const metaStr = typeof v === "object" ? JSON.stringify(v) : String(v).trim();
        if (metaStr !== "") metaObj[k] = metaStr;
      }
    }
    row["meta"] = Object.keys(metaObj).length ? JSON.stringify(metaObj) : null;

    const nonMetaSpec = spec.filter((f) => f !== "meta").length;
    const minMatches = Math.max(1, Math.ceil(nonMetaSpec * 0.75));
    const accept = strong >= 1 || matched >= minMatches;
    return accept || loose ? { row, template_id: templateId } : null;
  }

  /** Best-effort nested/renamed key match: segment match, prefix match, or (for non-name/
   *  address fields) substring match against the field's accepted alias set. */
  private locateNestedFieldValue(
      obj: Record<string, unknown>,
      field: string,
      nf: string,
      consumedKeys: Set<string>
  ): { value: unknown; key: string } | null {
    const accepted = Array.from(new Set([nf, ...(this.aliasMap.get(nf) || [])]));
    const noSubstring = nf === "name" || nf === "address";
    let bestScore = -1;
    let bestValue: unknown;
    let bestKey: string | undefined;

    for (const [k, val] of Object.entries(obj)) {
      if (val === null || val === undefined || val === "") continue;
      const nk = this.normalizeKey(k);
      if (consumedKeys.has(nk)) continue;
      const segments = k.split(/[.[\]]+/).filter(Boolean).map((s) => this.normalizeKey(s));
      const segmentMatch = segments.some((s) => accepted.includes(s));
      const prefixMatch = accepted.some((a) => nk.startsWith(a));
      // For "name" and "address" we do NOT use substring matching because "schoolname"
      // would match name and "country_code" would match address.
      const substringMatch = !noSubstring && accepted.some((a) => a.length >= 3 && nk.includes(a));
      if (!segmentMatch && !prefixMatch && !substringMatch) continue;

      const strVal = typeof val === "string" ? val : JSON.stringify(val);
      if (strVal.trim() === "") continue;
      const isString = typeof val === "string";
      const isValid = this.validateField(field, val);
      // First-match ordering wins over length so "first/current" entries are preferred.
      const score = (isString ? 100000 : 0) + (isValid ? 10000 : 0);
      if (score > bestScore) {
        bestScore = score;
        bestValue = val;
        bestKey = nk;
      }
    }
    return bestValue !== undefined ? { value: bestValue, key: bestKey! } : null;
  }

  /** If `first`/`last` keys exist and the current candidate for "name" doesn't already
   *  contain both, look for a full-name-shaped value that does. */
  private inferFullNameFromParts(
      obj: Record<string, unknown>,
      normalizedObjKeys: Map<string, unknown>,
      currentValue: unknown
  ): { value: string; key: string } | null {
    const firstVal = normalizedObjKeys.get("first");
    const lastVal = normalizedObjKeys.get("last");
    if (!(typeof firstVal === "string" && typeof lastVal === "string" && firstVal.trim() && lastVal.trim())) {
      return null;
    }
    const fn = this.normalizeKey(firstVal);
    const ln = this.normalizeKey(lastVal);
    const current = typeof currentValue === "string" ? this.normalizeKey(currentValue) : "";
    if (current && current.includes(fn) && current.includes(ln)) return null; // already correct

    let bestKey: string | undefined;
    let bestValue: string | undefined;
    for (const [k, val] of Object.entries(obj)) {
      if (typeof val !== "string") continue;
      if (this.normalizeKey(k).includes("first") || this.normalizeKey(k).includes("last")) continue;
      const nv = this.normalizeKey(val);
      if (nv.includes(fn) && nv.includes(ln) && (!bestValue || val.length > bestValue.length)) {
        bestValue = val;
        bestKey = k;
      }
    }
    return bestValue !== undefined ? { value: bestValue, key: this.normalizeKey(bestKey!) } : null;
  }

  private parseJsonRecord(line: string): { row: Record<string, unknown>; template_id: string } | null {
    const t = line.trim();
    if (t[0] !== "{" && t[0] !== "[") {
      // JSON-in-JSON: a cell that is itself a JSON-encoded string of a JSON object/array.
      if (t.length >= 2 && t[0] === "\"" && t[t.length - 1] === "\"") {
        try {
          const inner = JSON.parse(t) as string;
          if (typeof inner === "string") return this.parseJsonRecord(inner);
        } catch {
          /* fall through */
        }
      }
      return null;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(t);
    } catch {
      return null;
    }
    if (!parsed || typeof parsed !== "object") return null;
    let obj = parsed as Record<string, unknown>;
    if (Array.isArray(parsed)) {
      const first = parsed.find((x) => x && typeof x === "object" && !Array.isArray(x)) as
          | Record<string, unknown>
          | undefined;
      if (!first) return null;
      obj = first;
    }
    return this.extractFromObject(obj, "json");
  }

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
    return this.extractFromObject(obj, "kv");
  }

  // ===========================================================================================
  // Delimited/CSV parsing
  // ===========================================================================================

  private splitBestDelimited(line: string): string[] | null {
    let best: string[] | null = null;
    for (const delim of LineClassifier.DELIMITER_CANDIDATES) {
      const parts = LineClassifier.parseCsvLine(line, delim, "\"");
      if (parts.length < 2) continue;
      if (!best || parts.length > best.length) best = parts;
    }
    return best;
  }

  /**
   * Treats the first line as a header only when it is unmistakably one: ≥2 cells, every cell
   * a bare label with NO data content (no '@', no ≥7-digit run), AND it locates a MAJORITY
   * (≥ half, and ≥2) of the requested fields. This prevents a plain words-only first DATA row
   * (e.g. "Cell,Berlin") from being misread as a header — which would both drop that record
   * and install a wrong column map that corrupts every following row.
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
        if (this.keyMatchesField(parts[i].trim(), field)) {
          map[field] = i;
          matched++;
          break;
        }
      }
    }
    const nonMetaFields = this.fieldSpec.filter((f) => f !== "meta");
    // When the source header has MORE columns than fieldSpec (extra columns go to meta),
    // require only 1 match — the label-only check is already strict enough to avoid false
    // positives. When fieldSpec is the same size as (or larger than) the source, keep the
    // stricter majority rule.
    const need =
        parts.length > nonMetaFields.length
            ? Math.max(1, Math.ceil(nonMetaFields.length / 4))
            : Math.max(2, Math.ceil(nonMetaFields.length / 2));
    if (matched < need) return null;
    this.headerParts = parts.map((p) => p.trim());
    return map;
  }

  /**
   * Extracts fields from a delimited line using the client's explicit column map. A field
   * maps to a single 0-based column, or an array of columns whose non-empty cells are joined
   * (e.g. a multi-column address). Accepts only when a mapped strongly-typed field
   * (email/phone) is present AND validates — so this authoritative path fires on the intended
   * fixed-column rows and declines everything else (kv/JSON/binary), which then falls through
   * to the normal flow.
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
    // Require a mapped email/phone column to be present and valid — the signal that this
    // line really is one of the fixed-column rows the map was written for.
    if (present === 0 || strongPresent === 0 || strongValid === 0) return null;
    return row;
  }

  private parseDelimitedRecord(line: string): { row: Record<string, unknown>; usedHeader: boolean } | null {
    if (this.fieldSpec.length === 0) return null;
    const t = line.trim();
    if (t[0] === "{" || t[0] === "[") return null;
    const parts = this.splitBestDelimited(line);
    if (!parts) return null;

    if (this.headerMap) {
      const viaHeader = this.applyHeaderMap(line, parts);
      if (viaHeader !== undefined) return viaHeader;
      // fall through to content-based extraction below
    }
    return this.parseDelimitedByContent(parts);
  }

  /**
   * Applies `this.headerMap` to an already-split line. Returns `undefined` (not `null`) to
   * mean "no header verdict — fall through to content-based extraction", reserving `null`
   * for "this line replaced the header and produced no data row".
   */
  private applyHeaderMap(
      line: string,
      parts: string[]
  ): { row: Record<string, unknown>; usedHeader: boolean } | null | undefined {
    // Only trust the header if the line shape matches (column count close enough) or if
    // header-mapped strong fields validate. This prevents data corruption when complex CSV
    // rows with many columns hit a simple header's offsets.
    const headerColCount = this.headerParts?.length ?? 0;
    const columnCountDiff = Math.abs(headerColCount - parts.length);
    // Threshold of 2 allows minor variations (trailing commas/empty trailing columns) while
    // still catching major shape mismatches (e.g. a 5-column header vs a 30-column data row).
    let useHeaderMap = columnCountDiff <= 2;

    if (!useHeaderMap) {
      const strongFields = ["email", "phone", "zip", "date", "url"];
      useHeaderMap = strongFields.some((field) => {
        const idx = this.headerMap![field];
        return idx !== undefined && idx < parts.length && parts[idx] && this.validateField(field, parts[idx]);
      });
    }

    if (useHeaderMap) {
      const row: Record<string, unknown> = {};
      let matched = 0;
      const mappedIndices = new Set<number>(Object.values(this.headerMap!));
      for (let i = 0; i < this.fieldSpec.length; i++) {
        const field = this.fieldSpec[i];
        if (field === "meta") continue; // handled unconditionally below
        const idx = this.headerMap![field];
        const value = idx !== undefined && idx < parts.length ? parts[idx] : "";
        row[field] = value === "" || value === undefined ? null : value;
        if (row[field] !== null) matched++;
      }
      // Always collect ALL unmapped source columns into meta when the header is known,
      // regardless of whether "meta" is present in fieldSpec. This surfaces extra columns
      // (birthday, snils, passport_numbers, etc.) without requiring the caller to enumerate
      // every source column in field_spec.
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
      return matched > 0 ? { row, usedHeader: true } : null;
    }

    // Header shape mismatch and strong fields don't validate. The current line may itself be
    // a header for a later CSV section (mixed-format files often contain multiple delimited
    // sections). Try to detect a new header; if it works, install it and drop the line as a
    // header row. The following rows will use the new map.
    const newHeader = this.detectHeader(line);
    if (newHeader) {
      this.headerMap = newHeader;
      this.headerParts = parts.map((p) => p.trim());
      return null;
    }
    return undefined; // fall through to content-based extraction
  }

  /**
   * No header available: identify columns by CONTENT for strongly-validatable fields
   * (email/phone), then best-effort group remaining text columns into weak fields
   * (name/address/location), preserving everything unclaimed in meta.
   */
  private parseDelimitedByContent(parts: string[]): { row: Record<string, unknown>; usedHeader: boolean } | null {
    const row: Record<string, unknown> = {};
    let matched = 0;
    const claimed = new Set<number>();
    let strongMatched = 0;

    for (let i = 0; i < this.fieldSpec.length; i++) {
      const field = this.fieldSpec[i];
      const nf = this.normalizedFieldSpec[i];
      let value: unknown = null;
      if (nf === "email" || nf === "phone") {
        for (let j = 0; j < parts.length; j++) {
          if (claimed.has(j)) continue;
          if (this.validateField(field, parts[j])) {
            value = parts[j];
            claimed.add(j);
            break;
          }
        }
      }
      row[field] = value;
      if (value !== null) {
        matched++;
        strongMatched++;
      }
    }

    matched += this.groupWeakFieldsByContent(parts, row, claimed);

    // Preserve all remaining unclaimed source columns in meta so no data is lost, including
    // numeric IDs, OD codes, and salutations. The caller controls which columns are lifted
    // into named fields via field_spec; everything else lives in meta.
    const metaObj: Record<string, string> = {};
    for (let j = 0; j < parts.length; j++) {
      if (claimed.has(j)) continue;
      const v = String(parts[j] ?? "").trim();
      if (v !== "") metaObj[`col_${j}`] = v;
    }
    row["meta"] = Object.keys(metaObj).length ? JSON.stringify(metaObj) : null;
    if (row["meta"] !== null) matched++;

    // Accept rows with at least one strong (validatable) field, OR multi-column rows that
    // contain usable text we could map to a weak field. This prevents throwing away
    // address-only CSV lines that have no email/phone but are still structured data.
    return strongMatched > 0 || (parts.length > 4 && matched > 0) ? { row, usedHeader: false } : null;
  }

  /**
   * Best-effort assignment of non-ID-like unclaimed text columns to weak fields
   * (address/location/name) for headerless CSVs with a consistent column layout (e.g. street/
   * city/zip/country as separate columns). Groups each contiguous run of unclaimed text
   * columns into one weak field so address gets the street block and location gets the
   * city/state/zip/country block, instead of interleaving columns across fields. Mutates
   * `row` and `claimed` in place; returns how many additional fields were matched.
   */
  private groupWeakFieldsByContent(parts: string[], row: Record<string, unknown>, claimed: Set<number>): number {
    // Reject only obvious numeric IDs (7+ digits) and OD codes. Short numeric strings
    // (4-6 digits) are typically ZIP/postal codes and belong in the location grouping.
    const weakCandidates: number[] = [];
    for (let j = 0; j < parts.length; j++) {
      if (claimed.has(j)) continue;
      const v = String(parts[j] ?? "").trim();
      if (
          !v ||
          LineClassifier.DELIMITER_ONLY_RE.test(v) ||
          LineClassifier.ID_LIKE_RE.test(v) ||
          LineClassifier.SALUTATION_RE.test(v)
      ) {
        continue;
      }
      weakCandidates.push(j);
    }

    const weakFieldIndices: number[] = [];
    for (let i = 0; i < this.fieldSpec.length; i++) {
      const nf = this.normalizedFieldSpec[i];
      if (nf === "email" || nf === "phone" || nf === "zip" || nf === "date" || nf === "url" || nf === "meta") continue;
      if (row[this.fieldSpec[i]] !== null) continue;
      weakFieldIndices.push(i);
    }
    if (weakFieldIndices.length === 0 || weakCandidates.length === 0) return 0;

    // Split candidates into contiguous runs (empty columns break a run). For OD-style rows
    // the street block is the first run; city/state/zip/country may be one or more later runs.
    const runs: number[][] = [];
    let current: number[] = [];
    for (let k = 0; k < weakCandidates.length; k++) {
      const idx = weakCandidates[k];
      if (current.length === 0 || idx === weakCandidates[k - 1] + 1) {
        current.push(idx);
      } else {
        runs.push(current);
        current = [idx];
      }
    }
    if (current.length) runs.push(current);

    // Pre-split a single long run at the first postal code to separate address/location.
    if (runs.length === 1 && weakFieldIndices.length > 1) {
      const run = runs[0];
      const zipPos = run.findIndex((idx) => LineClassifier.ZIP_RE.test(String(parts[idx] ?? "").trim()));
      if (zipPos > 0) {
        runs.splice(0, 1, run.slice(0, zipPos), run.slice(zipPos));
      }
    }

    let matched = 0;
    for (let wi = 0; wi < weakFieldIndices.length; wi++) {
      const field = this.fieldSpec[weakFieldIndices[wi]];
      const isLast = wi === weakFieldIndices.length - 1;
      const chunks: number[][] = [];
      if (wi < runs.length) chunks.push(runs[wi]);
      // Merge all remaining runs into the last weak field (e.g. city/state/zip/country stay
      // together in location).
      if (isLast) {
        for (let r = wi + 1; r < runs.length; r++) chunks.push(runs[r]);
      }
      if (chunks.length === 0) continue;

      const values = chunks
          .flat()
          .map((idx) => String(parts[idx] ?? "").trim())
          .filter((v) => v !== "");
      if (values.length === 0) continue;

      for (const chunk of chunks) for (const idx of chunk) claimed.add(idx);
      row[field] = values.join(", ");
      matched++;
    }
    return matched;
  }

  // ===========================================================================================
  // Locator resolution / coercion
  // ===========================================================================================

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

  /** Normalizes a raw extracted row into output-ready string/number/boolean/null values.
   *  Returns `null` (rejecting the whole row) if any field's text content is
   *  binary-corrupted beyond the accepted threshold. */
  private coerce(row: Record<string, unknown>): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(row)) {
      if (v === null || v === undefined || v === "") {
        out[k] = null;
      } else if (typeof v === "boolean" || typeof v === "number") {
        out[k] = v;
      } else if (Array.isArray(v)) {
        const nf = this.normalizeKey(k);
        if (nf === "email" || nf === "phone" || nf === "url") {
          const first = v.find((x) => x !== null && x !== undefined && this.validateField(k, x));
          if (first !== undefined) {
            out[k] = String(first).trim();
            continue;
          }
        }
        out[k] = JSON.stringify(v);
      } else if (typeof v === "object") {
        out[k] = JSON.stringify(v);
      } else {
        const s = String(v).trim();
        // Field-level binary detection: reject rows with binary content in any field. Tabs,
        // newlines and CR are legitimate whitespace, not binary garbage.
        const binaryChars = s.match(LineClassifier.BINARY_RE) || [];
        const binaryCount = binaryChars.filter((c) => c !== "\t" && c !== "\n" && c !== "\r").length;
        if (s.length > 0 && binaryCount / s.length > LineClassifier.BINARY_RATIO_MAX) {
          // If any field has >5% binary content, reject the entire row.
          return null as unknown as Record<string, unknown>;
        }
        out[k] = s;
      }
    }
    return out;
  }

  // ===========================================================================================
  // Stateless static helpers (CSV splitting, fingerprinting). Kept as static methods rather
  // than module-level functions so the whole classifier lives in one class, per-instance
  // state stays clearly separated from what's genuinely stateless, and callers read
  // `LineClassifier.parseCsvLine(...)` instead of an unqualified free function.
  // ===========================================================================================

  /** Splits a single delimited line into cells, honoring a quote character for embedded
   *  delimiters/newlines and doubled-quote escaping. */
  private static parseCsvLine(line: string, delim: string, quoteChar: string = "\""): string[] {
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

  /** Cheap shape fingerprint used to key the per-job AI template cache: JSON lines fingerprint
   *  by sorted key set, delimited lines by delimiter + column count, everything else by
   *  length. */
  private static quickFingerprint(line: string): string {
    const trimmed = line.trim();
    if (trimmed.length === 0) return "empty";
    if (trimmed[0] === "{" || trimmed[0] === "[") {
      try {
        const parsed = JSON.parse(line);
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          return `json|${Object.keys(parsed).sort().join(",")}`;
        }
      } catch {
        /* ignore */
      }
    }
    for (const delim of LineClassifier.DELIMITER_CANDIDATES) {
      const parts = LineClassifier.parseCsvLine(line, delim, "\"");
      if (parts.length >= 3) return `csv|${delim}|${parts.length}`;
    }
    return `text|${trimmed.length}`;
  }
}
