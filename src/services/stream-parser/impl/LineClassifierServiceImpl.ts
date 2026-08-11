import pino from "pino";
import JSONbig from "json-bigint";
import { settings } from "@shared/Settings.js";
import { createLogger } from "@utils/logger/Log.js";
import { FailureClass, ColumnMap } from "@shared/models/job.js";
import { AIVerdict, ClassifyRequest } from "@service/ai-classifier/io/IAiClassifier.js";
import {AiRateLimiter, ClassifyResponse, ClassifyResult, IClassifier} from "@service/stream-parser/io/IClassifier.js";
import SafeRegexUtils from "@utils/validator/SafeRegex";
import {InstantiationError} from "@errors/InstantiationError.js";
import {aiClassifierServiceImpl} from "@service/ai-classifier/impl/AiClassifierServiceImpl";
import {RecordTemplate, RubbishTemplate} from "@shared/io/ITemplateRegistryService";

export type { ClassifyResult } from "@service/stream-parser/io/IClassifier.js";

/**
 * Minimal shape required of an AI-call rate limiter: something whose `acquire()` can be
 * awaited before making an AI request. Named so the shape isn't repeated at every use site.
 */

export class LineClassifierServiceImpl implements IClassifier
{
  /**
   * Singleton instance
   * @private
   */
  protected static instance: LineClassifierServiceImpl;

  private readonly jobId: string;
  private readonly fieldSpec: string[];
  private readonly recordTemplates: RecordTemplate[];
  private readonly rubbishTemplates: RubbishTemplate[];
  private readonly aiCache: Map<string, RecordTemplate | RubbishTemplate>;
  private headerMap: Record<string, number | number[]> | null = null;
  private headerParts: string[] | null = null;

  /** Delimiter that was used to split the header row. Once established, every
   *  subsequent data row is split using this SAME delimiter (rather than
   *  re-guessing per line) so that a free-text column (e.g. an address or tag
   *  list containing several ';' or ',' characters) can never cause a row to
   *  be split on a different, wrong delimiter than the header.
   */

  private headerDelimiter: string | null = null;
  private readonly columnMap: ColumnMap | null = null;
  private firstLine: boolean = true;
  private sqlDumpMode: boolean = false;
  private sqlCopyMode: boolean = false;
  private sqlCopyColumns: string[] | null = null;
  private sqlCopyTable: string | null = null;
  private coerceRejectsLogged: number = 0;
  private readonly logger: pino.Logger;
  private readonly normalizedFieldSpec: string[];
  private readonly aliasMap: Map<string, Set<string>>;

  /** Maps a normalized target field (e.g. "fullname") to the ordered, normalized
   *  source-side component labels that should be concatenated together to build
   *  that field's value when they appear as separate columns/keys (e.g. "full name"
   *  composed from "first name" + "last name"). Populated dynamically from the
   *  AI-resolved field mapping (per job) so any composite relationship the AI
   *  identifies - not just names - is handled generically, with no hardcoded
   *  per-field logic. Falls back to a minimal static default when AI data is
   *  unavailable for a job.
   */

  private readonly componentMap: Map<string, string[]>;
  private readonly aiRateLimiter?: AiRateLimiter;
  private readonly defaultMinMatches: number;
  private static readonly ALIASES: Record<string, string[]> = {
    email: ["email", "mail", "emailaddress", "e_mail", "emails", "useremail", "e"],
    name: ["name", "fullname", "full_name", "username", "surname", "фио", "n", "firstname", "lastname", "first_name", "last_name"],
    phone: ["phone", "mobile", "telephone", "phonenumber", "msisdn", "phones", "mobile_phone_no", "mobile_number", "телефон", "t"],
    address: ["address", "addr", "streetaddress", "addresses", "street", "адрес", "a"],
    location: ["location", "city", "country", "countryname", "county", "postcode", "postalcode", "postal", "zip", "zipcode", "state", "province", "region", "town", "geo", "locality", "location_id", "a", "город", "страна"],
  };

  /** Reverse index built once from {@link ALIASES}: every normalized alias term (e.g.
   *  "fullname", "msisdn", "a") mapped back to the static category it belongs to (e.g.
   *  "name", "phone", "location"). Lets an arbitrary job-supplied field name (e.g.
   *  "full_name", "phone_number") be recognized as belonging to a known category even
   *  though it isn't itself a top-level key of `ALIASES`, so its abbreviation aliases
   *  still apply.
   */

  private static readonly ALIAS_TO_CATEGORY: Map<string, string> = (() => {
    const m = new Map<string, string>();
    for (const [category, aliases] of Object.entries(LineClassifierServiceImpl.ALIASES))
    {
      m.set(category, category);
      for (const a of aliases) m.set(a, category);
    }
    return m;
  })();

  /** Substring keyword fallback used only when a field name doesn't directly resolve
   *  (by exact key or membership) to a static category, e.g. "email_address_1" or
   *  "contact_phone". Order matters: checked top-to-bottom, first match wins.
   */

  private static readonly CATEGORY_KEYWORDS: Array<{ category: string; re: RegExp }> = [
    { category: "email", re: /mail/ },
    { category: "phone", re: /phone|mobile|tel|msisdn/ },
    { category: "name", re: /name|surname/ },
    { category: "address", re: /address|street/ },
    { category: "location", re: /city|country|county|postcode|postal|zip|state|province|region|town|geo|locality/ },
  ];

  /**
   * Resolves the static alias category (name/email/phone/address/location) that a
   * job-supplied field most likely represents, so its abbreviation/synonym aliases
   * (e.g. "n"/"e"/"t"/"a") still apply even when the field isn't literally named
   * "name"/"email"/etc. (e.g. "full_name", "email_address", "phone_number").
   *
   * @param field - The raw field name from the job's field_spec.
   * @param nf - The normalized form of `field`.
   * @returns The static aliases for the resolved category, or `undefined` if none apply.
   */

  private static resolveStaticAliases(field: string, nf: string): string[] | undefined
  {
    const direct: string[] | undefined = LineClassifierServiceImpl.ALIASES[field] ?? LineClassifierServiceImpl.ALIASES[nf];

    if (direct)
    {
      return direct;
    }

    const viaMembership: string | undefined = LineClassifierServiceImpl.ALIAS_TO_CATEGORY.get(nf);

    if (viaMembership)
    {
      return LineClassifierServiceImpl.ALIASES[viaMembership];
    }

    for (const { category, re } of LineClassifierServiceImpl.CATEGORY_KEYWORDS)
    {
      if (re.test(nf))
      {
        return LineClassifierServiceImpl.ALIASES[category];
      }
    }

    return undefined;
  }

  /** Static fallback used only when no AI-resolved component data is available
   *  for the job (e.g. AI disabled or the call failed), so basic first+last name
   *  composition still works without AI.
   */

  private static readonly DEFAULT_COMPONENTS: Record<string, string[]> = {
    name: ["firstname", "lastname"],
  };

  private static readonly EMAIL_RE: RegExp = /^[A-Za-z0-9._%+=\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$/;
  private static readonly HEADER_LABEL_RE: RegExp = /^[\p{L}_][\p{L}\p{N} _.,\-]*$/u;

  private static readonly KV_SEG_RE: RegExp = /^\s*([A-Za-z][A-Za-z0-9 _]*?)\s*:\s*(.*)$/;

  /** Matches control/private-use/unassigned code points — true binary corruption.
   *  Deliberately excludes C1 controls (0x80-0x9F) because those code points are
   *  commonly used for printable characters when a file is mis-encoded as Latin-1.
   *  Deliberately excludes emoji/symbol categories (So/Sm/Sk), which are normal in real
   *  names/usernames and must not reject an otherwise-valid line. Reused by both the
   *  line-level binary gate and the field-level check in `coerce()`.
   */

  private static readonly BINARY_RE: RegExp = /[\0-\x08\x0B\x0C\x0E-\x1F\x7F\p{Co}\p{Cn}]/gu;
  private static readonly MAX_LINE_LENGTH: number = 64 * 1024;
  private static readonly NON_PRINTABLE_RATIO_MAX: number = 0.15;
  private static readonly BINARY_RATIO_MAX: number = 0.05;
  private static readonly NORMALIZE_CACHE_MAX: number = 2000;

  /** Columns that look like a synthetic/opaque ID rather than free text, so they're
   *  excluded from best-effort weak-field (name/address) grouping in headerless CSVs.
   */

  private static readonly ID_LIKE_RE: RegExp = /^\d{7,}$|^OD\d+$/i;
  private static readonly SALUTATION_RE: RegExp = /^(Mr|Mrs|Ms|Master|Miss)\.?$/i;
  private static readonly DELIMITER_ONLY_RE: RegExp = /^[,;]+$/;
  private static readonly ZIP_RE: RegExp = /^\d{4,6}$/;

  /** Delimiters tried, in order, when guessing a line's column separator. Shared by the
   *  column splitter and the fingerprinting helper.
   */

  private static readonly DELIMITER_CANDIDATES = [",", ";", "\t", "|"] as const;

  /** Precision-safe JSON parser: numbers that exceed Number's safe integer range are
   *  stored as strings instead of silently losing precision (e.g. int64 IDs).
   */

  private static readonly JSON_SAFE = JSONbig({ storeAsString: true });
  private readonly normalizeKeyCache: Map<string, string> = new Map<string, string>();

  /**
   * @param enforce - A function to enforce the Singleton pattern
   * @param jobId - Identifier of the job this classifier instance is bound to; used for logging and AI requests.
   * @param fieldSpec - Ordered list of target field names the classifier should extract from every line.
   * @param recordTemplates - Learned record templates (local + previously AI-discovered) available for matching.
   * @param rubbishTemplates - Known rubbish/noise signatures available for matching.
   * @param columnMap - Optional client-supplied fixed column map for headerless delimited files.
   * @param aiRateLimiter - Optional rate limiter whose `acquire()` is awaited before any AI call.
   * @param customAliases - Optional per-job alias map returned by the AI (target field -> source aliases).
   * @param customComponents - Optional per-job composite-field map returned by the AI (target field -> ordered source component labels to concatenate, e.g. "full name" -> ["first name", "last name"]).
   * @returns A new LineClassifierServiceImpl instance configured for the given job and field spec.
   */

  public constructor(enforce: () => void, jobId: string, fieldSpec: string[], recordTemplates: RecordTemplate[], rubbishTemplates: RubbishTemplate[], columnMap?: ColumnMap | null, aiRateLimiter?: AiRateLimiter | null, customAliases?: Record<string, string[]> | null, customComponents?: Record<string, string[]> | null)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE,"Cannot instantiate LineClassifierServiceImpl directly. Use getInstance()");
    }

    this.jobId = jobId;
    this.fieldSpec = fieldSpec;
    this.recordTemplates = recordTemplates;
    this.rubbishTemplates = rubbishTemplates;
    this.columnMap = columnMap && Object.keys(columnMap).length > 0 ? columnMap : null;
    this.aiRateLimiter = aiRateLimiter ?? undefined;
    this.aiCache = new Map();
    this.logger = createLogger(module);
    this.normalizedFieldSpec = fieldSpec.map((f) => this.normalizeKey(f));
    this.aliasMap = new Map<string, Set<string>>();

    // Seed every field with its own static aliases first (if any), then layer the
    // AI-resolved aliases on top. This is a UNION, never a replacement: an AI response
    // that only covers a subset of fields (e.g. just "name") must not strip the static
    // aliases (e.g. "msisdn" for phone, "useremail" for email) from every other field.
    for (const field of fieldSpec)
    {
      const nf: string = this.normalizeKey(field);
      const set: Set<string> = new Set<string>();
      set.add(nf);

      const staticAliases: string[] | undefined = LineClassifierServiceImpl.resolveStaticAliases(field, nf);
      if (staticAliases)
      {
        for (const a of staticAliases) set.add(this.normalizeKey(a));
      }

      if (customAliases)
      {
        for (const a of (customAliases[field] ?? [])) set.add(this.normalizeKey(a));
      }

      this.aliasMap.set(nf, set);
    }

    this.componentMap = new Map<string, string[]>();

    // Same union approach for composite-field components: seed from static defaults,
    // then layer AI-resolved components on top (AI wins if it identifies more/different
    // components for a field the static defaults also cover).
    for (const field of fieldSpec)
    {
      const nf: string = this.normalizeKey(field);
      const category: string = LineClassifierServiceImpl.ALIASES[field]
          ? field
          : (LineClassifierServiceImpl.ALIAS_TO_CATEGORY.get(nf) ?? nf);
      const staticParts: string[] | undefined = LineClassifierServiceImpl.DEFAULT_COMPONENTS[category];
      const aiParts: string[] | undefined = customComponents?.[field];
      const parts: string[] | undefined = (aiParts && aiParts.length > 1) ? aiParts : staticParts;

      if (parts && parts.length > 1)
      {
        this.componentMap.set(nf, parts.map((p) => this.normalizeKey(p)).filter((p) => p !== ""));
      }
    }

    this.defaultMinMatches = Math.max(1, Math.ceil(fieldSpec.filter((f) => f !== "meta").length * 0.75));
  }

  /**
   * Gets the single instance of the LineClassifierServiceImpl class.
   * @returns The single instance of the class
   */

  public static getInstance(jobId: string, fieldSpec: string[], recordTemplates: RecordTemplate[], rubbishTemplates: RubbishTemplate[], columnMap?: ColumnMap | null, aiRateLimiter?: AiRateLimiter | null, customAliases?: Record<string, string[]> | null, customComponents?: Record<string, string[]> | null): LineClassifierServiceImpl
  {
    if (!LineClassifierServiceImpl.instance || LineClassifierServiceImpl.instance.jobId !== jobId)
    {
      LineClassifierServiceImpl.instance = new LineClassifierServiceImpl(Enforce, jobId, fieldSpec, recordTemplates, rubbishTemplates, columnMap, aiRateLimiter, customAliases, customComponents);
    }

    return LineClassifierServiceImpl.instance;
  }

  /**
   * Allows an external (e.g. AI-derived) header map to be injected before
   * streaming begins. When set, the first line is treated as a header without
   * running the built-in heuristic detection. The raw header line is split
   * using the same delimited-line logic as the rest of the classifier.
   *
   * @param map - Map of field name to 0-based column index or array of indices.
   * @param headerLine - The raw CSV header line.
   */

  public setHeaderMap(map: Record<string, number | number[]>, headerLine: string): void
  {
    this.headerMap = map;
    const found: { parts: string[]; delim: string } | null = this.splitBestDelimitedWithDelim(headerLine);
    this.headerParts = found ? found.parts.map((p) => p.trim()) : [headerLine.trim()];
    this.headerDelimiter = found?.delim ?? null;
  }

  /**
   * Expands an injected or detected header map by adding unmapped columns that
   * semantically belong to a target composite field. This is dynamic and
   * pattern-driven, not a hardcoded list of column names for one file.
   */
  private expandCompositeHeaderMap(): void
  {
    if (!this.headerMap || !this.headerParts || this.fieldSpec.length === 0)
    {
      return;
    }

    const normalized = (s: string): string =>
      s.toLowerCase()
        .replace(/[-_.\s]+/g, "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "");

    const skip = /business|fax|secondary|alternate/;

    const patterns: Record<string, RegExp[]> = {
      address: [
        /(?:^|[^a-z0-9])addr(?:ess)?/,
        /(?:^|[^a-z0-9])(?:street|st|road|rd|lane|jalan|avenue|av)/,
        /(?:^|[^a-z0-9])(?:house|hse|apt|apartment|unit|block|lot|building|number|no)/,
      ],
      location: [
        /(?:^|[^a-z0-9])(?:city|town)/,
        /(?:^|[^a-z0-9])(?:county|state|province|region|district)/,
        /(?:^|[^a-z0-9])(?:postcode|postal|zip)/,
        /(?:^|[^a-z0-9])(?:country|countryname)/,
      ],
      name: [
        /first/,
        /last/,
        /sur/,
        /(?:full|complete)/,
        /name/,
      ],
      phone: [
        /(?:^|[^a-z0-9])(?:telephone|telefon|tel|mobile|cell|handphone|hp|phone)/,
      ],
      email: [
        /(?:^|[^a-z0-9])e?mail/,
      ],
    };

    const mapped = new Set<number>();
    for (const idxs of Object.values(this.headerMap))
    {
      if (idxs === undefined) continue;
      for (const i of Array.isArray(idxs) ? idxs : [idxs])
      {
        mapped.add(i);
      }
    }

    for (const field of this.fieldSpec)
    {
      const fieldPatterns = patterns[field];
      if (!fieldPatterns) continue;

      const matched = new Set<number>();
      const existing = this.headerMap[field];
      if (existing !== undefined)
      {
        for (const i of Array.isArray(existing) ? existing : [existing])
        {
          matched.add(i);
        }
      }

      for (let i = 0; i < this.headerParts.length; i++)
      {
        if (matched.has(i) || mapped.has(i)) continue;
        const norm = normalized(this.headerParts[i]);
        if (skip.test(norm)) continue;
        for (const re of fieldPatterns)
        {
          if (re.test(norm))
          {
            matched.add(i);
            break;
          }
        }
      }

      if (matched.size > 0)
      {
        const sorted = [...matched].sort((a, b) => a - b);
        this.headerMap[field] = sorted;
        for (const i of sorted)
        {
          mapped.add(i);
        }
      }
    }
  }

  /**
   * Classifies one line. Stages run in order; the first stage to return a non-null
   * result wins. See the class-level doc comment for the rationale behind the order.
   *
   * @param line - The raw line of input to classify.
   * @param _byteOffset - Byte offset of the line within its source stream (unused by current logic, kept for interface compatibility).
   * @param _byteLength - Byte length of the line within its source stream (unused by current logic, kept for interface compatibility).
   * @returns The synchronous classification verdict for this line (parsed, rubbish, or uncertain).
   */

  public classify(line: string, _byteOffset: number, _byteLength: number): ClassifyResult
  {
    const trimmed: string = line.trim();

    const sqlDumped: ClassifyResult | null = this.classifySqlDump(line, trimmed);

    if (sqlDumped)
    {
      return sqlDumped;
    }

    const gated: ClassifyResult | null = this.applyLengthAndBinaryGate(line, trimmed);

    if (gated)
    {
      return gated;
    }

    if (this.firstLine)
    {
      this.firstLine = false;

      const looksLikeStructuredRecord: boolean = trimmed[0] === "{" || trimmed[0] === "[" || this.parseKvRecord(line) !== null;

      if (!this.headerMap && !looksLikeStructuredRecord)
      {
        const header: Record<string, number | number[]> | null = this.detectHeader(line);

        if (header)
        {
          this.setHeaderMap(header, line);
        }
      }

      if (this.headerMap)
      {
        const parts: string[] | null = this.splitBestDelimited(line);
        if (parts)
        {
          this.headerParts = parts.map((p) => p.trim());
        }
        return { verdict: "rubbish", template_id: "header" };
      }
    }

    const columnMapped: ClassifyResult | null = this.classifyViaColumnMap(line);

    if (columnMapped)
    {
      return columnMapped;
    }

    const jsonParsed: { row: Record<string, unknown>; template_id: string; obj: Record<string, unknown> } | null = this.parseJsonRecord(line);
    const jsonObj: Record<string, unknown> | undefined = jsonParsed?.obj;

    if (jsonParsed)
    {
      return this.finalizeParsedOrReject(jsonParsed.row, jsonParsed.template_id);
    }

    const kvParsed: { row: Record<string, unknown>; template_id: string } | null = this.parseKvRecord(line);

    if (kvParsed)
    {
      return this.finalizeParsedOrReject(kvParsed.row, kvParsed.template_id);
    }

    const delimited: { row: Record<string, unknown>; usedHeader: boolean } | null = this.parseDelimitedRecord(line);

    if (delimited)
    {
      return this.finalizeParsedOrReject(delimited.row, delimited.usedHeader ? "csv-mapped" : "csv-auto");
    }

    const looksLikeJson: boolean = trimmed[0] === "{" || trimmed[0] === "[";

    if (looksLikeJson)
    {
      return this.classifyUnmappedJson(trimmed, jsonObj);
    }

    let cacheComputed: boolean = false;
    let cached: RecordTemplate | RubbishTemplate | undefined;

    const getCached: () => (RecordTemplate | RubbishTemplate | undefined) = (): RecordTemplate | RubbishTemplate | undefined =>
    {
      if (!cacheComputed)
      {
        cacheComputed = true;
        cached = this.aiCache.get(LineClassifierServiceImpl.quickFingerprint(line));
      }

      return cached;
    };

    const learned: ClassifyResult | null = this.classifyViaLearnedRecordTemplates(line);

    if (learned)
    {
      return learned;
    }

    const aiCachedRecord: ClassifyResult | null = this.classifyViaCachedRecordTemplate(line, getCached());

    if (aiCachedRecord)
    {
      return aiCachedRecord;
    }

    const rubbish: ClassifyResult | null = this.classifyViaRubbishTemplates(line, getCached());

    if (rubbish)
    {
      return rubbish;
    }

    return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
  }

  /**
   * Stage: length/empty/binary gate. Cheapest checks first; declined locally, never AI.
   *
   * @param line - The raw (untrimmed) line, used for the max-length check.
   * @param trimmed - The trimmed line, used for emptiness and character-ratio checks.
   * @returns A `rubbish`/`uncertain` verdict if the line is empty, oversized, or binary-corrupted; otherwise `null` to continue to later stages.
   */

  private applyLengthAndBinaryGate(line: string, trimmed: string): ClassifyResult | null
  {
    if (trimmed === "")
    {
      return {verdict: "rubbish", template_id: "length-gate"};
    }

    if (line.length > LineClassifierServiceImpl.MAX_LINE_LENGTH)
    {
      return { verdict: "uncertain", failure_class: FailureClass.TRANSFORM_ERROR };
    }

    let nonPrintable: number = 0;

    for (let i = 0; i < trimmed.length; i++)
    {
      const c: number = trimmed.charCodeAt(i);

      if ((c <= 0x08) || (c >= 0x0b && c <= 0x0c) || (c >= 0x0e && c <= 0x1f) || (c === 0x7f))
      {
        nonPrintable++;
      }
    }

    if (nonPrintable / trimmed.length > LineClassifierServiceImpl.NON_PRINTABLE_RATIO_MAX)
    {
      return { verdict: "rubbish", template_id: "binary-gate" };
    }

    let binaryCount: number = 0;
    LineClassifierServiceImpl.BINARY_RE.lastIndex = 0;

    while (LineClassifierServiceImpl.BINARY_RE.exec(trimmed) !== null)
    {
      binaryCount++;
    }

    if (binaryCount / trimmed.length > LineClassifierServiceImpl.BINARY_RATIO_MAX)
    {
      return { verdict: "rubbish", template_id: "binary-gate" };
    }

    return null;
  }

  /**
   * PostgreSQL pg_dump / COPY ... FROM stdin; handling.  All SQL scaffolding lines are
   * dropped as rubbish.  The COPY block is parsed column-by-column and mapped against the
   * configured fieldSpec, treating PostgreSQL NULL (`\N`) and empty cells as null.
   *
   * @param line - The raw input line.
   * @param trimmed - The trimmed input line.
   * @returns A verdict when the line belongs to a SQL dump, or `null` to continue with normal classification.
   */

  private classifySqlDump(line: string, trimmed: string): ClassifyResult | null
  {
    if (!this.sqlDumpMode && /^\s*--\s*PostgreSQL database dump/.test(trimmed))
    {
      this.sqlDumpMode = true;
      return { verdict: "rubbish", template_id: "pg-dump-start" };
    }

    if (!this.sqlDumpMode)
    {
      return null;
    }

    if (this.sqlCopyMode)
    {
      if (/^\s*\\\.\s*$/.test(trimmed))
      {
        this.sqlCopyMode = false;
        this.sqlCopyColumns = null;
        this.sqlCopyTable = null;
        return { verdict: "rubbish", template_id: "pg-copy-end" };
      }

      return this.parsePgCopyData(line);
    }

    const copyMatch: RegExpMatchArray | null = /^COPY\s+(?:public\.)?(\w+)\s*\(([^)]+)\)\s*FROM\s+stdin\s*;/i.exec(trimmed);

    if (copyMatch)
    {
      this.sqlCopyTable = copyMatch[1];
      this.sqlCopyColumns = copyMatch[2].split(",").map((s) => s.trim());
      this.sqlCopyMode = true;
      this.firstLine = false;
      return { verdict: "rubbish", template_id: "pg-copy-start" };
    }

    return { verdict: "rubbish", template_id: "pg-sql-junk" };
  }

  /**
   * Parse a single tab-delimited row from a PostgreSQL COPY block.
   * Maps every column to the best matching fieldSpec field using the alias map.
   * Unmapped columns are bundled into the `meta` JSON field when present in the fieldSpec.
   *
   * @param line - The raw COPY data row.
   * @returns A parsed row verdict.
   */

  private parsePgCopyData(line: string): ClassifyResult
  {
    const parts: string[] = line.split("\t");
    const row: Record<string, unknown> = {};

    for (const f of this.fieldSpec)
    {
      row[f] = null;
    }

    const meta: Record<string, unknown> = {};
    const hasMeta: boolean = this.fieldSpec.includes("meta");
    const columns: string[] = this.sqlCopyColumns ?? [];

    for (let i = 0; i < columns.length; i++)
    {
      const col: string = columns[i];
      const raw: string | null = i < parts.length ? parts[i] : null;
      const val: string | null = this.cleanPgCopyValue(raw);

      let mapped: boolean = false;

      for (const f of this.fieldSpec)
      {
        if (this.keyMatchesField(col, f))
        {
          if (f === "meta" && hasMeta)
          {
            meta[col] = val;
          }
          else
          {
            row[f] = val;
          }

          mapped = true;
          break;
        }
      }

      if (!mapped && hasMeta)
      {
        meta[col] = val;
      }
    }

    if (hasMeta)
    {
      row.meta = JSON.stringify(meta);
    }

    return this.finalizeParsedOrReject(row, "pg-copy");
  }

  /**
   * Convert PostgreSQL NULL markers and obvious placeholder/empty values to `null`.
   * Treats `\N`, `NULL`, `null`, `N/A`, `NA`, `[CHARACTER_NOT_ALLOWED]` and strings that
   * contain only dashes, asterisks, question marks or whitespace as missing data.
   *
   * @param raw - The raw cell value from a COPY row.
   * @returns The cleaned value or `null`.
   */

  private cleanPgCopyValue(raw: string | null): string | null
  {
    if (raw === null || raw === "")
    {
      return null;
    }

    const trimmed: string = raw.trim();

    if (trimmed === "")
    {
      return null;
    }

    if (trimmed === "\\N" || trimmed === "NULL" || trimmed === "null")
    {
      return null;
    }

    if (trimmed === "N/A" || trimmed === "NA" || trimmed === "[CHARACTER_NOT_ALLOWED]")
    {
      return null;
    }

    if (/^[\-_*?\s]+$/u.test(trimmed))
    {
      return null;
    }

    return trimmed;
  }

  /**
   * Stage: client-supplied explicit column map (headerless fixed-column files).
   * Authoritative for delimited rows — wins over learned templates — but only accepts a
   * line whose mapped email/phone column actually validates, so kv/JSON/binary lines
   * decline here and fall through to the structural recognizers.
   *
   * @param line - The raw line to attempt to classify via the configured column map.
   * @returns A `parsed`/`rubbish` verdict if the column map applies and a strong field validates; otherwise `null`.
   */

  private classifyViaColumnMap(line: string): ClassifyResult | null
  {
    if (!this.columnMap)
    {
      return null;
    }

    const mapped: Record<string, unknown> | null = this.applyColumnMap(line);

    if (!mapped)
    {
      return null;
    }
    return this.finalizeParsedOrReject(mapped, "csv-column-map");
  }

  /**
   * Stage: unmapped-but-valid JSON. Flattens and folds every key into field_spec + meta.
   *
   * @param trimmed - The trimmed line, expected to start with `{` or `[`.
   * @param preParsed - An already-parsed JSON object to reuse instead of re-parsing `trimmed` (avoids a duplicate `JSON.parse` when the caller already parsed it).
   * @returns A `parsed` verdict on success, or `uncertain` if the line isn't valid/extractable JSON.
   */

  private classifyUnmappedJson(trimmed: string, preParsed?: Record<string, unknown>): ClassifyResult
  {
    const parsed: Record<string, unknown> | null = preParsed ?? this.tryParseJsonObject(trimmed);

    if (!parsed)
    {
      return {verdict: "uncertain", failure_class: FailureClass.UNCERTAIN};
    }

    const extracted = this.extractFromObject(parsed, "json", undefined, true);

    if (!extracted || extracted.ambiguous)
    {
      return {verdict: "uncertain", failure_class: FailureClass.UNCERTAIN};
    }

    const coerced: Record<string, unknown> | null = this.coerce(extracted.row);

    if (coerced)
    {
      return {verdict: "parsed", row: coerced, template_id: "json"};
    }

    return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
  }

  /**
   * Stage: known learned record templates. Records have priority over rubbish; best-scoring
   * template (most present + non-empty fields) wins when several match.
   *
   * @param line - The raw line to test against every registered record template.
   * @returns A `parsed`/`rubbish` verdict for the best-scoring matching template, or `null` if none match.
   */

  private classifyViaLearnedRecordTemplates(line: string): ClassifyResult | null
  {
    let best: { row: Record<string, unknown>; template: RecordTemplate; score: number } | null = null;

    for (const t of this.recordTemplates)
    {
      if (t.length_hint !== undefined && line.length < t.length_hint)
      {
        continue;
      }

      try
      {
        const row: Record<string, unknown> | null = this.extractLine(line, t);

        if (!row)
        {
          continue;
        }

        const score: number = this.scoreExtractedRow(row);

        if (!best || score > best.score)
        {
          best = {row, template: t, score};
        }
      }
      catch
      {

      }
    }

    if (!best)
    {
      return null;
    }

    return this.finalizeParsedOrReject(best.row, best.template.template_id, best.template.version);
  }

  /**
   * Scores an extracted row: more present fields, and especially non-empty ones, win.
   *
   * @param row - The extracted field/value map to score.
   * @returns A numeric score: one point per non-empty field plus 0.1 per merely-present field.
   */

  private scoreExtractedRow(row: Record<string, unknown>): number
  {
    let meaningful: number = 0;
    let present: number = 0;
    for (const v of Object.values(row))
    {
      if (v !== undefined)
      {
        present++;

        if (v !== null && v !== "")
        {
          meaningful++;
        }
      }
    }

    return meaningful + present * 0.1;
  }

  /**
   * Stage: AI-cached record template learned earlier in this job.
   *
   * @param line - The raw line to extract using the cached template, if it is a record template.
   * @param cached - The template previously cached for this line's fingerprint, if any.
   * @returns A `parsed`/`rubbish` verdict if `cached` is a record template that extracts successfully; otherwise `null`.
   */

  private classifyViaCachedRecordTemplate(line: string, cached: RecordTemplate | RubbishTemplate | undefined): ClassifyResult | null
  {
    if (!cached || !("field_map" in cached))
    {
      return null;
    }

    const row: Record<string, unknown> | null = this.extractLine(line, cached);

    if (!row)
    {
      return null;
    }

    return this.finalizeParsedOrReject(row, cached.template_id, cached.version);
  }

  /**
   * Checks whether a rubbish template's signature matches `line` at or above the configured
   * confidence threshold. Shared by the synchronous rubbish-template stage and the AI-cache
   * resolution path so the acceptance rule can't drift between the two call sites.
   *
   * @param t - The rubbish template to test.
   * @param line - The raw line to test the signature against.
   * @returns `true` if `t.confidence` meets `settings.RUBBISH_CONFIDENCE_MIN` and `t.signature` matches `line`.
   */

  private matchesRubbishSignature(t: RubbishTemplate, line: string): boolean
  {
    return (t.confidence || 0) >= settings.RUBBISH_CONFIDENCE_MIN && SafeRegexUtils.safeRegexTest(t.signature, line);
  }

  /**
   * Stage: known high-confidence rubbish templates, then AI-cached rubbish.
   *
   * @param line - The raw line to test against rubbish signatures.
   * @param cached - The template previously cached for this line's fingerprint, if any.
   * @returns A `rubbish` verdict if a high-confidence signature (local or cached) matches; otherwise `null`.
   */

  private classifyViaRubbishTemplates(line: string, cached: RecordTemplate | RubbishTemplate | undefined): ClassifyResult | null
  {
    for (const t of this.rubbishTemplates)
    {
      if (this.matchesRubbishSignature(t, line))
      {
        return { verdict: "rubbish", template_id: t.template_id };
      }
    }

    if (cached && "signature" in cached && this.matchesRubbishSignature(cached, line))
    {
      return { verdict: "rubbish", template_id: cached.template_id };
    }

    return null;
  }

  /**
   * Coerces a row and turns it into a `parsed` result, or a `rubbish`/binary-field result
   * if coercion rejects it (field-level binary content). Centralizes the
   * coerce-then-branch pattern repeated across every "found a match" stage above.
   *
   * @param row - The raw extracted field/value map to coerce and finalize.
   * @param templateId - The template identifier to attach to the resulting verdict.
   * @param templateVersion - Optional template version to attach to the resulting verdict.
   * @returns A `parsed` verdict with the coerced row, or a `rubbish` "binary-field" verdict if coercion rejected the row.
   */

  private finalizeParsedOrReject(row: Record<string, unknown>, templateId: string, templateVersion?: number): ClassifyResult
  {
    const coerced: Record<string, unknown> | null = this.coerce(row);

    if (!coerced)
    {
      return {verdict: "rubbish", template_id: "binary-field"};
    }

    return templateVersion !== undefined
        ? { verdict: "parsed", row: coerced, template_id: templateId, template_version: templateVersion }
        : { verdict: "parsed", row: coerced, template_id: templateId };
  }

  /**
   * Classifies a line using AI, consulting and updating the per-job AI template cache.
   *
   * @param line - The raw line to classify via the AI classifier service.
   * @param contextLines - Nearby lines supplied to the AI service as classification context.
   * @param remainingBudget - Optional remaining AI-call budget; if `<= 0`, classification short-circuits to `uncertain` without calling the AI service.
   * @returns A Promise resolving to the classification verdict, annotated with `ai_calls_used`.
   * @throws Propagates any error thrown by `aiClassifierService.classifyAi`, `aiClassifierService.parseJsonLine`, or the configured `aiRateLimiter.acquire()`.
   */

  public async classifyWithAI(line: string, contextLines: string[], remainingBudget?: number): Promise<ClassifyResult>
  {
    const fp: string = LineClassifierServiceImpl.quickFingerprint(line);
    const cached: RecordTemplate | RubbishTemplate | undefined = this.aiCache.get(fp);

    if (cached)
    {
      this.logger.info("ai_cache_hit", { fingerprint: fp, template_id: cached.template_id });
      return { ...this.toResult(line, cached), ai_calls_used: 0 };
    }

    this.logger.info("ai_cache_miss", { fingerprint: fp, line_length: line.length, context_lines: contextLines.length });

    if (remainingBudget !== undefined && remainingBudget <= 0)
    {
      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN, ai_calls_used: 0 };
    }

    const req: ClassifyRequest = {
      unknown_line: line,
      field_spec: this.fieldSpec,
      context_lines: contextLines,
      job_id: this.jobId,
    };

    const trimmed: string = line.trim();
    const isJsonLine:boolean = trimmed[0] === "{" || trimmed[0] === "[";

    if (isJsonLine)
    {
      this.logger.info("ai_call_initiated", { fingerprint: fp, line_length: line.length, context_lines: contextLines.length, reason: "json_parse" });
      const { result, ai_calls_used } = await this.tryDiscoverJsonFields(line, aiClassifierServiceImpl.parseJsonLine);

      if (result)
      {
        return {...result, ai_calls_used};
      }

      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN, ai_calls_used };
    }

    this.logger.info("ai_call_initiated", { fingerprint: fp, line_length: line.length, context_lines: contextLines.length });

    if (this.aiRateLimiter)
    {
      await this.aiRateLimiter.acquire();
    }

    const resp: ClassifyResponse = await aiClassifierServiceImpl.classifyAi(req);
    const ai_calls_used = 1;

    if (resp.kind === AIVerdict.UNCERTAIN || !resp.template)
    {
      this.logger.info("ai_call_uncertain", { fingerprint: fp, kind: resp.kind });
      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN, ai_calls_used };
    }

    this.aiCache.set(fp, resp.template);

    this.logger.info("ai_cache_saved", { fingerprint: fp, template_id: resp.template.template_id });

    const t: RecordTemplate | RubbishTemplate = resp.template;

    if ("field_map" in t && !this.recordTemplates.some((r) => r.template_id === t.template_id))
    {
      this.recordTemplates.push(t as RecordTemplate);
      this.logger.info("ai_template_learned", { template_id: t.template_id, kind: "record", source: "ai_call" });
    }
    else if ("signature" in t && !this.rubbishTemplates.some((r) => r.template_id === t.template_id))
    {
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
   *
   * @param line - The raw JSON (or quoted-JSON-string) line to discover fields from.
   * @param parseJsonLine - The AI service function used to parse the line into target fields.
   * @returns A Promise resolving to the parsed result (or `null` if nothing could be extracted) together with the number of AI calls used.
   * @throws Does not throw: internal AI/parse failures are caught and logged, falling back to local extraction.
   */

  private async tryDiscoverJsonFields(line: string, parseJsonLine: (jsonLine: string, targets: string[]) => Promise<Record<string, unknown> | null>): Promise<{ result: ClassifyResult | null; ai_calls_used: number }>
  {
    const template: string = line.trim();

    if (template[0] !== "{" && template[0] !== "[" && !(template.length >= 2 && template[0] === "\"" && template[template.length - 1] === "\""))
    {
      return { result: null, ai_calls_used: 0 };
    }

    let parsed: unknown;

    try
    {
      parsed = LineClassifierServiceImpl.JSON_SAFE.parse(template);
    }
    catch
    {
      return { result: null, ai_calls_used: 0 };
    }

    if (!parsed || typeof parsed !== "object")
    {
      return {result: null, ai_calls_used: 0};
    }

    let obj: Record<string, unknown> = parsed as Record<string, unknown>;

    if (Array.isArray(parsed))
    {
      const first = parsed.find((x) => x && typeof x === "object" && !Array.isArray(x)) as | Record<string, unknown> | undefined;

      if (!first)
      {
        return {result: null, ai_calls_used: 0};
      }

      obj = first;
    }

    try
    {
      if (this.aiRateLimiter)
      {
        await this.aiRateLimiter.acquire();
      }

      const aiRow: Record<string, unknown> | null = await parseJsonLine(line, this.fieldSpec);

      if (aiRow && typeof aiRow === "object" && !Array.isArray(aiRow))
      {
        const coerced: Record<string, unknown> | null = this.coerce(aiRow);

        if (coerced)
        {
          this.logger.info("ai_json_parse_succeeded", { fingerprint: LineClassifierServiceImpl.quickFingerprint(line), keys: Object.keys(coerced).length });

          return { result: { verdict: "parsed", row: coerced, template_id: "ai-json" }, ai_calls_used: 1 };
        }
      }
    }
    catch (err)
    {
      this.logger.warn("ai_json_parse_failed", { error: String(err) });
    }

    const extracted = this.extractFromObject(obj, "json", undefined, true);

    if (extracted && !extracted.ambiguous)
    {
      const coerced: Record<string, unknown> | null = this.coerce(extracted.row);

      if (coerced)
      {
        return {result: {verdict: "parsed", row: coerced, template_id: "json"}, ai_calls_used: 1};
      }
    }

    return { result: null, ai_calls_used: 1 };
  }

  /**
   * Runs classification with a safety timeout so pathological lines can't hang the pipeline.
   *
   * @param line - The raw line to classify.
   * @param contextLines - Nearby lines supplied to the AI service as classification context.
   * @param timeoutMs - Maximum time, in milliseconds, to wait before falling back to `uncertain`.
   * @param remainingBudget - Optional remaining AI-call budget passed through to `classifyWithAI`.
   * @returns A Promise resolving to the classification verdict, or an `uncertain` verdict if `timeoutMs` elapses first.
   * @throws Propagates any error thrown by the underlying `classifyWithAI` call.
   */

  public async classifyWithTimeout(line: string, contextLines: string[], timeoutMs: number, remainingBudget?: number): Promise<ClassifyResult>
  {
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
   *
   * @param row - The finalized row to check before it is emitted.
   * @returns `true` if every populated email/phone field validates (or none are populated); `false` otherwise.
   */

  public rowStrongFieldsOk(row: Record<string, unknown>): boolean
  {
    for (let i = 0; i < this.fieldSpec.length; i++)
    {
      const field: string = this.fieldSpec[i];
      const normalizedFields: string = this.normalizedFieldSpec[i];

      if (normalizedFields !== "email" && normalizedFields !== "phone")
      {
        continue;
      }

      const v: unknown = row[field];

      if (v !== undefined && v !== null && String(v).trim() !== "" && !this.validateField(field, v))
      {
        return false;
      }
    }

    return true;
  }

  /**
   * Nulls out invalid email/phone values in-place so the rest of the row can still
   * be emitted.
   *
   * @param row - The finalized row to clean in-place.
   */

  public cleanInvalidStrongFields(row: Record<string, unknown>): void
  {
    for (let i = 0; i < this.fieldSpec.length; i++)
    {
      const field: string = this.fieldSpec[i];
      const normalizedField: string = this.normalizedFieldSpec[i];

      if (normalizedField !== "email" && normalizedField !== "phone")
      {
        continue;
      }

      const v: unknown = row[field];

      if (v !== undefined && v !== null && String(v).trim() !== "" && !this.validateField(field, v))
      {
        row[field] = null;
      }
    }
  }

  /**
   * Resolves a learned/AI template (record or rubbish) against a line, used by the AI-cache
   * hit path in `classifyWithAI`.
   *
   * @param line - The raw line to resolve against the given template.
   * @param tmpl - The record or rubbish template to apply.
   * @returns A `parsed`/`rubbish` verdict if the template applies and (for rubbish) meets the confidence threshold; otherwise `uncertain`.
   */

  private toResult(line: string, tmpl: RecordTemplate | RubbishTemplate): ClassifyResult
  {
    if ("signature" in tmpl)
    {
      if (this.matchesRubbishSignature(tmpl, line))
      {
        return { verdict: "rubbish", template_id: tmpl.template_id };
      }

      return { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
    }

    const row: Record<string, unknown> | null = this.extractLine(line, tmpl);

    if (!row)
    {
      return {verdict: "uncertain", failure_class: FailureClass.UNCERTAIN};
    }

    return this.finalizeParsedOrReject(row, tmpl.template_id, tmpl.version);
  }

  /**
   * Extracts a field/value row from a line using a record template's structure and field_map.
   *
   * @param line - The raw line to extract from.
   * @param rec - The record template describing how to parse the line and locate each field.
   * @returns The extracted row, or `null` if the line's structure doesn't parse, no field was present, or a strongly-typed field is present but invalid.
   */

  private extractLine(line: string, rec: RecordTemplate): Record<string, unknown> | null
  {
    const parsed: string | unknown[] | Record<string, unknown> | null = this.parseStructure(line, rec);

    if (!parsed)
    {
      return null;
    }

    const row: Record<string, unknown> = {};
    let presentCount: number = 0;
    let strongPresent: number = 0;
    let strongValid: number = 0;

    for (let i = 0; i < this.fieldSpec.length; i++)
    {
      const field: string = this.fieldSpec[i];

      const loc: { locator: string; type: string } = rec.field_map[field];

      if (!loc)
      {
        row[field] = undefined;
        continue;
      }

      const locator: string | undefined = this.resolveLocatorString(loc);
      const value: unknown = locator ? this.applyLocator(line, parsed, locator) : undefined;

      if (value !== undefined)
      {
        presentCount++;
      }

      const normalizedFields: string = this.normalizedFieldSpec[i];

      if ((normalizedFields === "email" || normalizedFields === "phone") && value !== undefined && value !== null && String(value).trim() !== "")
      {
        strongPresent++;

        if (this.validateField(field, value))
        {
          strongValid++;
        }
      }

      row[field] = value;
    }

    if (presentCount === 0)
    {
      return null;
    }

    if (strongPresent > 0 && strongValid === 0)
    {
      return null;
    }

    return row;
  }

  /**
   * Normalizes a field_map locator entry to its canonical `"kind:value"` string form.
   * Accepts the current `{ locator: string }` shape, a legacy raw locator string, or
   * older `{ index | key | regex }` objects that may still exist in cache/DB.
   *
   * @param loc - The raw locator value from a template's field_map (string or legacy object shape).
   * @returns The canonical `"kind:value"` locator string, or `undefined` if `loc` doesn't match any known shape.
   */

  private resolveLocatorString(loc: unknown): string | undefined
  {
    if (typeof loc === "string")
    {
      return loc;
    }

    const rawLoc = loc as Record<string, unknown>;

    if (typeof rawLoc.locator === "string")
    {
      return rawLoc.locator;
    }

    if (typeof rawLoc.index === "number")
    {
      return `index:${rawLoc.index}`;
    }

    if (typeof rawLoc.key === "string")
    {
      return `key:${rawLoc.key}`;
    }

    if (typeof rawLoc.regex === "string")
    {
      return `regex:${rawLoc.regex}`;
    }

    return undefined;
  }

  /**
   * Parses a line into the intermediate structure (object/array/string) a record
   * template's locators can be applied to, according to the template's declared structure kind.
   *
   * @param line - The raw line to parse.
   * @param rec - The record template whose `structure` (json/kv/csv/regex/fixed) determines the parse strategy.
   * @returns The parsed structure appropriate to `rec.structure`, or `null` if parsing failed or produced nothing usable.
   */

  private parseStructure(line: string, rec: RecordTemplate): string | unknown[] | Record<string, unknown> | null
  {
    if (rec.structure === "json")
    {
      if (line[0] !== "{" && line[0] !== "[") return null;

      try
      {
        const obj = LineClassifierServiceImpl.JSON_SAFE.parse(line);

        if (obj && typeof obj === "object" && !Array.isArray(obj))
        {
          return obj;
        }
      }
      catch
      {
        return null;
      }
    }

    if (rec.structure === "kv")
    {
      const obj: Record<string, string> = {};

      let parts: string[];

      if (line.includes(" - "))
      {
        parts = line.split(" - ");
      }
      else if (line.includes(";"))
      {
        parts = line.split(";");
      }
      else
      {
        parts = line.split(/\s+/);
      }

      for (const part of parts)
      {
        let k: string | undefined, v: string | undefined;

        if (part.includes("="))
        {
          [k, v] = part.split("=", 2);
        }
        else if (part.includes(":"))
        {
          [k, v] = part.split(":", 2);
        }
        else if (part.includes("-"))
        {
          [k, v] = part.split("-", 2);
        }

        if (k && v !== undefined)
        {
          obj[k.trim()] = v.trim();
        }
      }

      return Object.keys(obj).length > 0 ? obj : null;
    }

    if (rec.structure === "csv")
    {
      const delim = rec.delimiter ?? ",";
      return LineClassifierServiceImpl.parseCsvLine(line, delim, LineClassifierServiceImpl.csvQuoteFor(delim));
    }

    if (rec.structure === "regex" || rec.structure === "fixed")
    {
      return line;
    }

    return null;
  }

  /**
   * Normalizes a field/column/key label for tolerant matching (lower-cased, non-alphanumerics
   * stripped). Results are memoized in a bounded cache since the same small set of labels
   * (field names, aliases, header cells) recurs across every line in a job.
   *
   * @param s - The raw label to normalize.
   * @returns The normalized (lowercase, alphanumeric-only) form of `s`.
   */

  private normalizeKey(s: string): string
  {
    const cached: string | undefined = this.normalizeKeyCache.get(s);

    if (cached !== undefined)
    {
      return cached;
    }

    const out: string = s.toLowerCase().replace(/[^\p{L}\p{N}]/gu, "");

    if (this.normalizeKeyCache.size < LineClassifierServiceImpl.NORMALIZE_CACHE_MAX)
    {
      this.normalizeKeyCache.set(s, out);
    }

    return out;
  }

  /**
   * Does a source key/column label correspond to a requested field (exact or alias)?
   *
   * @param key - The source key/column label (e.g. a CSV header cell or object key).
   * @param field - The target field name from the field spec to compare against.
   * @returns `true` if `key` normalizes to `field` directly or to one of its known aliases.
   */

  /**
   * Is the given normalized target field a composite field — i.e. one the AI (or the
   * static default) has identified as built from 2+ separate source components
   * (e.g. "full name" from "first name" + "last name")? Fully data-driven via
   * `componentMap`, so any composite relationship the AI identifies for any field
   * is handled the same way, with no per-field hardcoding.
   *
   * @param nf - The already-normalized target field name.
   * @returns `true` if this field has 2+ registered components to combine.
   */

  private isCompositeField(nf: string): boolean
  {
    const components: string[] | undefined = this.componentMap.get(nf);
    return components !== undefined && components.length > 1;
  }

  private keyMatchesField(key: string, field: string): boolean
  {
    const normalizedKey: string = this.normalizeKey(key);
    const normalizedField: string = this.normalizeKey(field);

    if (!normalizedKey)
    {
      return false;
    }

    const tokens: string[] = key
      .split(/[,;]+/)
      .map((t) => this.normalizeKey(t.trim()))
      .filter((t) => t.length > 0);

    if (tokens.length === 0)
    {
      return false;
    }

    const aliases: Set<string> = this.aliasMap.get(normalizedField) ?? new Set<string>([normalizedField]);

    if (tokens.some((t) => aliases.has(t)))
    {
      return true;
    }

    const components: string[] | undefined = this.componentMap.get(normalizedField);

    if (components && components.length > 1)
    {
      const matchedComponents: Set<string> = new Set(tokens.filter((t) => components.includes(t)));
      return matchedComponents.size >= Math.min(2, components.length);
    }

    return tokens.some((token) => {
      for (const a of aliases)
      {
        if (token === a)
        {
          return true;
        }

        if (token.length > a.length && token.startsWith(a) && /^\d*$/.test(token.slice(a.length)))
        {
          return true;
        }
      }

      return false;
    });
  }

  /**
   * Content validation, used to identify columns in a headerless CSV and to reject junk.
   *
   * @param field - The target field name, used to determine which validation rule applies (email/phone/other).
   * @param value - The candidate value to validate.
   * @returns `true` if `value` is non-empty and (for email/phone) matches the expected format; `true` for any other non-empty field.
   */

  private validateField(field: string, value: unknown): boolean
  {
    if (value === null || value === undefined)
    {
      return false;
    }

    const v: string = String(value).trim();

    if (v === "")
    {
      return false;
    }

    const normalizedField: string = this.normalizeKey(field);

    if (normalizedField === "email")
    {
      return LineClassifierServiceImpl.EMAIL_RE.test(v);
    }

    if (normalizedField === "phone")
    {
      if (v.includes("@"))
      {
        return false;
      }

      const digits: string = v.replace(/\D/g, "");
      return digits.length >= 10 && digits.length <= 15;
    }

    return true;
  }

  /**
   * Recursively flattens a nested object into dot-notation keys. Arrays of objects are
   * flattened with numeric indexes (e.g. `messages[0].body`); scalar arrays are kept as
   * arrays so they are JSON-stringified in meta/output rather than mangled to
   * "[object Object]".
   *
   * @param obj - The (possibly nested) object to flatten.
   * @param prefix - The dot-notation key prefix accumulated so far during recursion (used internally; omit when calling from outside).
   * @returns A flat object whose keys are dot/bracket-notation paths into the original structure.
   */

  private flattenObject(obj: Record<string, unknown>, prefix = ""): Record<string, unknown>
  {
    const out: Record<string, unknown> = {};

    for (const [k, v] of Object.entries(obj))
    {
      const key: string = prefix ? `${prefix}.${k}` : k;

      if (v !== null && typeof v === "object" && !Array.isArray(v))
      {
        const nested: Record<string, unknown> = this.flattenObject(v as Record<string, unknown>, key);

        if (Object.keys(nested).length === 0)
        {
          out[key] = {};
        }
        else
        {
          Object.assign(out, nested);
        }
        continue;
      }

      if (Array.isArray(v))
      {
        const allObjects: boolean = v?.length > 0 && v?.every((x) => x !== null && typeof x === "object" && !Array.isArray(x));

        if (allObjects)
        {
          for (let i = 0; i < v?.length; i++)
          {
            Object.assign(out, this.flattenObject(v[i] as Record<string, unknown>, `${key}[${i}]`));
          }
        }
        else
        {
          out[key] = v;
        }
        continue;
      }

      let sv: unknown = v;

      if (typeof sv === "string")
      {
        const t: string = sv.trim();

        if (t.length >= 2 && t[0] === "\"" && t[t.length - 1] === "\"")
        {
          try
          {
            const inner = LineClassifierServiceImpl.JSON_SAFE.parse(sv);
            if (typeof inner === "string") sv = inner;
          }
          catch
          {
            /* not a JSON string */
          }
        }
      }

      if (typeof sv === "string" && (sv.trim().startsWith("{") || sv.trim().startsWith("[")) && key !== "meta")
      {
        try
        {
          const parsed = LineClassifierServiceImpl.JSON_SAFE.parse(sv);

          if (parsed && typeof parsed === "object")
          {

            if (Array.isArray(parsed))
            {
              const allObjects: boolean = parsed.length > 0 && parsed.every((x) => x !== null && typeof x === "object" && !Array.isArray(x));

              if (allObjects)
              {
                for (let i = 0; i < parsed.length; i++)
                {
                  Object.assign(out, this.flattenObject(parsed[i] as Record<string, unknown>, `${key}[${i}]`));
                }
              }
              else
              {
                out[key] = parsed;
              }

              continue;
            }
            const nestedParsed: Record<string, unknown> = this.flattenObject(parsed as Record<string, unknown>, key);

            if (Object.keys(nestedParsed).length === 0)
            {
              out[key] = {};
            }
            else
            {
              Object.assign(out, nestedParsed);
            }
            continue;
          }
        }
        catch
        {
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
   *
   * @param rawObj - The source object (JSON record or KV-parsed object) to extract fields from.
   * @param templateId - The template identifier to attach to the result on success.
   * @param fieldSpecOverride - Optional field list to use instead of the instance's `fieldSpec` (used by the AI-JSON discovery path).
   * @param loose - If `true`, accepts the extraction even if the normal match-count/strong-field threshold isn't met.
   * @returns The extracted row, template id, and an optional `ambiguous` flag, or `null` if extraction didn't meet the acceptance threshold and `loose` is `false`.
   */

  private extractFromObject(rawObj: Record<string, unknown>, templateId: string, fieldSpecOverride?: string[], loose = false): { row: Record<string, unknown>; template_id: string; ambiguous?: boolean } | null
  {
    const obj: Record<string, unknown> = this.flattenObject(rawObj);
    const spec: string[] = fieldSpecOverride ?? this.fieldSpec;
    const normalizedSpec: string[] = fieldSpecOverride ? fieldSpecOverride.map((f) => this.normalizeKey(f)) : this.normalizedFieldSpec;
    const row: Record<string, unknown> = {};
    let matched: number = 0;
    let strong: number = 0;
    const normalizedObjKeys = new Map<string, unknown>();
    const leafToFull = new Map<string, string>();
    const consumedKeys = new Set<string>();
    const extraMeta: Record<string, unknown> = {};

    for (const [k, val] of Object.entries(obj))
    {
      const nk: string = this.normalizeKey(k);

      if (!normalizedObjKeys.has(nk))
      {
        normalizedObjKeys.set(nk, val);
      }

      if (k.includes("."))
      {
        const leaf: string = k.slice(k.lastIndexOf(".") + 1);
        const nLeaf: string = this.normalizeKey(leaf);

        if (!normalizedObjKeys.has(nLeaf))
        {
          normalizedObjKeys.set(nLeaf, val);
          leafToFull.set(nLeaf, nk);
        }
      }
    }

    for (let i = 0; i < spec.length; i++)
    {
      const field: string = spec[i];

      if (field === "meta")
      {
        continue;
      }

      const nf: string = normalizedSpec[i];
      let value: unknown = normalizedObjKeys.get(nf);
      let matchedKey: string | undefined = nf;

      if (value !== undefined)
      {
        const vFull: string | undefined = leafToFull.get(nf);
        if (consumedKeys.has(nf) || (vFull && consumedKeys.has(vFull)))
        {
          value = undefined;
        }
      }

      if (value === undefined)
      {
        const aliases: Set<string> | undefined = this.aliasMap.get(nf);

        if (aliases)
        {
          for (const a of aliases)
          {
            const av: unknown = normalizedObjKeys.get(a);

            if (av !== undefined)
            {
              const aFull: string | undefined = leafToFull.get(a);
              if (consumedKeys.has(a) || (aFull && consumedKeys.has(aFull))) continue;
              value = av;
              matchedKey = a;
              break;
            }
          }
        }
      }

      if (value === undefined)
      {
        const found = this.locateNestedFieldValue(obj, field, nf, consumedKeys);

        if (found)
        {
          value = found.value;
          matchedKey = found.key;
        }
      }

      if (this.isCompositeField(nf))
      {
        const inferred = this.inferCompositeFromParts(nf, normalizedObjKeys, value);

        if (inferred)
        {
          value = inferred.value;
          matchedKey = inferred.key;

          for (const consumedComponent of inferred.consumedComponents)
          {
            consumedKeys.add(consumedComponent);
          }
        }
      }

      if (value !== undefined && value !== null && String(value).trim() !== "")
      {
        if (Array.isArray(value))
        {
          const meaningful: unknown[] = value.filter((x) => x !== null && x !== undefined && String(x).trim() !== "");

          row[field] = meaningful.length > 0 ? meaningful[0] : null;

          if (meaningful.length > 1)
          {
            extraMeta[`${field}_all`] = meaningful;
          }
        }
        else
        {
          row[field] = value;
        }

        matched++;

        consumedKeys.add(matchedKey!);
        const fullKey = leafToFull.get(matchedKey!);

        if (fullKey)
        {
          consumedKeys.add(fullKey);
        }

        if ((nf === "email" || nf === "phone") && this.validateField(field, row[field]))
        {
          strong++;
        }
      }
      else
      {
        row[field] = null;
      }
    }

    const metaObj: Record<string, unknown> = {};

    for (const [k, v] of Object.entries(obj))
    {
      if (!consumedKeys.has(this.normalizeKey(k)) && v !== undefined)
      {
        metaObj[k] = typeof v === "string" ? v.trim() : v;
      }
    }

    Object.assign(metaObj, extraMeta);
    row["meta"] = Object.keys(metaObj).length ? JSON.stringify(metaObj) : null;

    let ambiguous = false;
    for (let i = 0; i < spec.length; i++)
    {
      const field = spec[i];
      if (field === "meta" || row[field] !== null) continue;
      const nf = normalizedSpec[i];
      const looseCandidates = Object.keys(obj).filter((k) =>
        !consumedKeys.has(this.normalizeKey(k)) && this.normalizeKey(k).includes(nf.slice(0, 3))
      );
      if (looseCandidates.length > 1) ambiguous = true;
    }

    const minMatches: number = fieldSpecOverride ? Math.max(1, Math.ceil(fieldSpecOverride.filter((f) => f !== "meta").length * 0.75)) : this.defaultMinMatches;

    const accept: boolean = strong >= 1 || matched >= minMatches;

    return accept || loose ? { row, template_id: templateId, ambiguous } : null;
  }

  /**
   * Best-effort nested/renamed key match: segment match, prefix match, or (for non-name/
   * address fields) substring match against the field's accepted alias set.
   *
   * @param obj - The flattened source object to search.
   * @param field - The target field name (used for value validation, e.g. email/phone format).
   * @param nf - The normalized form of `field`, used to build the accepted-alias set.
   * @param consumedKeys - Normalized keys already claimed by other fields, excluded from consideration.
   * @returns The best-scoring matching value and its normalized key, or `null` if nothing matched.
   */

  private locateNestedFieldValue(obj: Record<string, unknown>, field: string, nf: string, consumedKeys: Set<string>): { value: unknown; key: string } | null
  {
    const accepted: string[] = Array.from(new Set([nf, ...(this.aliasMap.get(nf) || [])]));
    const noSubstring: boolean = nf === "name" || nf === "address" || nf === "location";
    let bestScore: number = -1;
    let bestValue: unknown;
    let bestKey: string | undefined;
    let tiedCount: number = 0;

    for (const [k, val] of Object.entries(obj))
    {
      if (val === null || val === undefined || val === "")
      {
        continue;
      }

      const nk: string = this.normalizeKey(k);

      if (consumedKeys.has(nk))
      {
        continue;
      }

      const segments: string[] = k.split(/[.[\]]+/).filter(Boolean).map((s) => this.normalizeKey(s));
      const segmentMatch: boolean = segments.some((s) => accepted.includes(s));
      const prefixMatch: boolean = accepted.some((a) => a.length >= 2 && nk.startsWith(a));
      const substringMatch: boolean = !noSubstring && accepted.some((a) => a.length >= 3 && nk.includes(a));

      if (!segmentMatch && !prefixMatch && !substringMatch)
      {
        continue;
      }

      const strVal: string = typeof val === "string" ? val : JSON.stringify(val);

      if (strVal.trim() === "")
      {
        continue;
      }

      const isString: boolean = typeof val === "string";
      const isValid: boolean = this.validateField(field, val);
      const score: number = (isString ? 100000 : 0) + (isValid ? 10000 : 0);

      if (score > bestScore)
      {
        bestScore = score;
        bestValue = val;
        bestKey = nk;
        tiedCount = 1;
      }
      else if (score === bestScore)
      {
        tiedCount++;
      }
    }

    if (tiedCount > 1)
    {
      return null;
    }

    return bestValue !== undefined ? { value: bestValue, key: bestKey! } : null;
  }

  /**
   * Generic composite-field resolver: if this target field has 2+ registered source
   * components (from `componentMap`, driven by the AI-resolved job mapping or the
   * static default) and 2+ of them are present as separate source keys with
   * non-empty string values, join them (in the AI-given order) into a single value.
   * Not specific to names — works for any composite relationship the AI identifies
   * (e.g. full name from first+last, full address from street+city+zip, etc.).
   *
   * @param nf - The already-normalized target field name.
   * @param normalizedObjKeys - Map of normalized source keys to their values.
   * @param currentValue - The value currently selected for this field, if any.
   * @returns The combined value, its synthetic key, and which component keys were consumed; or `null` if fewer than 2 components were found or the current value already contains them all.
   */

  private inferCompositeFromParts(nf: string, normalizedObjKeys: Map<string, unknown>, currentValue: unknown): { value: string; key: string; consumedComponents: string[] } | null
  {
    const components: string[] | undefined = this.componentMap.get(nf);

    if (!components || components.length < 2)
    {
      return null;
    }

    const foundValues: string[] = [];
    const consumedComponents: string[] = [];

    for (const component of components)
    {
      const v: unknown = normalizedObjKeys.get(component);

      if (typeof v === "string" && v.trim())
      {
        foundValues.push(v.trim());
        consumedComponents.push(component);
      }
    }

    if (foundValues.length < 2)
    {
      return null;
    }

    const current: string = typeof currentValue === "string" ? this.normalizeKey(currentValue) : "";
    const allPartsAlreadyPresent: boolean = current !== "" && foundValues.every((v) => current.includes(this.normalizeKey(v)));

    if (allPartsAlreadyPresent)
    {
      return null;
    }

    const combined: string = foundValues.join(" ").trim();

    return combined ? { value: combined, key: consumedComponents.join("+"), consumedComponents } : null;
  }

  /**
   * Attempts to parse a line as a JSON record and extract field_spec fields from it.
   * Also handles a line that is itself a JSON-encoded string wrapping another JSON value.
   *
   * @param line - The raw line to attempt to parse as JSON.
   * @returns The extracted row, template id, and parsed object; or `null` if the line isn't valid/extractable JSON.
   */

  private parseJsonRecord(line: string): { row: Record<string, unknown>; template_id: string; obj: Record<string, unknown> } | null
  {
    const t: string = line.trim();
    const obj: Record<string, unknown> | null = this.tryParseJsonObject(t);

    if (!obj)
    {
      if (t.length >= 2 && t[0] === "\"" && t[t.length - 1] === "\"")
      {
        try
        {
          const inner = LineClassifierServiceImpl.JSON_SAFE.parse(t) as string;
          return this.parseJsonRecord(inner);
        }
        catch
        {
          /* fall through */
        }
      }
      return null;
    }

    const extracted = this.extractFromObject(obj, "json");

    if (extracted?.ambiguous)
    {
      return null;
    }

    return extracted ? { row: extracted.row, template_id: extracted.template_id, obj } : null;
  }

  /**
   * Attempts to parse a line as `key: value` (or `key: value - key: value - ...`) segments
   * and extract field_spec fields from the resulting object.
   *
   * @param line - The raw line to attempt to parse as key/value segments.
   * @returns The extracted row and template id, or `null` if the line contains no `:` or no segments parsed.
   */

  private parseKvRecord(line: string): { row: Record<string, unknown>; template_id: string } | null
  {
    if (!line.includes(":"))
    {
      return null;
    }

    const obj: Record<string, string> = {};

    for (const seg of line.split(/\s+-\s+/))
    {
      const m: RegExpExecArray | null = LineClassifierServiceImpl.KV_SEG_RE.exec(seg);
      if (m) obj[m[1].trim()] = m[2].trim();
    }

    if (Object.keys(obj).length === 0)
    {
      return null;
    }

    return this.extractFromObject(obj, "kv");
  }

  /**
   * Parses a trimmed line as JSON, unwrapping an array to its first object element.
   *
   * @param trimmed - The trimmed line, expected to start with `{` or `[`.
   * @returns The parsed object (or first object element of a parsed array), or `null` if parsing fails or produces no usable object.
   */

  private tryParseJsonObject(trimmed: string): Record<string, unknown> | null
  {
    if (trimmed[0] !== "{" && trimmed[0] !== "[")
    {
      return null;
    }

    let parsed: unknown;

    try
    {
      parsed = LineClassifierServiceImpl.JSON_SAFE.parse(trimmed);
    }
    catch
    {
      return null;
    }

    if (Array.isArray(parsed))
    {
      const first = parsed.find((x) => x && typeof x === "object" && !Array.isArray(x)) as
          | Record<string, unknown>
          | undefined;

      return first ?? null;
    }

    if (!parsed || typeof parsed !== "object")
    {
      return null;
    }

    return parsed as Record<string, unknown>;
  }

  /**
   * Splits a line using each candidate delimiter and returns the split that yields the
   * most columns (with at least 2), used to guess a delimited line's column separator.
   *
   * @param line - The raw line to split.
   * @returns The best (widest, ≥2-column) split found across all delimiter candidates present in the line, or `null` if none produced ≥2 columns.
   */

  private splitBestDelimited(line: string): string[] | null
  {
    return this.splitBestDelimitedWithDelim(line)?.parts ?? null;
  }

  /**
   * Same delimiter-guessing logic as `splitBestDelimited`, but also returns which
   * delimiter produced the winning split so callers can remember and reuse it
   * (see `headerDelimiter`).
   *
   * @param line - The raw line to split.
   * @returns The winning split and its delimiter, or `null` if no candidate produced ≥2 columns.
   */

  private splitBestDelimitedWithDelim(line: string): { parts: string[]; delim: string } | null
  {
    let best: { parts: string[]; delim: string } | null = null;

    for (const delim of LineClassifierServiceImpl.DELIMITER_CANDIDATES)
    {
      if (!line.includes(delim))
      {
        continue;
      }

      const parts: string[] = LineClassifierServiceImpl.parseCsvLine(line, delim, LineClassifierServiceImpl.csvQuoteFor(delim));

      if (parts.length < 2)
      {
        continue;
      }

      if (!best || parts.length > best.parts.length)
      {
        best = { parts, delim };
      }
    }
    return best;
  }

  /**
   * Treats the first line as a header only when it is unmistakably one: ≥2 cells, every cell
   * a bare label with NO data content (no '@', no ≥7-digit run), AND it locates a MAJORITY
   * (≥ half, and ≥2) of the requested fields. This prevents a plain words-only first DATA row
   * (e.g. "Cell,Berlin") from being misread as a header — which would both drop that record
   * and install a wrong column map that corrupts every following row.
   *
   * @param line - The candidate header line.
   * @returns A map of field name to column index if the line qualifies as a header; otherwise `null`.
   */

  public detectHeader(line: string): Record<string, number | number[]> | null
  {
    const found: { parts: string[]; delim: string } | null = this.splitBestDelimitedWithDelim(line);

    if (!found || found.parts.length < 2)
    {
      return null;
    }

    const parts: string[] = found.parts;

    for (const c of parts)
    {
      const v: string = c.trim();

      if (v === "")
      {
        continue;
      }

      if (v.length > 1 && v[0] === "{" && v[v.length - 1] === "}")
      {
        continue;
      }

      if (v.includes("@") || v.replace(/\D/g, "").length >= 7)
      {
        return null;
      }

      if (!LineClassifierServiceImpl.HEADER_LABEL_RE.test(v))
      {
        return null;
      }
    }

    const map: Record<string, number | number[]> = {};
    let matched: number = 0;
    for (const field of this.fieldSpec)
    {
      if (field === "meta")
      {
        for (let i = 0; i < parts.length; i++)
        {
          const v: string = parts[i].trim();
          if (v.length > 1 && v[0] === "{" && v[v.length - 1] === "}")
          {
            map[field] = i;
            break;
          }
        }
        continue;
      }

      const nf: string = this.normalizeKey(field);
      const matchingIndices: number[] = [];

      for (let i = 0; i < parts.length; i++)
      {
        if (this.keyMatchesField(parts[i].trim(), field))
        {
          matchingIndices.push(i);
        }
      }

      const components: string[] | undefined = this.componentMap.get(nf);

      if (components && components.length > 1 && matchingIndices.length < components.length)
      {
        const componentIndices: Map<string, number> = new Map<string, number>();

        for (let i = 0; i < parts.length; i++)
        {
          const pn: string = this.normalizeKey(parts[i]);

          if (components.includes(pn) && !componentIndices.has(pn))
          {
            componentIndices.set(pn, i);
          }
        }

        if (componentIndices.size >= 2)
        {
          matchingIndices.length = 0;
          for (const idx of componentIndices.values())
          {
            matchingIndices.push(idx);
          }
        }
      }

      if (matchingIndices.length === 1)
      {
        map[field] = matchingIndices[0];
        matched++;
      }
      else if (matchingIndices.length > 1)
      {
        map[field] = matchingIndices;
        matched++;
      }
    }

    const nonMetaFields: string[] = this.fieldSpec.filter((f) => f !== "meta");
    const need: number = Math.max(1, Math.floor(nonMetaFields.length / 2));

    if (matched < need)
    {
      return null;
    }

    this.headerParts = parts.map((p) => p.trim());
    this.headerDelimiter = found.delim;
    return map;
  }

  /**
   * Extracts fields from a delimited line using the client's explicit column map. A field
   * maps to a single 0-based column, or an array of columns whose non-empty cells are joined
   * (e.g. a multi-column address). Accepts only when a mapped strongly-typed field
   * (email/phone) is present AND validates — so this authoritative path fires on the intended
   * fixed-column rows and declines everything else (kv/JSON/binary), which then falls through
   * to the normal flow.
   *
   * @param line - The raw delimited line to map via `this.columnMap`.
   * @returns The extracted row if at least one mapped strongly-typed field validates; otherwise `null`.
   */

  private applyColumnMap(line: string): Record<string, unknown> | null
  {
    const map: ColumnMap = this.columnMap!;
    const parts: string[] | null = this.splitBestDelimited(line);

    if (!parts)
    {
      return null;
    }

    const row: Record<string, unknown> = {};
    let present: number = 0;
    let strongPresent: number = 0;
    let strongValid: number = 0;

    for (let i = 0; i < this.fieldSpec.length; i++)
    {
      const field: string = this.fieldSpec[i];
      const nf: string = this.normalizedFieldSpec[i];
      const spec: number | number[] = map[field];
      let value: unknown = null;

      if (typeof spec === "number")
      {
        value = spec < parts.length ? parts[spec] : null;
      }
      else if (Array.isArray(spec))
      {
        const cells: string[] = spec.map((i) => (i < parts.length ? String(parts[i] ?? "").trim() : "")).filter((c) => c !== "");
        value = cells.length ? cells.join(", ") : null;
      }

      if (value !== null && String(value).trim() !== "")
      {
        row[field] = value;
        present++;
        if (nf === "email" || nf === "phone")
        {
          strongPresent++;

          if (this.validateField(field, value))
          {
            strongValid++;
          }
        }
      }
      else
      {
        row[field] = null;
      }
    }

    if (present === 0 || strongPresent === 0 || strongValid === 0)
    {
      return null;
    }

    return row;
  }

  /**
   * Attempts to extract a delimited-line record, first via any established header map,
   * then falling back to content-based (headerless) extraction.
   *
   * @param line - The raw line to parse as a delimited record.
   * @returns The extracted row and whether a header map was used, or `null` if no delimited record could be extracted.
   */
  private parseDelimitedRecord(line: string): { row: Record<string, unknown>; usedHeader: boolean } | null
  {
    if (this.fieldSpec.length === 0)
    {
      return null;
    }

    const t: string = line.trim();

    if (t[0] === "{" || t[0] === "[")
    {
      return null;
    }

    const parts: string[] | null = this.headerDelimiter
      ? LineClassifierServiceImpl.parseCsvLine(line, this.headerDelimiter, LineClassifierServiceImpl.csvQuoteFor(this.headerDelimiter))
      : this.splitBestDelimited(line);
    if (!parts) return null;

    if (this.headerMap)
    {
      const viaHeader = this.applyHeaderMap(line, parts);

      if (viaHeader !== undefined)
      {
        return viaHeader;
      }
    }

    return this.parseDelimitedByContent(parts);
  }

  /**
   * Applies `this.headerMap` to an already-split line. Returns `undefined` (not `null`) to
   * mean "no header verdict — fall through to content-based extraction", reserving `null`
   * for "this line replaced the header and produced no data row".
   *
   * @param line - The raw line (used to re-check for a possible new header if the current map no longer fits).
   * @param parts - The line already split into delimited cells.
   * @returns The extracted row (with `usedHeader: true`) if the header map applies; `null` if the line replaced the header; or `undefined` to fall through to content-based extraction.
   */

  private applyHeaderMap(line: string, parts: string[]): { row: Record<string, unknown>; usedHeader: boolean } | null | undefined
  {
    const headerColCount: number = this.headerParts?.length ?? 0;
    const columnCountDiff: number = Math.abs(headerColCount - parts.length);
    let useHeaderMap: boolean = columnCountDiff <= 2;

    if (!useHeaderMap)
    {
      const strongFields: string[] = ["email", "phone", "zip", "date", "url"];
      useHeaderMap = strongFields.some((field) => {
        const spec: number | number[] | undefined = this.headerMap![field];
        const idx: number | undefined = Array.isArray(spec) ? spec[0] : spec;
        return idx !== undefined && idx < parts.length && parts[idx] && this.validateField(field, parts[idx]);
      });
    }

    if (useHeaderMap)
    {
      const row: Record<string, unknown> = {};
      let matched: number = 0;
      const mappedIndices = new Set<number>(
        Object.values(this.headerMap!).flatMap((v) => (Array.isArray(v) ? v : [v]))
      );

      for (let i = 0; i < this.fieldSpec.length; i++)
      {
        const field: string = this.fieldSpec[i];

        if (field === "meta")
        {
          continue;
        }

        const spec: number | number[] | undefined = this.headerMap![field];
        let value: string;

        if (Array.isArray(spec))
        {
          const cells: string[] = spec.map((idx) => (idx < parts.length ? String(parts[idx] ?? "").trim() : "")).filter((c) => c !== "");
          value = cells.join(", ");
        }
        else
        {
          value = spec !== undefined && spec < parts.length ? parts[spec] : "";
        }

        row[field] = value === "" || value === undefined ? null : value;

        if (row[field] !== null)
        {
          matched++;
        }
      }

      if (this.headerParts)
      {
        const metaObj: Record<string, string> = {};
        for (let j = 0; j < this.headerParts.length; j++)
        {
          if (!mappedIndices.has(j))
          {
            const v: string = j < parts.length ? String(parts[j] ?? "").trim() : "";

            if (v !== "")
            {
              metaObj[this.headerParts[j]] = v;
            }
          }
        }

        const metaColumnIndex: number | number[] | undefined = this.headerMap!["meta"];
        if (metaColumnIndex !== undefined)
        {
          const metaValue: string = Array.isArray(metaColumnIndex)
            ? metaColumnIndex.map((idx) => (idx < parts.length ? String(parts[idx] ?? "").trim() : "")).filter((c) => c !== "").join(", ")
            : (metaColumnIndex < parts.length ? String(parts[metaColumnIndex] ?? "").trim() : "");
          row["meta"] = metaValue || null;
        }
        else
        {
          row["meta"] = Object.keys(metaObj).length ? JSON.stringify(metaObj) : null;
        }

        if (row["meta"] !== null)
        {
          matched++;
        }
      }

      return matched > 0 ? { row, usedHeader: true } : null;
    }

    const newHeader: Record<string, number | number[]> | null = this.detectHeader(line);

    if (newHeader)
    {
      this.headerMap = newHeader;
      this.headerParts = parts.map((p) => p.trim());
      return null;
    }

    return undefined;
  }

  /**
   * No header available: identify columns by CONTENT for strongly-validatable fields
   * (email/phone), then best-effort group remaining text columns into weak fields
   * (name/address/location), preserving everything unclaimed in meta.
   *
   * @param parts - The line already split into delimited cells.
   * @returns The extracted row and `usedHeader: false`, or `null` if no strong field matched and there weren't enough columns/matches to accept a headerless guess.
   */

  private parseDelimitedByContent(parts: string[]): { row: Record<string, unknown>; usedHeader: boolean } | null
  {
    const row: Record<string, unknown> = {};
    let matched: number = 0;
    const claimed = new Set<number>();
    let strongMatched: number = 0;

    for (let i = 0; i < this.fieldSpec.length; i++)
    {
      const field: string = this.fieldSpec[i];
      const nf: string = this.normalizedFieldSpec[i];
      let value: unknown = null;
      if (nf === "email" || nf === "phone")
      {
        for (let j = 0; j < parts.length; j++)
        {
          if (claimed.has(j))
          {
            continue;
          }

          if (this.validateField(field, parts[j]))
          {
            value = parts[j];
            claimed.add(j);
            break;
          }
        }
      }

      row[field] = value;

      if (value !== null)
      {
        matched++;
        strongMatched++;
      }
    }

    matched += this.groupWeakFieldsByContent(parts, row, claimed);

    const metaObj: Record<string, string> = {};

    for (let j = 0; j < parts.length; j++)
    {
      if (claimed.has(j))
      {
        continue;
      }

      const v: string = String(parts[j] ?? "").trim();

      if (v !== "")
      {
        metaObj[`col_${j}`] = v;
      }
    }

    row["meta"] = Object.keys(metaObj).length ? JSON.stringify(metaObj) : null;

    if (row["meta"] !== null)
    {
      matched++;
    }

    return strongMatched > 0 || (parts.length > 4 && matched > 0) ? { row, usedHeader: false } : null;
  }

  /**
   * Best-effort assignment of non-ID-like unclaimed text columns to weak fields
   * (address/location/name) for headerless CSVs with a consistent column layout (e.g. street/
   * city/zip/country as separate columns). Groups each contiguous run of unclaimed text
   * columns into one weak field so address gets the street block and location gets the
   * city/state/zip/country block, instead of interleaving columns across fields. Mutates
   * `row` and `claimed` in place; returns how many additional fields were matched.
   *
   * @param parts - The line already split into delimited cells.
   * @param row - The row being built; mutated in place with any weak fields assigned.
   * @param claimed - The set of column indices already claimed by strong fields; mutated in place as weak fields claim columns.
   * @returns The number of additional weak fields that were assigned a value.
   */

  private groupWeakFieldsByContent(parts: string[], row: Record<string, unknown>, claimed: Set<number>): number
  {
    const weakCandidates: number[] = [];

    for (let j = 0; j < parts.length; j++)
    {
      if (claimed.has(j))
      {
        continue;
      }

      const v: string = String(parts[j] ?? "").trim();

      if (!v || LineClassifierServiceImpl.DELIMITER_ONLY_RE.test(v) || LineClassifierServiceImpl.ID_LIKE_RE.test(v) || LineClassifierServiceImpl.SALUTATION_RE.test(v))
      {
        continue;
      }

      weakCandidates.push(j);
    }

    const weakFieldIndices: number[] = [];

    for (let i = 0; i < this.fieldSpec.length; i++)
    {
      const nf: string = this.normalizedFieldSpec[i];

      if (nf === "email" || nf === "phone" || nf === "zip" || nf === "date" || nf === "url" || nf === "meta")
      {
        continue;
      }

      if (row[this.fieldSpec[i]] !== null)
      {
        continue;
      }

      weakFieldIndices.push(i);
    }

    if (weakFieldIndices.length === 0 || weakCandidates.length === 0)
    {
      return 0;
    }

    const runs: number[][] = [];
    let current: number[] = [];

    for (let k = 0; k < weakCandidates.length; k++)
    {
      const idx: number = weakCandidates[k];

      if (current.length === 0 || idx === weakCandidates[k - 1] + 1)
      {
        current.push(idx);
      }
      else
      {
        runs.push(current);
        current = [idx];
      }
    }

    if (current.length)
    {
      runs.push(current);
    }

    if (runs.length === 1 && weakFieldIndices.length > 1)
    {
      const run: number[] = runs[0];
      const zipPos: number = run.findIndex((idx) => LineClassifierServiceImpl.ZIP_RE.test(String(parts[idx] ?? "").trim()));

      if (zipPos > 0)
      {
        runs.splice(0, 1, run.slice(0, zipPos), run.slice(zipPos));
      }
    }

    let matched: number = 0;

    for (let wi = 0; wi < weakFieldIndices.length; wi++)
    {
      const field: string = this.fieldSpec[weakFieldIndices[wi]];
      const isLast: boolean = wi === weakFieldIndices.length - 1;
      const chunks: number[][] = [];

      if (wi < runs.length)
      {
        chunks.push(runs[wi]);
      }

      if (isLast)
      {
        for (let r = wi + 1; r < runs.length; r++) chunks.push(runs[r]);
      }

      if (chunks.length === 0)
      {
        continue;
      }

      const values: string[] = chunks.flat().map((idx) => String(parts[idx] ?? "").trim()).filter((v) => v !== "");

      if (values.length === 0)
      {
        continue;
      }

      for (const chunk of chunks) for (const idx of chunk) claimed.add(idx);
      row[field] = values.join(", ");
      matched++;
    }
    return matched;
  }

  /**
   * Applies a resolved locator string (`index:`, `key:`, or `regex:`) to a parsed line
   * structure to extract a single field's raw value.
   *
   * @param line - The original raw line, used as the regex target for `regex:` locators when the parsed structure is not itself a string.
   * @param parsed - The line's parsed structure (array, object, or string) that the locator addresses.
   * @param loc - The canonical `"kind:value"` locator string.
   * @returns The located value, or `undefined` if the locator kind is unrecognized, out of range, or produces no match.
   */

  private applyLocator(line: string, parsed: string | unknown[] | Record<string, unknown>, loc: string): unknown
  {
    if (typeof loc !== "string" || !loc)
    {
      return undefined;
    }

    if (loc.startsWith("index:"))
    {
      const index: number = parseInt(loc.replace("index:", ""));

      if (Array.isArray(parsed) && index < parsed.length)
      {
        return parsed[index];
      }

      return undefined;
    }

    if (loc.startsWith("key:"))
    {
      const key: string = loc.replace("key:", "");

      if (parsed && !Array.isArray(parsed) && typeof parsed === "object")
      {
        return (parsed as Record<string, unknown>)[key];
      }

      return undefined;
    }

    if (loc.startsWith("regex:"))
    {
      const regexStr: string = loc.replace("regex:", "");
      const re: RegExp | null = SafeRegexUtils.safeRegex(regexStr);

      if (!re)
      {
        return undefined;
      }

      const target: string = typeof parsed === "string" ? parsed : line;
      const match: RegExpExecArray | null = re.exec(target);

      if (match)
      {
        return match[1] ?? match[0];
      }

      return undefined;
    }

    return undefined;
  }

  /**
   * Normalizes a raw extracted row into output-ready string/number/boolean/null values.
   * Returns `null` (rejecting the whole row) if any field's text content is
   * binary-corrupted beyond the accepted threshold.
   *
   * @param row - The raw extracted field/value map to coerce.
   * @returns The coerced row with normalized values, or `null` if any field is rejected for binary corruption.
   */

  private coerce(row: Record<string, unknown>): Record<string, unknown> | null
  {
    const out: Record<string, unknown> = {};

    for (const [k, v] of Object.entries(row))
    {
      if (v === null || v === undefined || v === "")
      {
        out[k] = null;
      }
      else if (typeof v === "boolean" || typeof v === "number")
      {
        out[k] = v;
      }
      else if (Array.isArray(v))
      {
        const nf: string = this.normalizeKey(k);

        if (nf === "email" || nf === "phone" || nf === "url")
        {
          const first = v.find((x) => x !== null && x !== undefined && this.validateField(k, x));

          if (first !== undefined)
          {
            out[k] = String(first).trim();
            continue;
          }
        }
        out[k] = JSON.stringify(v);
      }
      else if (typeof v === "object")
      {
        out[k] = JSON.stringify(v);
      }
      else
      {
        const s: string = String(v).trim();
        const binaryChars: string[] = s.match(LineClassifierServiceImpl.BINARY_RE) || [];
        const binaryCount: number = binaryChars.filter((c) => c !== "\t" && c !== "\n" && c !== "\r").length;

        if (s.length > 0 && binaryCount / s.length > LineClassifierServiceImpl.BINARY_RATIO_MAX)
        {
          if (this.coerceRejectsLogged < 10)
          {
            this.logger.warn("coerce_rejected", { job_id: this.jobId, field: k, value: s.substring(0, 200), binary_count: binaryCount, length: s.length, ratio: +(binaryCount / s.length).toFixed(3) });
            this.coerceRejectsLogged++;
          }
          return null;
        }
        out[k] = s;
      }
    }

    return out;
  }

  /**
   * Splits a single delimited line into cells, honoring a quote character for embedded
   * delimiters/newlines and doubled-quote escaping.
   *
   * @param line - The raw line to split.
   * @param delim - The delimiter character to split on.
   * @param quoteChar - The quote character used to protect embedded delimiters (defaults to `"`); pass an empty string to disable quote handling.
   * @returns The line split into trimmed cell strings.
   */

  private static csvQuoteFor(delim: string): string
  {
    return delim === "\t" ? "" : "\"";
  }

  private static parseCsvLine(line: string, delim: string, quoteChar: string = "\""): string[]
  {
    const quote: string | null = quoteChar || null;
    const parts: string[] = [];
    let current: string = "";
    let inQuote: boolean = false;

    for (let i = 0; i < line.length; i++)
    {
      const c: string = line[i];
      const next: string = line[i + 1];

      if (quote && c === quote)
      {
        if (inQuote && next === quote)
        {
          current += quote;
          i++;
        }
        else
        {
          inQuote = !inQuote;
        }
      }
      else if (c === delim && !inQuote)
      {
        parts.push(current.trim());
        current = "";
      }
      else
      {
        current += c;
      }
    }

    parts.push(current.trim());

    return parts;
  }

  /**
   * Cheap shape fingerprint used to key the per-job AI template cache: JSON lines fingerprint
   * by sorted key set, delimited lines by delimiter + column count, everything else by
   * length.
   *
   * @param line - The raw line to fingerprint.
   * @returns A short string identifying the line's shape, suitable as an AI-cache key.
   */

  private static quickFingerprint(line: string): string
  {
    const trimmed: string = line.trim();

    if (trimmed.length === 0)
    {
      return "empty";
    }

    if (trimmed[0] === "{" || trimmed[0] === "[")
    {
      try
      {
        const parsed = JSON.parse(line);

        if (parsed && typeof parsed === "object" && !Array.isArray(parsed))
        {
          return `json|${Object.keys(parsed).sort().join(",")}`;
        }
      }
      catch
      {
        /* ignore */
      }
    }

    for (const delim of LineClassifierServiceImpl.DELIMITER_CANDIDATES)
    {
      const parts: string[] = LineClassifierServiceImpl.parseCsvLine(line, delim, LineClassifierServiceImpl.csvQuoteFor(delim));

      if (parts.length >= 3)
      {
        return `csv|${delim}|${parts.length}`;
      }
    }

    return `text|${trimmed.length}`;
  }
}

export function Enforce(): void {}
