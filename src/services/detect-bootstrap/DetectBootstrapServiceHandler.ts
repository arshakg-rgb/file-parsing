import crypto from "crypto";
import jschardet, {IDetectedMap} from "jschardet";
import { transition } from "@service/job-service/StateMachineImpl.js";
import { settings } from "@shared/Settings.js";
import { EventType, makeJobEvent } from "@shared/models/events.js";
import { JobStatus, ClassifyMessage, ParseMessage, ColumnMap } from "@shared/models/job.js";
import { templateRegistry } from "@shared/TemplateRegistryService.js";
import { createLogger } from "@utils/logger/Log.js";
import {
  ClassifyKind,
  ClassifyRequest, ClassifyResponse, CSV_DELIMITERS, HEADER_PATTERNS,
  HeaderStripResult,
  ProbeResult
} from "@service/detect-bootstrap/io/IDetectBootstrap";
import EncodingService from "@utils/normalizers/Encoding";
import HealthService from "@utils/response/Health";
import {MetricsUtils} from "@utils/response/Metrics";
import {aiClassifierServiceImpl} from "@service/ai-classifier/impl/AiClassifierServiceImpl";
import {DatabaseService} from "@shared/DatabaseManager";
import {GcsUtils} from "@shared/GcsUtils";
import {QueueService} from "@shared/QueueService";
import {QueueConsumerPool} from "@shared/QueueConsumerPool.js";
import {InstantiationError} from "@errors/InstantiationError";
import {RecordTemplate, RubbishTemplate} from "@shared/io/ITemplateRegistryService";
import {expandJsonColumns} from "@service/detect-bootstrap/impl/JsonColumnExpander";


export class DetectBootstrapService
{
  /**
   * Singleton instance
   * @private
   */

  private static instance: DetectBootstrapService;

  private running: boolean = false;

  /**
   *  Total Bootstraps @private
   */

  private totalBootstraps: number = 0;

  /**
   *  Total Probes @private
   */

  private totalProbes: number = 0;
  /**
   * Total Templates Created @private
   */

  private totalTemplatesCreated: number = 0;

  private stats = {
    csvDetected: 0,
    jsonDetected: 0,
    textDetected: 0,
    encodingDetections: 0,
    headerSkips: 0,
    aiTimeouts: 0,
    cacheHits: 0,
    cacheMisses: 0,
  };

  public readonly logger = createLogger(module);

  private classify: ((req: ClassifyRequest) => Promise<ClassifyResponse>) | null = null;

  private gcsUtils: GcsUtils;

  private queueService: QueueService;

  /**
   * @param enforce - A function to enforce the Singleton pattern
   * @param gcsUtils
   * @param queueService
   * Private constructor for singleton pattern
   */

  private constructor(enforce: () => void, gcsUtils: GcsUtils, queueService: QueueService)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE,"Cannot instantiate RetryServiceImpl directly. Use getInstance()");
    }

    if (process.env.HEALTH_CHECK_PORT && process.env.QUEUE_PUSH_MODE !== "true")
    {
      HealthService.startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
    }

    this.gcsUtils = gcsUtils;
    this.queueService = queueService;
    this.initializeClassifier();
  }

  /**
   * Get singleton instance
   */

  static getInstance(): DetectBootstrapService
  {
    if (!DetectBootstrapService.instance)
    {
      DetectBootstrapService.instance = new DetectBootstrapService(Enforce, GcsUtils.getInstance(), QueueService.getInstance());
    }

    return DetectBootstrapService.instance;
  }

  /**
   * Initialize the classifier based on configuration
   */
  private async initializeClassifier(): Promise<void>
  {
    if (this.classify)
    {
      return;
    }

    if (settings.BEDROCK_MODEL_ID === "mock")
    {
      const { mockClassify } = await import("@service/ai-classifier/mock.js");
      this.classify = async (req: ClassifyRequest) => {
        const resp = await mockClassify(req);
        return resp.template ? { kind: resp.kind, template: resp.template } : { kind: "uncertain" };
      };
    }
    else
    {
      this.classify = async (req: ClassifyRequest) => {
        const aiReq = {
          ...req,
          context_lines: req.context_lines || [],
        };

        const aiResp: ClassifyResponse = await aiClassifierServiceImpl.classifyAi(aiReq);

        return aiResp.template
            ? { kind: aiResp.kind as ClassifyKind, template: aiResp.template }
            : { kind: "uncertain" };
      };
    }
  }

  /**
   * Initialize the service
   */

  async initialize(): Promise<void>
  {
    await DatabaseService.getInstance().waitForDb();
    await templateRegistry.loadFromDatabase();
    await this.initializeClassifier();
    this.logger.info("detect_bootstrap_initialized");
  }

  /**
   * Start the consumer loop
   */

  async start(): Promise<void>
  {
    this.logger.info("detect_bootstrap_build_marker", { marker: "v2-json-mode" });

    if (this.running)
    {
      this.logger.warn("detect_bootstrap_already_running");
      return;
    }

    this.running = true;
    await this.initialize();
    this.logger.info("detect_bootstrap_started");

    await this.consumerLoop();
  }

  /**
   * Emit a job event to the event system
   *
   * @param jobId - Job identifier
   * @param eventType - Type of event to emit
   * @param data - Event payload data
   */

  private async emit(jobId: string, eventType: EventType, data: Record<string, unknown>): Promise<void>
  {
    await this.queueService.publishEvent(makeJobEvent(eventType, jobId, "detect-bootstrap", data));
  }

  /**
   * Compute the optimal window size for probing
   *
   * @param avgRowBytes - Average row size in bytes
   * @param maxRowBytes - Maximum row size in bytes
   * @returns Optimal window size in bytes
   */

  private computeWindowSize(avgRowBytes: number, maxRowBytes: number): number
  {
    return Math.min(settings.PROBE_WINDOW_MAX_BYTES, Math.max(settings.PROBE_WINDOW_MIN_BYTES, settings.PROBE_TARGET_LINES * avgRowBytes, 4 * maxRowBytes));
  }

  /**
   * Compute probe offsets for adaptive file structure detection
   *
   * @param fileSize - Total file size in bytes
   * @param windowSize - Size of each probe window
   * @returns Array of byte offsets to probe
   */

  private computeProbeOffsets(fileSize: number, windowSize: number): number[]
  {
    const count: number = Math.max(settings.PROBE_COUNT_MIN, Math.min(settings.PROBE_COUNT_MAX, Math.floor(fileSize / settings.PROBE_SIZE_PER_COUNT)));

    if (fileSize <= windowSize)
    {
      return [0];
    }

    const offsets: number[] = Array.from({ length: count }, (_, i) => Math.floor(i * ((fileSize - windowSize) / (count - 1))));
    offsets[0] = 0;
    offsets[offsets.length - 1] = Math.max(0, fileSize - windowSize);
    return [...new Set(offsets)].sort((a, b) => a - b);
  }

  /**
   * Detect file encoding from raw bytes
   *
   * Prefers UTF-8 when bytes validate as UTF-8 to avoid jschardet misdetection.
   *
   * @param raw - Raw file bytes
   * @returns Detected encoding label
   */

  private detectEncoding(raw: Buffer): string
  {
    this.stats.encodingDetections++;

    // Check for un-BOM'd UTF-16 BEFORE trusting isLikelyUtf8: a NUL byte is valid
    // single-byte UTF-8, so ASCII-content-as-UTF-16 buffers otherwise "pass" the UTF-8
    // check and get decoded as garbled NUL-interleaved single characters.
    const utf16: "utf-16le" | "utf-16be" | null = EncodingService.looksLikeUtf16(raw.subarray(0, 65536));

    if (utf16)
    {
      return utf16;
    }

    if (EncodingService.isLikelyUtf8(raw.subarray(0, 65536)))
    {
      return "utf-8";
    }

    const result: IDetectedMap = jschardet.detect(raw.slice(0, 65536));
    return EncodingService.normalizeEncoding(result.encoding);
  }

  /**
   * Measure row width statistics from raw bytes
   *
   * @param raw - Raw file bytes
   * @param encoding - File encoding
   * @returns Tuple of [average row bytes, maximum row bytes]
   */
  private measureRowWidth(raw: Buffer, encoding: string): [number, number]
  {
    const text: string = EncodingService.decode(raw, encoding);
    const lines: string[] = text.split(/\r?\n/).filter((l) => l.trim());

    if (!lines.length)
    {
      return [256, 512];
    }

    const sizes: number[] = lines.map((l) => Buffer.byteLength(l, EncodingService.bufferEncodingFor(encoding)));
    const avg: number = sizes.reduce((a, b) => a + b, 0) / sizes.length;

    return [avg, Math.max(...sizes)];
  }

  /**
   * Generate a fingerprint for a probe window
   *
   * @param raw - Raw probe bytes
   * @param encoding - File encoding
   * @returns SHA256 hash truncated to 24 characters
   */

  private fingerprintProbe(raw: Buffer, encoding: string): string
  {
    const text: string = EncodingService.decode(raw, encoding);
    const lines: string[] = text.split(/\r?\n/).filter((l) => l.trim());

    if (!lines.length)
    {
      return this.hash("empty");
    }

    const first: string = lines[0];
    for (const delim of CSV_DELIMITERS)
    {
      const parts: string[] = first.split(delim);

      if (parts.length > 1)
      {
        this.stats.csvDetected++;
        return this.hash(`csv|${delim}|${parts.length}|${encoding}`);
      }
    }

    try {
      const parsed = JSON.parse(first);

      if (typeof parsed === "object" && parsed !== null)
      {
        this.stats.jsonDetected++;
        const keys: string = Object.keys(parsed).sort().join(",");

        return this.hash(`json|${keys}`);
      }
    } catch {
      // not JSON, fall through to plain text fingerprint
    }

    this.stats.textDetected++;

    return this.hash(`text|${first.length}|${encoding}`);
  }

  /**
   * SHA256 hash a fingerprint seed string, truncated to 24 characters
   */

  private hash(seed: string): string
  {
    return crypto.createHash("sha256").update(seed).digest("hex").slice(0, 24);
  }

  /**
   * Extract sample lines from raw bytes
   *
   * @param raw - Raw file bytes
   * @param encoding - File encoding
   * @param n - Maximum number of lines to extract
   * @returns Array of non-empty lines
   */

  private extractSampleLines(raw: Buffer, encoding: string, n: number): string[]
  {
    const text: string = EncodingService.decode(raw, encoding);
    return text.split(/\r?\n/).filter((l) => l.trim()).slice(0, n);
  }

  /**
   * Detect and strip a delimited header line from sample lines, so classification
   * runs against actual data rather than column names.
   *
   * @param sampleLines - Lines extracted from a probe window
   * @param jobId - Job identifier, for logging
   * @returns The data lines to classify, and whether a header was found
   */

  private stripHeaderLine(sampleLines: string[], jobId: string): HeaderStripResult
  {
    const firstLine: string = sampleLines[0];
    const hadHeader: boolean = HEADER_PATTERNS.some((pattern) => pattern.test(firstLine));

    this.logger.info("detect_header_check", {
      job_id: jobId,
      firstLine,
      hasHeader: hadHeader,
      sampleLinesCount: sampleLines.length,
    });

    if (!hadHeader || sampleLines.length <= 1)
    {
      return { dataLines: sampleLines, hadHeader: false, headerLine: undefined };
    }

    this.stats.headerSkips++;
    const dataLines: string[] = sampleLines.slice(1);
    this.logger.info("detect_header_skipped", { job_id: jobId, dataLinesCount: dataLines.length });
    return { dataLines, hadHeader: true, headerLine: firstLine };
  }

  /**
   * Normalize a message's field_spec into a string array, tolerating a
   * JSON-encoded string form of the field spec.
   *
   * @param fieldSpec - Raw field_spec from the incoming message
   * @returns Parsed field spec array (empty array if parsing fails)
   */

  private parseFieldSpec(fieldSpec: string | string[]): string[]
  {
    if (typeof fieldSpec !== "string")
    {
      return fieldSpec;
    }

    try
    {
      return JSON.parse(fieldSpec);
    }
    catch
    {
      return [];
    }
  }

  /**
   * Classify an unknown line via the AI classifier, racing against a timeout.
   * Retries up to 3 times on timeout/failure; if all attempts fail the job is
   * aborted by throwing an error that the consumer loop converts to a FAILED status.
   *
   * @param req - Classification request
   * @param jobId - Job identifier, for logging
   * @param fingerprint - Fingerprint of the probe window, for logging
   * @returns Classification response
   * @throws Error when the AI call fails after 3 attempts
   */

  private async classifyWithTimeout(req: ClassifyRequest, jobId: string, fingerprint: string): Promise<ClassifyResponse>
  {
    const MAX_ATTEMPTS = 3;
    let lastErr: Error | unknown;

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++)
    {
      this.logger.info("ai_call_initiated", {
        job_id: jobId,
        source: "detect-bootstrap",
        attempt,
        max_attempts: MAX_ATTEMPTS,
        unknown_line_length: req.unknown_line.length,
        context_lines: (req.context_lines || []).length,
        fingerprint,
      });

      try
      {
        const aiTimeout = new Promise<never>((_, reject) => setTimeout(() => reject(new Error("ai_classify_timeout")), settings.AI_CLASSIFY_TIMEOUT_MS));
        const resp: ClassifyResponse = await Promise.race([this.classify!(req), aiTimeout]);

        this.logger.info("ai_call_completed", {
          job_id: jobId,
          source: "detect-bootstrap",
          attempt,
          verdict: resp.kind,
          has_template: !!resp.template,
          fingerprint,
        });

        return resp;
      }
      catch (aiErr)
      {
        lastErr = aiErr;
        this.stats.aiTimeouts++;

        this.logger.warn("ai_call_timeout", {
          job_id: jobId,
          source: "detect-bootstrap",
          attempt,
          max_attempts: MAX_ATTEMPTS,
          fingerprint,
          error: String(aiErr),
        });
        MetricsUtils.increment("detect.ai_timeout", 1);
      }
    }

    this.logger.error("ai_call_failed", {
      job_id: jobId,
      source: "detect-bootstrap",
      fingerprint,
      attempts: MAX_ATTEMPTS,
      error: String(lastErr),
    });

    throw new Error(`AI call failed after ${MAX_ATTEMPTS} attempts: ${String(lastErr)}`);
  }

  /**
   * Process a single probe window: read it, fingerprint it, resolve against an
   * existing template if one already matches, or classify it to seed a new one.
   * Returns null if this fingerprint was already seen in an earlier window
   * (a pure cache hit — no template resolution needed).
   *
   * @param bucket - GCS bucket
   * @param key - GCS object key
   * @param offset - Byte offset of the probe window
   * @param windowSize - Size of the probe window
   * @param fileSize - Total file size, to clamp the window's end
   * @param encoding - File encoding
   * @param fieldSpecArray - Parsed field spec for the job
   * @param jobId - Job identifier
   * @param seen - Fingerprints already seen in this job's probes
   * @returns The probe's fingerprint and resolved template id (if any)
   */

  private async processProbeWindow(bucket: string, key: string, offset: number, windowSize: number, fileSize: number, encoding: string, fieldSpecArray: string[], jobId: string, seen: Set<string>): Promise<ProbeResult | null>
  {
    const end: number = Math.min(offset + windowSize - 1, fileSize - 1);
    const probeRaw: Buffer = await this.gcsUtils.readRange(bucket, key, offset, end);
    const fingerprint: string = this.fingerprintProbe(probeRaw, encoding);

    if (seen.has(fingerprint))
    {
      this.stats.cacheHits++;
      return null;
    }

    seen.add(fingerprint);
    this.stats.cacheMisses++;

    const existing: RecordTemplate | RubbishTemplate = templateRegistry.getByFingerprint(fingerprint);

    if (existing)
    {
      return { fingerprint, templateId: existing.template_id };
    }

    const sampleLines: string[] = this.extractSampleLines(probeRaw, encoding, 10);

    if (!sampleLines.length)
    {
      return {fingerprint, templateId: null};
    }

    const { dataLines } = this.stripHeaderLine(sampleLines, jobId);

    if (!dataLines.length)
    {
      return {fingerprint, templateId: null};
    }

    const req: ClassifyRequest = {
      unknown_line: dataLines[0],
      field_spec: fieldSpecArray,
      context_lines: dataLines.slice(1),
      job_id: jobId,
    };

    const resp: ClassifyResponse = await this.classifyWithTimeout(req, jobId, fingerprint);
    if (!resp) return { fingerprint, templateId: null };

    if (resp.template)
    {
      this.totalTemplatesCreated++;

      this.logger.info("ai_template_saved", {
        job_id: jobId,
        source: "detect-bootstrap",
        kind: resp.kind,
        template_id: resp.template.template_id,
        fingerprint,
        saved_to_registry: true,
      });

      this.logger.info("seed_template_created", {
        job_id: jobId,
        kind: resp.kind,
        template_id: resp.template.template_id,
        fingerprint,
      });

      MetricsUtils.increment("detect.template_created", 1, { kind: resp.kind });

      return { fingerprint, templateId: resp.template.template_id };
    }

    this.logger.info("ai_no_template_returned", {
      job_id: jobId,
      source: "detect-bootstrap",
      verdict: resp.kind,
      fingerprint,
    });

    return { fingerprint, templateId: null };
  }

  /**
   * Extract the column names declared inside a MySQL CREATE TABLE block.
   *
   * @param sampleLines - Non-empty lines from the head of the file
   * @returns Array of column names, or empty array if no CREATE TABLE block is found
   */
  private extractMySqlCreateTableColumns(sampleLines: string[]): string[]
  {
    let inCreateTable = false;
    const columns: string[] = [];

    for (const rawLine of sampleLines)
    {
      const line = rawLine.trim();

      if (!inCreateTable)
      {
        if (/^CREATE TABLE\s+/i.test(line))
        {
          inCreateTable = true;
        }
        continue;
      }

      // Closing paren of the CREATE TABLE column list
      if (/^\)\s*[;,]?/.test(line))
      {
        break;
      }

      // Column definitions start with a backtick-quoted identifier followed by a type
      const match = line.match(/^`([^`]+)`\s+\w+/);
      if (match)
      {
        columns.push(match[1]);
      }
    }

    return columns;
  }

  /**
   * Extract the candidate headers/keys from the first meaningful bytes of the file.
   * Works for CSV/TSV header lines, JSON/JSONL object keys, and MySQL CREATE TABLE
   * dumps.
   *
   * @param bucket - GCS bucket
   * @param key - GCS object key
   * @param encoding - Detected file encoding
   * @param fileSize - Total file size in bytes
   * @param jobId - Job identifier for logging
   * @returns Array of detected headers, or null if extraction failed
   */

  private async extractHeaders(bucket: string, key: string, encoding: string, fileSize: number, jobId: string, fieldSpec: string[], extraOffsets: number[] = [], extraWindowSize: number = 0): Promise<{ headers: string[]; fieldMap?: ColumnMap } | null>
  {
    try
    {
      const headEnd: number = Math.min(settings.PROBE_WINDOW_MAX_BYTES - 1, fileSize - 1);
      if (headEnd < 0)
      {
        return { headers: [] };
      }

      const headRaw: Buffer = await this.gcsUtils.readRange(bucket, key, 0, headEnd);
      const headText: string = EncodingService.decode(headRaw, encoding).replace(/\0/g, "");

      const copyMatch = headText.match(/COPY\s+\S+\s*\(([^)]+)\)\s*FROM\s+stdin;?/im);
      if (copyMatch)
      {
        return { headers: copyMatch[1].split(",").map((h) => h.trim().replace(/^["`]+|["`]+$/g, "")).filter((h) => h.length > 0) };
      }

      const sampleLines: string[] = this.extractSampleLines(headRaw, encoding, 500);

      if (!sampleLines.length)
      {
        return { headers: [] };
      }

      const { hadHeader, headerLine, dataLines } = this.stripHeaderLine(sampleLines, jobId);
      const candidate: string | undefined = hadHeader ? headerLine : sampleLines[0];

      if (!candidate)
      {
        return { headers: [] };
      }

      const trimmed: string = candidate.trim();
      const headUpper: string = headText.toUpperCase();
      const isSqlDump: boolean = headUpper.includes("MYSQL DUMP") || headUpper.startsWith("CREATE TABLE") || headUpper.includes("INSERT INTO") || headUpper.includes("POSTGRESQL") || headUpper.includes("PG_DUMP");

      // No real header row, and not JSON/SQL (those have their own dedicated parsers and
      // already work correctly): the naive comma/tab split below would just re-label the
      // first raw DATA row as "headers", which is meaningless for any other delimiter
      // (e.g. "|"). Ask the AI to look at a few raw sample lines and propose a real
      // semantic label per column instead, restricted to plain headerless delimited
      // files (e.g. "url|user|password" credential logs) only.
      if (!hadHeader && trimmed[0] !== "{" && trimmed[0] !== "[" && !copyMatch && !isSqlDump)
      {
        const aiMode: string = settings.AI_INLINE_MODE;
        const aiEnabled: boolean = aiMode === "mock" || aiMode === "live";

        if (aiEnabled)
        {
          const probeLines: string[] = (dataLines.length > 0 ? dataLines : sampleLines).slice(0, 8);

          try
          {
            const inferred = await aiClassifierServiceImpl.inferHeadersFromSample(probeLines, fieldSpec, jobId);

            if (inferred && inferred.headers.length > 0)
            {
              this.logger.info("ai_headers_inferred", { job_id: jobId, headers: inferred.headers, field_map: inferred.fieldMap });
              return { headers: inferred.headers, fieldMap: inferred.fieldMap };
            }
          }
          catch (err)
          {
            this.logger.warn("ai_headers_inference_failed", { job_id: jobId, error: String(err) });
          }
        }

        // No real header row exists and AI inference either did not run or did not
        // produce a usable result. Do NOT fall through to the naive comma/tab split
        // below - that would just re-label this raw DATA row (e.g. an
        // "android://...|1625780328|jaaaaaaaaaaa" credential line) as "headers".
        // Returning null leaves the job header-less so the stream-parser's own
        // content-based / AI header fallback can take over at parse time instead.
        this.logger.info("headerless_delimited_no_ai_headers", { job_id: jobId });
        return null;
      }

      // PostgreSQL COPY header: capture the column list inside the parentheses
      for (const line of sampleLines)
      {
        const lineTrim = line.trim();
        const copyMatch = lineTrim.match(/COPY\s+\S+\s*\(([^)]+)\)\s*FROM\s+stdin;?/i);
        if (copyMatch)
        {
          return { headers: copyMatch[1].split(",").map((h) => h.trim().replace(/^["`]+|["`]+$/g, "")).filter((h) => h.length > 0) };
        }
      }

      if (trimmed.startsWith("{") || trimmed.startsWith("["))
      {
        try
        {
          const parsed: unknown = JSON.parse(trimmed);

          if (parsed && typeof parsed === "object" && !Array.isArray(parsed))
          {
            const keys = new Set<string>();
            const collectKeysFromLines = (lines: string[]): void =>
            {
              for (const line of lines)
              {
                const lineTrim = line.trim();
                if (!lineTrim)
                {
                  continue;
                }
                try
                {
                  const p: unknown = JSON.parse(lineTrim);
                  if (p && typeof p === "object" && !Array.isArray(p))
                  {
                    for (const k of Object.keys(p as Record<string, unknown>))
                    {
                      keys.add(k);
                    }
                  }
                }
                catch
                {
                  // ignore malformed sample
                }
              }
            };

            // JSONL files can have optional/inconsistent keys per record (e.g. an
            // "Expertise" array only present on a handful of records out of thousands).
            // A head-only, line-capped, or sparsely-sampled scan can easily miss those,
            // silently dropping the field into `meta` for every row instead of giving it
            // its own column. For files small enough to read in full cheaply, scan every
            // line so no key is ever missed regardless of where it first appears.
            const FULL_SCAN_MAX_BYTES = 20 * 1024 * 1024;

            if (fileSize <= FULL_SCAN_MAX_BYTES)
            {
              collectKeysFromLines(this.extractSampleLines(headRaw, encoding, Number.MAX_SAFE_INTEGER));

              if (fileSize > headEnd + 1)
              {
                try
                {
                  const restRaw: Buffer = await this.gcsUtils.readRange(bucket, key, headEnd + 1, fileSize - 1);
                  collectKeysFromLines(this.extractSampleLines(restRaw, encoding, Number.MAX_SAFE_INTEGER));
                }
                catch (err)
                {
                  this.logger.warn("extract_headers_full_scan_failed", { job_id: jobId, error: String(err) });
                }
              }
            }
            else
            {
              collectKeysFromLines(sampleLines);

              // Too large to scan in full: sample additional windows spread across the
              // rest of the file so keys appearing later still have a chance of being
              // discovered (best-effort - not a full-coverage guarantee for huge files).
              if (extraOffsets.length > 0 && extraWindowSize > 0)
              {
                for (const offset of extraOffsets)
                {
                  if (offset <= headEnd)
                  {
                    continue;
                  }

                  try
                  {
                    const end: number = Math.min(offset + extraWindowSize - 1, fileSize - 1);
                    const extraRaw: Buffer = await this.gcsUtils.readRange(bucket, key, offset, end);
                    const extraLines: string[] = this.extractSampleLines(extraRaw, encoding, 200);
                    collectKeysFromLines(extraLines.slice(1));
                  }
                  catch (err)
                  {
                    this.logger.warn("extract_headers_extra_probe_failed", { job_id: jobId, offset, error: String(err) });
                  }
                }
              }
            }

            if (keys.size)
            {
              return { headers: Array.from(keys) };
            }
          }

          if (Array.isArray(parsed) && parsed.length > 0 && parsed[0] && typeof parsed[0] === "object")
          {
            const keys = new Set<string>();
            for (const item of parsed as Record<string, unknown>[])
            {
              if (item && typeof item === "object")
              {
                for (const k of Object.keys(item))
                {
                  keys.add(k);
                }
              }
            }
            if (keys.size)
            {
              return { headers: Array.from(keys) };
            }
          }
        }
        catch
        {
          // not a JSON payload, fall through to delimiter split
        }
      }

      if (headUpper.includes("CREATE TABLE"))
      {
        const createTableColumns: string[] = this.extractMySqlCreateTableColumns(sampleLines);
        if (createTableColumns.length > 0)
        {
          return { headers: createTableColumns };
        }
      }

      // Pick whichever known delimiter actually splits this header line into the most
      // fields (previously only tab vs comma were considered, so pipe/semicolon-
      // delimited headers with no commas or tabs collapsed into a single field).
      const delimiter: string = CSV_DELIMITERS.reduce((best, d) => (trimmed.split(d).length > trimmed.split(best).length ? d : best), CSV_DELIMITERS[0]);
      const rawHeaders: string[] = trimmed.split(delimiter).map((h) => h.trim().replace(/^["']|["']$/g, ""));

      return { headers: expandJsonColumns(rawHeaders, delimiter, dataLines) };
    }
    catch (err)
    {
      this.logger.warn("extract_headers_failed", { job_id: jobId, error: String(err) });
      throw err;
    }
  }

  /**
   * Detect encoding and probe window sizing from the head of the file.
   *
   * @param bucket - GCS bucket
   * @param key - GCS object key
   * @param fileSize - Total file size in bytes
   * @returns Detected encoding and computed probe window size
   */

  private async detectFileProperties(bucket: string, key: string, fileSize: number): Promise<{ encoding: string; windowSize: number }>
  {
    const headEnd: number = Math.min(settings.PROBE_WINDOW_MIN_BYTES - 1, fileSize - 1);
    const headRaw: Buffer = await this.gcsUtils.readRange(bucket, key, 0, headEnd);
    const encoding: string = this.detectEncoding(headRaw);
    const [avgRow, maxRow] = this.measureRowWidth(headRaw, encoding);
    const windowSize: number = this.computeWindowSize(avgRow, maxRow);

    return { encoding, windowSize };
  }

  /**
   * Forward the job to the parse queue with resolved seed template ids.
   *
   * @param msg - Original classify message
   * @param jobId - Job identifier
   * @param fileSize - Resolved file size
   * @param seedTemplateIds - Template ids seeded/resolved during probing
   */
  private async forwardToParse(msg: ClassifyMessage, jobId: string, fileSize: number, seedTemplateIds: string[], headers?: string[], inferredFieldMap?: ColumnMap): Promise<void>
  {
    const parseMsg: ParseMessage = {
      job_id: jobId,
      s3_url: msg.s3_url,
      size: fileSize,
      field_spec: msg.field_spec,
      column_map: msg.column_map ?? inferredFieldMap,
      headers,
      seed_template_ids: seedTemplateIds,
    };

    this.logger.info("detect_sending_to_parse", { job_id: jobId, queue_url: settings.PARSE_QUEUE_URL });

    try
    {
      await this.queueService.sendRaw(settings.PARSE_QUEUE_URL, parseMsg as unknown as Record<string, unknown>);
      this.logger.info("detect_parse_message_sent", { job_id: jobId });
    }
    catch (sendErr)
    {
      this.logger.error(
          "detect_send_to_parse_failed",
          { job_id: jobId, queue_url: settings.PARSE_QUEUE_URL },
          sendErr instanceof Error ? sendErr : new Error(String(sendErr))
      );

      throw sendErr;
    }
  }

  /**
   * Main bootstrap job handler - detects file properties and seeds templates
   *
   * This function performs adaptive probing to:
   * 1. Detect file encoding
   * 2. Measure row characteristics
   * 3. Generate fingerprints for probe windows
   * 4. Check for existing templates by fingerprint
   * 5. Classify unknown lines to create new seed templates
   * 6. Forward to parse service with seed template IDs
   *
   * @param msg - Classify message containing job details
   * @throws Error if bootstrapping fails
   */

  async bootstrapJob(msg: ClassifyMessage): Promise<void>
  {
    const bootstrapStartTime: number = Date.now();
    this.totalBootstraps++;

    await templateRegistry.loadFromDatabase();

    const jobId: string = msg.job_id;
    await transition(jobId, JobStatus.DETECTING);

    this.logger.info("detect_start", { jobId, s3_url: msg.s3_url, size: msg.size });

    const [bucket, key] = this.gcsUtils.parseGcsUrl(msg.s3_url);
    const fileSize: number = msg.size || (await this.gcsUtils.objectSize(bucket, key));

    if (!fileSize)
    {
      throw new Error(`Source file has zero size: ${msg.s3_url}`);
    }

    const { encoding, windowSize } = await this.detectFileProperties(bucket, key, fileSize);

    const offsets: number[] = this.computeProbeOffsets(fileSize, windowSize);
    this.totalProbes += offsets.length;
    this.logger.info("probing", { job_id: jobId, probe_count: offsets.length, file_size: fileSize });
    MetricsUtils.increment("detect.probe_start", 1, { probe_count: String(offsets.length) });

    const fieldSpecArray: string[] = this.parseFieldSpec(msg.field_spec);

    const seen = new Set<string>();
    const seedTemplateIds: string[] = [];

    for (const offset of offsets)
    {
      const result: ProbeResult = await this.processProbeWindow(
          bucket,
          key,
          offset,
          windowSize,
          fileSize,
          encoding,
          fieldSpecArray,
          jobId,
          seen
      );

      if (result?.templateId)
      {
        seedTemplateIds.push(result.templateId);
      }
    }

    const headerResult: { headers: string[]; fieldMap?: ColumnMap } | null = await this.extractHeaders(bucket, key, encoding, fileSize, jobId, fieldSpecArray, offsets, windowSize);
    const headers: string[] | null = headerResult?.headers ?? null;
    const inferredFieldMap: ColumnMap | undefined = headerResult?.fieldMap && Object.keys(headerResult.fieldMap).length > 0 ? headerResult.fieldMap : undefined;

    if (headers)
    {
      const updates: { headers: string[]; column_map?: ColumnMap } = { headers };

      if (inferredFieldMap && !msg.column_map)
      {
        updates.column_map = inferredFieldMap;
      }

      await DatabaseService.getInstance().repositories.jobs.updateFields(jobId, updates);
    }

    if (fieldSpecArray.length === 0)
    {
      if (!headers || headers.length === 0)
      {
        throw new Error("No headers could be detected; the job cannot proceed without a field_spec");
      }

      this.logger.info("detect_headers_awaiting_field_spec", { job_id: jobId, headers });
      return;
    }

    const bootstrapDuration: number = Date.now() - bootstrapStartTime;

    this.logger.info("detect_complete", {
      job_id: jobId,
      seeds: seedTemplateIds.length,
      probes: offsets.length,
      duration_ms: bootstrapDuration,
    });

    MetricsUtils.increment("detect.complete", 1, { seeds: String(seedTemplateIds.length) });
    MetricsUtils.set("detect.duration_ms", bootstrapDuration);

    await transition(jobId, JobStatus.PARSING);
    await this.forwardToParse(msg, jobId, fileSize, seedTemplateIds, headers, inferredFieldMap);
  }

  /**
   * Main consumer loop for processing classify messages
   *
   * Continuously polls the classify queue for messages and processes them.
   * Handles graceful shutdown and message acknowledgment.
   *
   * @throws Error if database connection fails
   */

  private async consumerLoop(): Promise<void>
  {
    await DatabaseService.getInstance().waitForDb();
    await templateRegistry.loadFromDatabase();
    this.logger.info("detect_bootstrap_consumer_started");

    const pool = new QueueConsumerPool<ClassifyMessage>(this.queueService, this.logger, {
      queueUrl: settings.CLASSIFY_QUEUE_URL,
      parser: (body) => JSON.parse(body) as ClassifyMessage,
      concurrency: settings.QUEUE_CONCURRENCY,
      memorySoftLimit: settings.QUEUE_MEMORY_SOFT_LIMIT_MB * 1024 * 1024,
      isRunning: () => this.running,
    });

    await pool.run(async (payload, receiptHandle) => {
      try
      {
        await this.bootstrapJob(payload);
        await this.queueService.deleteMessage(settings.CLASSIFY_QUEUE_URL, receiptHandle);
      }
      catch (exc)
      {
        const errMsg: string = String(exc);

        try
        {
          // Fail the job immediately instead of relying on the job-events bus,
          // so the UI never gets stuck at Detecting.
          await transition(payload.job_id, JobStatus.FAILED, errMsg);
        }
        catch (transitionErr)
        {
          this.logger.warn("detect_failed_transition_error", { job_id: payload.job_id, error: String(transitionErr) });
        }

        this.logger.error("detect_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
        MetricsUtils.increment("detect.error", 1);
        await this.emit(payload.job_id, EventType.ERROR_OCCURRED, { error: errMsg });
        await this.queueService.deleteMessage(settings.CLASSIFY_QUEUE_URL, receiptHandle);
      }
    });

    this.logger.info("detect_bootstrap_consumer_stopped");
  }
}

DetectBootstrapService.getInstance()
    .start()
    .catch((err) => {
      DetectBootstrapService.getInstance().logger.error({ error: String(err) }, "detect_bootstrap_start_failed");
      process.exit(1);
    });

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void
{
}
