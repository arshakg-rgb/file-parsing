import crypto from "crypto";
import jschardet from "jschardet";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import FirestoreCacheUtils from "@utils/cache/FirestoreCacheUtils.js";
import { EventType, makeJobEvent } from "@shared/models/events.js";
import { JobStatus, ClassifyMessage, ParseMessage } from "@shared/models/job.js";
import { sendRaw, publishEvent } from "@shared/QueueService.js";
import { decode, normalizeEncoding, bufferEncodingFor, isLikelyUtf8 } from "@utils/normalizers/encoding.js";
import { templateRegistry } from "@shared/TemplateRegistryService.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
import { metrics } from "@utils/response/metrics.js";
import { AiClassifierService } from "@service/ai_classifier/AiClassifierServiceHandler.js";
import { mockClassify } from "@service/ai_classifier/mock.js";
import { DetectBootstrapService } from "@service/detect_bootstrap/DetectBootstrapService.js";
import {
  ClassifyRequest,
  ClassifyResponse,
  CSV_DELIMITERS,
  HEADER_PATTERNS, HeaderStripResult, ProbeResult
} from "@service/detect_bootstrap/io/IDetectBootstrap.js";

/**
 * DetectBootstrapServiceImpl is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class DetectBootstrapServiceImpl extends ServiceManager implements DetectBootstrapService
{
  /**
   * Singleton instance
   * @private
   */

  protected static instance: DetectBootstrapServiceImpl;

  /**
   * Logger instance
   * @private
   */

  private logger: Logger;

  /**
   * Gcs Utils
   * @private
   */

  private gcsUtils: FirestoreCacheUtils;

  /**
   * Classify
   * @private
   */

  private classify: (req: ClassifyRequest) => Promise<ClassifyResponse>;

  /**
   * Constructs a new DetectBootstrapServiceImpl instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */

  protected constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Cannot instantiate DetectBootstrapServiceImpl directly. Use getInstance()");
    }
    super(enforce);

    this.logger = createLogger("detect_bootstrap");
    this.gcsUtils = FirestoreCacheUtils.getInstance();
    this.classify = this.buildClassifier();
  }

  /**
   * Gets the single instance of the DetectBootstrapServiceImpl class.
   * @returns The single instance of the class
   */

  public static getInstance(): DetectBootstrapServiceImpl
  {
    if (!DetectBootstrapServiceImpl.instance) {
      DetectBootstrapServiceImpl.instance = new DetectBootstrapServiceImpl(Enforce);
    }
    return DetectBootstrapServiceImpl.instance;
  }

  /**
   * Builds the classify function according to configuration - either the
   * mock classifier (for local/test use) or the real AI classifier service.
   * @returns A classify function matching the ClassifyRequest/ClassifyResponse contract
   */
  private buildClassifier(): (req: ClassifyRequest) => Promise<ClassifyResponse> {
    const config = this.getConfig();

    if (config.settings.BEDROCK_MODEL_ID === "mock") {
      return async (req: ClassifyRequest): Promise<ClassifyResponse> => {
        const resp = await mockClassify(req);
        return resp.template ? { kind: resp.kind, template: resp.template } : { kind: "uncertain" };
      };
    }

    const aiService = AiClassifierService.getInstance();
    return async (req: ClassifyRequest): Promise<ClassifyResponse> => {
      const aiReq = {
        ...req,
        context_lines: req.context_lines || [],
      };
      return (await aiService.classifyAi(aiReq)) as ClassifyResponse;
    };
  }

  /**
   * Gets logger
   * @returns The logger result
   */
  public getLogger(): Logger {
    return this.logger;
  }

  /**
   * Gets gcs utils
   * @returns The firestore cache utils result
   */
  public getGcsUtils(): FirestoreCacheUtils {
    return this.gcsUtils;
  }

  /**
   * Detects bootstrap
   * @param req - The HTTP request object
   * @returns A promise that resolves to the result
   */
  public async detectBootstrap(req: ClassifyRequest): Promise<ClassifyResponse> {
    return this.classify(req);
  }

  /**
   * Classifies line
   * @param req - The HTTP request object
   * @returns A promise that resolves to the result
   */
  public async classifyLine(req: ClassifyRequest): Promise<ClassifyResponse> {
    return this.classify(req);
  }

  /**
   * Computes window size
   * @param avgRowBytes - The avg row bytes
   * @param maxRowBytes - The max row bytes
   * @returns The numeric result
   */
  public computeWindowSize(avgRowBytes: number, maxRowBytes: number): number {
    const config = this.getConfig();
    return Math.min(
        config.settings.PROBE_WINDOW_MAX_BYTES,
        Math.max(config.settings.PROBE_WINDOW_MIN_BYTES, config.settings.PROBE_TARGET_LINES * avgRowBytes, 4 * maxRowBytes)
    );
  }

  /**
   * Computes probe offsets
   * @param fileSize - The file size
   * @param windowSize - The window size
   * @returns The list of results
   */
  public computeProbeOffsets(fileSize: number, windowSize: number): number[] {
    const config = this.getConfig();
    const count = Math.max(
        config.settings.PROBE_COUNT_MIN,
        Math.min(config.settings.PROBE_COUNT_MAX, Math.floor(fileSize / config.settings.PROBE_SIZE_PER_COUNT))
    );
    if (fileSize <= windowSize) return [0];
    const offsets = Array.from({ length: count }, (_, i) => Math.floor(i * ((fileSize - windowSize) / (count - 1))));
    offsets[0] = 0;
    offsets[offsets.length - 1] = Math.max(0, fileSize - windowSize);
    return [...new Set(offsets)].sort((a, b) => a - b);
  }

  /**
   * Detects encoding
   * @param raw - The raw
   * @returns The string result
   */
  public detectEncoding(raw: Buffer): string {
    // Prefer UTF-8 when the bytes actually validate as UTF-8
    if (isLikelyUtf8(raw.subarray(0, 65536))) return "utf-8";
    const result = jschardet.detect(raw.slice(0, 65536));
    return normalizeEncoding(result.encoding);
  }

  /**
   * Performs the measure row width operation.
   * @param raw - The raw
   * @param encoding - The encoding
   * @returns The [number, number] result
   */
  public measureRowWidth(raw: Buffer, encoding: string): [number, number] {
    const text = decode(raw, encoding);
    const lines = text.split(/\r?\n/).filter((l) => l.trim());
    if (!lines.length) return [256, 512];
    const sizes = lines.map((l) => Buffer.byteLength(l, bufferEncodingFor(encoding)));
    const avg = sizes.reduce((a, b) => a + b, 0) / sizes.length;
    return [avg, Math.max(...sizes)];
  }

  /**
   * SHA256 hash a fingerprint seed string, truncated to 24 characters
   * @private
   */
  private hash(seed: string): string {
    return crypto.createHash("sha256").update(seed).digest("hex").slice(0, 24);
  }

  /**
   * Performs the fingerprint probe operation.
   * @param raw - The raw
   * @param encoding - The encoding
   * @returns The string result
   */
  public fingerprintProbe(raw: Buffer, encoding: string): string {
    const text = decode(raw, encoding);
    const lines = text.split(/\r?\n/).filter((l) => l.trim());
    if (!lines.length) return this.hash("empty");

    const first = lines[0];
    for (const delim of CSV_DELIMITERS) {
      const parts = first.split(delim);
      if (parts.length > 1) {
        return this.hash(`csv|${delim}|${parts.length}|${encoding}`);
      }
    }

    try {
      const parsed = JSON.parse(first);
      if (typeof parsed === "object" && parsed !== null) {
        const keys = Object.keys(parsed).sort().join(",");
        return this.hash(`json|${keys}`);
      }
    } catch {
      // not JSON, fall through to plain text fingerprint
    }

    return this.hash(`text|${first.length}|${encoding}`);
  }

  /**
   * Extracts sample lines
   * @param raw - The raw
   * @param encoding - The encoding
   * @param n - The n
   * @returns The list of results
   */
  public extractSampleLines(raw: Buffer, encoding: string, n: number): string[] {
    const text = decode(raw, encoding);
    return text.split(/\r?\n/).filter((l) => l.trim()).slice(0, n);
  }

  /**
   * Detect and strip a delimited header line from sample lines, so classification
   * runs against actual data rather than column names.
   * @param sampleLines - Lines extracted from a probe window
   * @param jobId - Job identifier, for logging
   * @returns The data lines to classify, and whether a header was found
   */
  private stripHeaderLine(sampleLines: string[], jobId: string): HeaderStripResult {
    const firstLine = sampleLines[0];
    const hadHeader = HEADER_PATTERNS.some((pattern) => pattern.test(firstLine));

    this.logger.info("detect_header_check", {
      job_id: jobId,
      firstLine,
      hasHeader: hadHeader,
      sampleLinesCount: sampleLines.length,
    });

    if (!hadHeader || sampleLines.length <= 1) {
      return { dataLines: sampleLines, hadHeader: false };
    }

    const dataLines = sampleLines.slice(1);
    this.logger.info("detect_header_skipped", { job_id: jobId, dataLinesCount: dataLines.length });
    return { dataLines, hadHeader: true };
  }

  /**
   * Normalize a message's field_spec into a string array, tolerating a
   * JSON-encoded string form of the field spec.
   * @param fieldSpec - Raw field_spec from the incoming message
   * @returns Parsed field spec array (empty array if parsing fails)
   */
  private parseFieldSpec(fieldSpec: string | string[]): string[] {
    if (typeof fieldSpec !== "string") return fieldSpec;
    try {
      return JSON.parse(fieldSpec);
    } catch {
      return [];
    }
  }

  /**
   * Classify an unknown line via the AI classifier, racing against a timeout.
   * Handles all timeout logging/metrics; returns null if the call timed out.
   * @param req - Classification request
   * @param jobId - Job identifier, for logging
   * @param fingerprint - Fingerprint of the probe window, for logging
   * @returns Classification response, or null on timeout
   */
  private async classifyWithTimeout(req: ClassifyRequest, jobId: string, fingerprint: string): Promise<ClassifyResponse | null> {
    this.logger.info("ai_call_initiated", {
      job_id: jobId,
      source: "detect_bootstrap",
      unknown_line_length: req.unknown_line.length,
      context_lines: (req.context_lines || []).length,
      fingerprint,
    });

    try {
      const config = this.getConfig();
      const aiTimeout = new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("ai_classify_timeout")), config.settings.AI_CLASSIFY_TIMEOUT_MS)
      );
      const resp = await Promise.race([this.classify(req), aiTimeout]);
      this.logger.info("ai_call_completed", {
        job_id: jobId,
        source: "detect_bootstrap",
        verdict: resp.kind,
        has_template: !!resp.template,
        fingerprint,
      });
      return resp;
    } catch (aiErr) {
      this.logger.warn("ai_call_timeout", { job_id: jobId, source: "detect_bootstrap", fingerprint, error: String(aiErr) });
      metrics.increment("detect.ai_timeout", 1);
      return null;
    }
  }

  /**
   * Process a single probe window: read it, fingerprint it, resolve against an
   * existing template if one already matches, or classify it to seed a new one.
   * Returns null if this fingerprint was already seen in an earlier window
   * (a pure cache hit — no template resolution needed).
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
  private async processProbeWindow(
      bucket: string,
      key: string,
      offset: number,
      windowSize: number,
      fileSize: number,
      encoding: string,
      fieldSpecArray: string[],
      jobId: string,
      seen: Set<string>
  ): Promise<ProbeResult | null> {
    const end = Math.min(offset + windowSize - 1, fileSize - 1);
    const probeRaw = await this.gcsUtils.readRange(bucket, key, offset, end);
    const fingerprint = this.fingerprintProbe(probeRaw, encoding);

    if (seen.has(fingerprint)) return null;
    seen.add(fingerprint);

    const existing = templateRegistry.getByFingerprint(fingerprint);
    if (existing) {
      return { fingerprint, templateId: existing.template_id };
    }

    const sampleLines = this.extractSampleLines(probeRaw, encoding, 10);
    if (!sampleLines.length) return { fingerprint, templateId: null };

    const { dataLines } = this.stripHeaderLine(sampleLines, jobId);
    if (!dataLines.length) return { fingerprint, templateId: null };

    const req: ClassifyRequest = {
      unknown_line: dataLines[0],
      field_spec: fieldSpecArray,
      context_lines: dataLines.slice(1),
      job_id: jobId,
    };

    const resp = await this.classifyWithTimeout(req, jobId, fingerprint);
    if (!resp) return { fingerprint, templateId: null };

    const templateId = this.extractTemplateId(resp.template);
    if (templateId) {
      this.logger.info("ai_template_saved", {
        job_id: jobId,
        source: "detect_bootstrap",
        kind: resp.kind,
        template_id: templateId,
        fingerprint,
        saved_to_registry: true,
      });
      this.logger.info("seed_template_created", { job_id: jobId, kind: resp.kind, template_id: templateId, fingerprint });
      metrics.increment("detect.template_created", 1, { kind: resp.kind });
      return { fingerprint, templateId };
    }

    this.logger.info("ai_no_template_returned", { job_id: jobId, source: "detect_bootstrap", verdict: resp.kind, fingerprint });
    return { fingerprint, templateId: null };
  }

  /**
   * Detect encoding and probe window sizing from the head of the file.
   * @param bucket - GCS bucket
   * @param key - GCS object key
   * @param fileSize - Total file size in bytes
   * @returns Detected encoding and computed probe window size
   */
  private async detectFileProperties(bucket: string, key: string, fileSize: number): Promise<{ encoding: string; windowSize: number }> {
    const headEnd = Math.min(this.getConfig().settings.PROBE_WINDOW_MIN_BYTES - 1, fileSize - 1);
    const headRaw = await this.gcsUtils.readRange(bucket, key, 0, headEnd);
    const encoding = this.detectEncoding(headRaw);
    const [avgRow, maxRow] = this.measureRowWidth(headRaw, encoding);
    const windowSize = this.computeWindowSize(avgRow, maxRow);
    return { encoding, windowSize };
  }

  /**
   * Forward the job to the parse queue with resolved seed template ids.
   * @param msg - Original classify message
   * @param jobId - Job identifier
   * @param fileSize - Resolved file size
   * @param seedTemplateIds - Template ids seeded/resolved during probing
   */
  private async forwardToParse(msg: ClassifyMessage, jobId: string, fileSize: number, seedTemplateIds: string[]): Promise<void> {
    const parseMsg: ParseMessage = {
      job_id: jobId,
      s3_url: msg.s3_url,
      size: fileSize,
      field_spec: msg.field_spec,
      seed_template_ids: seedTemplateIds,
    };

    const config = this.getConfig();
    try {
      await sendRaw(config.settings.PARSE_QUEUE_URL, parseMsg as unknown as Record<string, unknown>);
    } catch (sendErr) {
      this.logger.error(
          "detect_send_to_parse_failed",
          { job_id: jobId, queue_url: config.settings.PARSE_QUEUE_URL },
          sendErr instanceof Error ? sendErr : new Error(String(sendErr))
      );
      throw sendErr;
    }
  }

  /**
   * Bootstraps job
   * @param msg - The msg
   */
  public async bootstrapJob(msg: ClassifyMessage): Promise<void> {
    await templateRegistry.loadFromDatabase();

    const jobId = msg.job_id;
    this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.DETECTING });
    this.logger.info("detect_start", { jobId, s3_url: msg.s3_url, size: msg.size });

    const [bucket, key] = this.gcsUtils.parseGcsUrl(msg.s3_url);
    const fileSize = msg.size || (await this.gcsUtils.objectSize(bucket, key));

    const { encoding, windowSize } = await this.detectFileProperties(bucket, key, fileSize);

    const offsets = this.computeProbeOffsets(fileSize, windowSize);
    this.logger.info("probing", { job_id: jobId, probe_count: offsets.length, file_size: fileSize });
    metrics.increment("detect.probe_start", 1, { probe_count: String(offsets.length) });

    const fieldSpecArray = this.parseFieldSpec(msg.field_spec);

    const seen = new Set<string>();
    const seedTemplateIds: string[] = [];

    for (const offset of offsets) {
      const result = await this.processProbeWindow(bucket, key, offset, windowSize, fileSize, encoding, fieldSpecArray, jobId, seen);
      if (result?.templateId) seedTemplateIds.push(result.templateId);
    }

    this.logger.info("detect_complete", { job_id: jobId, seeds: seedTemplateIds.length, probes: offsets.length });
    metrics.increment("detect.complete", 1, { seeds: String(seedTemplateIds.length) });

    await this.forwardToParse(msg, jobId, fileSize, seedTemplateIds);
  }

  /**
   * Emits the operation
   * @param jobId - The job identifier
   * @param eventType - The event type
   * @param data - The data to process
   */
  private emit(jobId: string, eventType: EventType, data: Record<string, unknown>) {
    publishEvent(makeJobEvent(eventType, jobId, "detect_bootstrap", data));
  }

  private extractTemplateId(template: unknown): string | null
  {
    if (template && typeof template === "object" && "template_id" in template) {
      return (template as { template_id: string }).template_id;
    }
    return null;
  }
}

export default DetectBootstrapServiceImpl;
