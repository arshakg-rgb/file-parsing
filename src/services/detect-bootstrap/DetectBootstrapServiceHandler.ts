import pino from "pino";
import crypto from "crypto";
import jschardet, {IDetectedMap} from "jschardet";
import { settings } from "@shared/Settings.js";
import { EventType, makeJobEvent } from "@shared/models/events.js";
import { JobStatus, ClassifyMessage, ParseMessage } from "@shared/models/job.js";
import { receiveMessages, deleteMessage, sendRaw, publishEvent } from "@shared/QueueService.js";
import { parseGcsUrl, objectSize, readRange } from "@shared/GcsUtils.js";
import { templateRegistry, RecordTemplate, RubbishTemplate } from "@shared/TemplateRegistryService.js";
import { createLogger } from "@utils/logger/Log.js";
import { waitForDb } from "@shared/DatabaseManager.js";
import { aiClassifierService } from "@service/ai-classifier/AiClassifierServiceHandler.js";
import {
  ClassifyKind,
  ClassifyRequest, ClassifyResponse, CSV_DELIMITERS, HEADER_PATTERNS,
  HeaderStripResult,
  ProbeResult
} from "@service/detect-bootstrap/io/IDetectBootstrap";
import EncodingService from "@utils/normalizers/Encoding";
import HealthService from "@utils/response/Health";
import {MetricsUtils} from "@utils/response/Metrics";


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

  private logger = createLogger(module);

  private classify: ((req: ClassifyRequest) => Promise<ClassifyResponse>) | null = null;

  /**
   * Private constructor for singleton pattern
   */
  private constructor()
  {
    if (process.env.HEALTH_CHECK_PORT)
    {
      HealthService.startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
    }

    this.initializeClassifier();
  }

  /**
   * Get singleton instance
   */

  static getInstance(): DetectBootstrapService
  {
    if (!DetectBootstrapService.instance)
    {
      DetectBootstrapService.instance = new DetectBootstrapService();
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

        const aiResp: ClassifyResponse = await aiClassifierService.classifyAi(aiReq);

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
    await waitForDb();
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

  private emit(jobId: string, eventType: EventType, data: Record<string, unknown>): void
  {
    publishEvent(makeJobEvent(eventType, jobId, "detect-bootstrap", data));
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
      return { dataLines: sampleLines, hadHeader: false };
    }

    this.stats.headerSkips++;
    const dataLines: string[] = sampleLines.slice(1);
    this.logger.info("detect_header_skipped", { job_id: jobId, dataLinesCount: dataLines.length });
    return { dataLines, hadHeader: true };
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
   * Handles all timeout logging/metrics; returns null if the call timed out.
   *
   * @param req - Classification request
   * @param jobId - Job identifier, for logging
   * @param fingerprint - Fingerprint of the probe window, for logging
   * @returns Classification response, or null on timeout
   */

  private async classifyWithTimeout(req: ClassifyRequest, jobId: string, fingerprint: string): Promise<ClassifyResponse | null>
  {
    this.logger.info("ai_call_initiated", {
      job_id: jobId,
      source: "detect-bootstrap",
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
        verdict: resp.kind,
        has_template: !!resp.template,
        fingerprint,
      });

      return resp;
    }
    catch (aiErr)
    {
      this.stats.aiTimeouts++;

      this.logger.warn("ai_call_timeout", {
        job_id: jobId,
        source: "detect-bootstrap",
        fingerprint,
        error: String(aiErr),
      });
      MetricsUtils.increment("detect.ai_timeout", 1);

      return null;
    }
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
    const probeRaw: Buffer = await readRange(bucket, key, offset, end);
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
    const headRaw: Buffer = await readRange(bucket, key, 0, headEnd);
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
  private async forwardToParse(msg: ClassifyMessage, jobId: string, fileSize: number, seedTemplateIds: string[]): Promise<void>
  {
    const parseMsg: ParseMessage = {
      job_id: jobId,
      s3_url: msg.s3_url,
      size: fileSize,
      field_spec: msg.field_spec,
      column_map: msg.column_map,
      seed_template_ids: seedTemplateIds,
    };

    this.logger.info("detect_sending_to_parse", { job_id: jobId, queue_url: settings.PARSE_QUEUE_URL });

    try
    {
      await sendRaw(settings.PARSE_QUEUE_URL, parseMsg as unknown as Record<string, unknown>);
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
    this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.DETECTING });

    this.logger.info("detect_start", { jobId, s3_url: msg.s3_url, size: msg.size });

    const [bucket, key] = parseGcsUrl(msg.s3_url);
    const fileSize: number = msg.size || (await objectSize(bucket, key));

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

    const bootstrapDuration: number = Date.now() - bootstrapStartTime;

    this.logger.info("detect_complete", {
      job_id: jobId,
      seeds: seedTemplateIds.length,
      probes: offsets.length,
      duration_ms: bootstrapDuration,
    });

    MetricsUtils.increment("detect.complete", 1, { seeds: String(seedTemplateIds.length) });
    MetricsUtils.set("detect.duration_ms", bootstrapDuration);

    await this.forwardToParse(msg, jobId, fileSize, seedTemplateIds);
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
    await waitForDb();
    await templateRegistry.loadFromDatabase();
    this.logger.info("detect_bootstrap_consumer_started");

    while (this.running)
    {
      const messages = await receiveMessages<ClassifyMessage>(
          settings.CLASSIFY_QUEUE_URL,
          (body) => JSON.parse(body) as ClassifyMessage,
          1
      );

      for (const { payload, receiptHandle } of messages)
      {
        try
        {
          await this.bootstrapJob(payload);
          await deleteMessage(settings.CLASSIFY_QUEUE_URL, receiptHandle);
        }
        catch (exc)
        {
          const errMsg: string = String(exc);
          this.logger.error("detect_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
          MetricsUtils.increment("detect.error", 1);
          this.emit(payload.job_id, EventType.ERROR_OCCURRED, { error: errMsg });
          await deleteMessage(settings.CLASSIFY_QUEUE_URL, receiptHandle);
        }
      }
    }

    this.logger.info("detect_bootstrap_consumer_stopped");
  }
}

DetectBootstrapService.getInstance()
    .start()
    .catch((err) => {
      console.error("detect_bootstrap_start_failed", { error: String(err) });
      process.exit(1);
    });
