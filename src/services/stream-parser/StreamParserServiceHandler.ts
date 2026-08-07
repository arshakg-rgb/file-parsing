import { settings } from "@shared/Settings.js";
import { EventType, makeJobEvent } from "@shared/models/events.js";
import { JobStatus, ParseMessage, FailureClass, JobCounts, totalFailed, ColumnMap } from "@shared/models/job.js";
import {templateRegistry} from "@shared/TemplateRegistryService.js";
import { OutputManager } from "@shared/OutputManager.js";
import { CsvOutputWriter } from "@shared/CsvOutputWriter.js";
import { QualityGate } from "@shared/QualityGate.js";
import { AdaptiveProbing } from "@shared/AdaptiveProbing.js";
import { createLogger } from "@utils/logger/Log.js";
import { DatabaseManager } from "@shared/DatabaseManager.js";
import jschardet, { IDetectedMap } from "jschardet";
import JSONbig from "json-bigint";
import crypto from "crypto";
import EncodingService from "@utils/normalizers/Encoding";
import HealthService from "@utils/response/Health";
import {MetricsUtils} from "@utils/response/Metrics";
import {ClassifyResult, LineClassifierServiceImpl} from "@service/stream-parser/impl/LineClassifierServiceImpl";
import {aiClassifierServiceImpl} from "@service/ai-classifier/impl/AiClassifierServiceImpl";
import {Repositories} from "@config/db/repositories";
import {OutputBuffer} from "@shared/OutputBuffer";
import {AIRateLimiterHandle} from "@service/stream-parser/io/IClassifier";
import { DatabaseService } from "@shared/DatabaseManager";
import {GcsUtils} from "@shared/GcsUtils";
import {InstantiationError} from "@errors/InstantiationError";
import {QueueService} from "@shared/QueueService";
import {RecordTemplate, RubbishTemplate} from "@shared/io/ITemplateRegistryService";
const JSON_SAFE = JSONbig({ storeAsString: true });


/**
 * Stream Parser Service - singleton responsible for streaming file parsing
 * with inline AI classification.
 *
 * This is the only class in this module. Follows ORM-style patterns with:
 * - Class-based architecture with instance state
 * - Dependency injection for services
 * - Lifecycle management (initialize, start, stop)
 * - Repository-style methods for data operations
 * - Clean separation of concerns
 *
 * Backward-compatible static entrypoints (parseJob, bootstrap) are
 * provided so existing call sites that used the old free-function API
 * keep working without going through getInstance() directly.
 *
 * @class StreamParserService
 */
export class StreamParserService
{
  /**
   * Singleton instance
   * @private
   */

  private static instance: StreamParserService;

  private running: boolean = false;

  /**
   * Maximum number of parse jobs to process concurrently.
   * @private
   */
  private readonly concurrency: number = Math.max(1, settings.STREAM_PARSER_CONCURRENCY);

  /**
   * RSS soft limit in bytes. When passed, the consumer will not start new
   * parse jobs until the process is back under the limit.
   * @private
   */
  private readonly memorySoftLimit: number = settings.STREAM_PARSER_MEMORY_SOFT_LIMIT_MB * 1024 * 1024;

  /**
   * In-flight parse job promises keyed by receipt handle.
   * @private
   */
  private activeJobs: Map<string, Promise<void>> = new Map();

  /**
   * Deadline extension interval (30 seconds)
   * @private
   */

  private readonly DEADLINE_EXTEND_INTERVAL_MS: number = 30000;

  /**
   * How many seconds to extend the queue ack deadline by each time a parse
   * job is still running past DEADLINE_EXTEND_INTERVAL_MS.
   * @private
   */

  private readonly ACK_DEADLINE_EXTENSION_SEC: number = 60;

  /**
   * How often (in lines) to emit a parse_progress log line.
   * @private
   */

  private readonly PARSE_PROGRESS_LOG_INTERVAL: number = 10000;

  /**
   * Max number of recent lines retained as rolling context for the AI
   * classifier. AI_CONTEXT_LINES (the slice actually sent per call) must be
   * <= this value.
   * @private
   */

  private readonly CONTEXT_LINES_CACHE_SIZE: number = 5;

  /**
   * Number of trailing lines from the recent-lines cache sent to the AI
   * classifier as context for an uncertain line.
   * @private
   */

  private readonly AI_CONTEXT_LINES: number = 3;

  /**
   * Number of leading lines in a job for which verbose classification debug
   * logs are emitted.
   * @private
   */

  private readonly DEBUG_LINE_SAMPLE_COUNT: number = 5;

  /**
   * Max parse messages pulled from the queue in a single receive call.
   * @private
   */

  private readonly MAX_PARSE_BATCH_SIZE: number = 10;

  /**
   * Long-poll wait time (seconds) for queue receives in the consumer loop.
   * @private
   */

  private readonly PARSE_QUEUE_LONG_POLL_SECONDS: number = 5;

  /**
   * Fraction of RAM_FLUSH_WATERMARK at which the "over watermark" flag is
   * cleared again (hysteresis, to avoid flapping around the high watermark).
   * @private
   */

  private readonly RAM_FLUSH_WATERMARK_LOW_RATIO: number = 0.7;

  /**
   * Backoff delay used when polling the queue and finding nothing to do, or
   * when waiting for an active-job slot to free up.
   * @private
   */

  private readonly QUEUE_POLL_BACKOFF_MS: number = 1000;

  /**
   * AI rate limiter state - token bucket, enforcing both RPM (requests per
   * minute) and burst limits. Lives directly on the service instance instead
   * of a separate class/closure so StreamParserService remains the only
   * class in this module. Exposed to collaborators via getAIRateLimiter().
   * @private
   */

  private aiRateLimiterRequests: number[] = [];

  /**
   * Cached delegation handle returned by getAIRateLimiter(), so repeated
   * calls hand out the same object identity instead of allocating a new
   * closure per call.
   * @private
   */

  private aiRateLimiterHandle: AIRateLimiterHandle | null = null;

  /**
   * Parse Count
   * @private
   */

  private parseCount: number = 0;

  private stats = {
    totalLinesProcessed: 0,
    totalAiCalls: 0,
    totalAiRecoveries: 0,
    cacheHits: 0,
    cacheMisses: 0
  };

  private logger = createLogger(module);

  private gcsUtils: GcsUtils;

  private queueService: QueueService;

  /**
   * Private constructor for singleton pattern
   */

  private constructor(enforce: () => void, gcsUtisl: GcsUtils, queueService: QueueService)
  {

    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Cannot instantiate StreamParserService directly. Use getInstance()");
    }

    if (process.env.HEALTH_CHECK_PORT)
    {
      HealthService.startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
    }

    this.gcsUtils = gcsUtisl;
    this.queueService = queueService;
    this.registerSignalHandlers();
  }

  /**
   * Get singleton instance
   */

  static getInstance(): StreamParserService
  {
    if (!StreamParserService.instance)
    {
      StreamParserService.instance = new StreamParserService(Enforce, GcsUtils.getInstance(), QueueService.getInstance());
    }

    return StreamParserService.instance;
  }

  /**
   * Register signal handlers for graceful shutdown
   */

  private registerSignalHandlers(): void
  {
    process.on("SIGTERM", () => this.shutdown("SIGTERM"));
    process.on("SIGINT", () => this.shutdown("SIGINT"));
  }

  /**
   * Graceful shutdown handler
   */

  private shutdown(signal: string): void
  {
    this.logger.warn("stream_parser_shutting_down", { signal });
    this.running = false;

    const active = Array.from(this.activeJobs.values());
    if (active.length > 0)
    {
      Promise.all(active).then(() => process.exit(0)).catch(() => process.exit(1));
    }
    else
    {
      process.exit(0);
    }
  }

  /**
   * Initialize the service
   */
  async initialize(): Promise<void>
  {
    await DatabaseService.getInstance().waitForDb();
    await templateRegistry.loadFromDatabase();
    this.logger.info("stream_parser_initialized");
  }

  /**
   * Start the consumer loop
   */
  async start(): Promise<void>
  {
    if (this.running)
    {
      this.logger.warn("stream_parser_already_running");
      return;
    }

    this.running = true;
    await this.initialize();
    this.logger.info("stream_parser_started", {
      k_revision: process.env.K_REVISION,
      k_service: process.env.K_SERVICE,
      ai_inline_mode: process.env.AI_INLINE_MODE,
    });

    await this.consumerLoop();
  }

  /**
   * Sanitize text for PostgreSQL storage
   * - Strip null bytes (Postgres text/JSON columns reject \u0000)
   * - Escape lone/invalid \u sequences that aren't valid unicode
   *
   * @param str - Input string to sanitize
   * @returns Sanitized string safe for PostgreSQL
   */

  private sanitizeForPg(str: string): string
  {
    if (str.indexOf("\u0000") === -1 && str.indexOf("\\u") === -1)
    {
      return str;
    }

    return str
        .replace(/\u0000/g, "")
        .replace(/\\u(?![0-9a-fA-F]{4})/g, "\\\\u");
  }

  /**
   * Sanitize all values recursively - single source of truth for type handling
   *
   * @param value - Any value to sanitize
   * @returns Sanitized value
   */

  private sanitizeValue(value: unknown): unknown
  {
    if (typeof value === "string")
    {
      return this.sanitizeForPg(value);
    }

    if (Array.isArray(value))
    {
      return value.map(v => this.sanitizeValue(v));
    }

    if (value instanceof Date)
    {
      return value;
    }

    if (typeof value === "object" && value !== null)
    {
      return this.sanitizeRecord(value as Record<string, unknown>);
    }

    return value;
  }

  /**
   * Sanitize all string values in a record recursively
   * Handles nested objects, arrays, and Date objects correctly
   *
   * @param record - Record to sanitize
   * @returns Sanitized record
   */

  private sanitizeRecord(record: Record<string, unknown>): Record<string, unknown>
  {
    const sanitized: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(record))
    {
      sanitized[key] = this.sanitizeValue(value);
    }

    return sanitized;
  }

  /**
   * Extract JSON records from a parsed JSON value for processing as individual
   * classifier inputs. Arrays become one record per element. Objects become one
   * record per top-level value.
   *
   * @param data - Parsed JSON value
   * @returns List of JSON-stringified records
   */

  private extractJsonRecords(data: unknown): string[]
  {
    const records: string[] = [];

    const pushRecord = (value: unknown): void =>
    {
      if (value === undefined)
      {
        return;
      }
      records.push(JSON.stringify(value));
    };

    if (Array.isArray(data))
    {
      for (const item of data) pushRecord(item);
    }
    else if (typeof data === "object")
    {
      for (const [, value] of Object.entries(data as Record<string, unknown>))
      {
        if (Array.isArray(value))
        {
          for (const item of value) pushRecord(item);
        }
        else
        {
          pushRecord(value);
        }
      }
    }
    else
    {
      pushRecord(data);
    }

    return records;
  }

  /**
   * Acquire an AI rate-limit token, waiting as needed to respect both the
   * burst limit and the requests-per-minute limit. Mutates
   * `aiRateLimiterRequests` in place (token bucket).
   * @private
   */

  private async acquireAiRateLimitToken(): Promise<void>
  {
    const rpm: number = settings.AI_RATE_LIMIT_RPM;
    const burst: number = settings.AI_RATE_LIMIT_BURST;
    const now: number = Date.now();
    const oneMinuteAgo: number = now - 60000;

    this.aiRateLimiterRequests = this.aiRateLimiterRequests.filter(time => time > oneMinuteAgo);

    if (this.aiRateLimiterRequests.length >= burst)
    {
      const oldestRequest: number  = this.aiRateLimiterRequests[0];
      const waitTime: number  = oldestRequest + 60000 - now;

      if (waitTime > 0)
      {
        this.logger.warn("ai_rate_limit_burst", { waitTime, currentRequests: this.aiRateLimiterRequests.length, burst });
        await new Promise(resolve => setTimeout(resolve, waitTime));
        this.aiRateLimiterRequests = this.aiRateLimiterRequests.filter(time => time > oneMinuteAgo);
      }
    }

    if (this.aiRateLimiterRequests.length >= rpm)
    {
      const oldestRequest: number  = this.aiRateLimiterRequests[0];
      const waitTime: number  = oldestRequest + 60000 - now;

      if (waitTime > 0)
      {
        this.logger.warn("ai_rate_limit_rpm", { waitTime, currentRequests: this.aiRateLimiterRequests.length, rpm });
        await new Promise(resolve => setTimeout(resolve, waitTime));
        this.aiRateLimiterRequests = this.aiRateLimiterRequests.filter(time => time > oneMinuteAgo);
      }
    }

    this.aiRateLimiterRequests.push(now);
    this.logger.debug("ai_rate_limit_acquired", { currentRequests: this.aiRateLimiterRequests.length, rpm, burst });
  }

  /**
   * Get current AI rate limiter statistics.
   * @private
   */

  private getAiRateLimiterStats(): { currentRequests: number; rpm: number; burst: number }
  {
    return {
      currentRequests: this.aiRateLimiterRequests.length,
      rpm: settings.AI_RATE_LIMIT_RPM,
      burst: settings.AI_RATE_LIMIT_BURST
    };
  }

  /**
   * Reset the AI rate limiter (useful for testing).
   * @private
   */

  private resetAiRateLimiter(): void
  {
    this.aiRateLimiterRequests = [];
  }

  /**
   * Get the AI rate limiter handle passed to collaborators (e.g. the line
   * classifier). Cached so repeated calls return the same object identity.
   * @returns AIRateLimiterHandle instance
   */

  private getAIRateLimiter(): AIRateLimiterHandle
  {
    if (!this.aiRateLimiterHandle)
    {
      this.aiRateLimiterHandle = {
        acquire: () => this.acquireAiRateLimitToken(),
        getStats: () => this.getAiRateLimiterStats(),
        reset: () => this.resetAiRateLimiter(),
      };
    }

    return this.aiRateLimiterHandle;
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
    await this.queueService.publishEvent(makeJobEvent(eventType, jobId, "stream-parser", data));
  }

  /**
   * Parse a file job with streaming line-by-line processing
   *
   *
   * This is the main entry point for parsing a single file. It:
   * 1. Loads templates from the database
   * 2. Detects file encoding and structure via adaptive probing
   * 3. Streams lines through the classifier
   * 4. Uses inline AI for uncertain lines (if enabled)
   * 5. Outputs to Parquet and CSV
   * 6. Maintains trace records and DLQ for failed lines
   *
   * @param msg - Parse message containing job details
   * @param receiptHandle
   * @throws Error if fatal error occurs during parsing
   */

  async parseJob(msg: ParseMessage, receiptHandle?: string): Promise<void>
  {
    const parseStartTime: number = Date.now();
    this.parseCount++;

    const activeReceiptHandle: string | null = receiptHandle || null;
    let lastDeadlineExtension: number = Date.now();

    await templateRegistry.loadFromDatabase();

    const jobId: string = msg.job_id;
    await this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.PARSING });
    this.logger.info("parse_start", { job_id: jobId, s3_url: msg.s3_url, size: msg.size });
    MetricsUtils.increment("parse.start", 1);

    const [bucket, key] = GcsUtils.getInstance().parseGcsUrl(msg.s3_url);

    let fieldSpec: string[] = [];

    if (typeof msg.field_spec === "string")
    {
      try
      {
        fieldSpec = JSON.parse(msg.field_spec);
      }
      catch
      {
        this.logger.warn("field_spec_parse_failed", { job_id: jobId, field_spec: msg.field_spec });
        fieldSpec = [];
      }
    }
    else
    {
      fieldSpec = msg.field_spec;
    }

    const fileSize: number = msg.size || (await this.gcsUtils.objectSize(bucket, key));
    const probing: AdaptiveProbing = AdaptiveProbing.getInstance();
    const probeCount: number = probing.calculateProbeCount(fileSize);
    const probeOffsets: number[] = probing.generateProbeOffsets(fileSize, probeCount);

    this.logger.info("adaptive_probing", { job_id: jobId, probe_count: probeCount, file_size: fileSize });
    MetricsUtils.increment("parse.probing_start", 1, { probe_count: String(probeCount) });

    const probeStartTime: number = Date.now();

    let detectedEncoding: string = "utf-8";
    let avgRowWidth: number = 0;
    let maxRowWidth: number = 0;

    for (const offset of probeOffsets)
    {
      const endOffset: number = Math.min(offset + settings.PROBE_WINDOW_MIN_BYTES - 1, fileSize - 1);

      try
      {
        const buffer: Buffer = await this.gcsUtils.readRange(bucket, key, offset, endOffset);

        if (EncodingService.isLikelyUtf8(buffer))
        {
          detectedEncoding = "utf-8";
        }
        else
        {
          const detected: IDetectedMap = jschardet.detect(buffer);

          if (detected.encoding && detected.confidence > 0.9)
          {
            detectedEncoding = EncodingService.normalizeEncoding(detected.encoding);
          }
        }

        const content: string = buffer.toString("utf-8").replace(/\0/g, "");
        const lines: string[] = content.split("\n").filter(line => line.trim());
        if (lines.length > 0)
        {
          const widths: number[] = lines.map(l => l.length);
          avgRowWidth = Math.max(avgRowWidth, widths.reduce((a, b) => a + b, 0) / widths.length);
          maxRowWidth = Math.max(maxRowWidth, ...widths);
        }
      }
      catch (err)
      {
        this.logger.warn("probe_failed", { job_id: jobId, offset, error: String(err) });
      }
    }

    const probeDuration: number = Date.now() - probeStartTime;

    this.logger.info("probing_complete", {
      job_id: jobId,
      encoding: detectedEncoding,
      avg_row_width: avgRowWidth,
      max_row_width: maxRowWidth,
      duration_ms: probeDuration
    });

    const recordTemplates: RecordTemplate[] = templateRegistry.getAllRecordTemplates();
    const rubbishTemplates: RubbishTemplate[] = templateRegistry.getAllRubbishTemplates();
    const columnMap = (msg as unknown as Record<string, unknown>).column_map as ColumnMap | undefined;
    const classifier: LineClassifierServiceImpl = LineClassifierServiceImpl.getInstance(jobId, fieldSpec, recordTemplates, rubbishTemplates, columnMap, this.getAIRateLimiter());
    const outputManager = new OutputManager();
    const csvWriter = new CsvOutputWriter(jobId, fieldSpec);
    const qualityGate: QualityGate = QualityGate.getInstance();

    const counts: JobCounts = { parsed: 0, dropped_rubbish: 0, failed_by_class: {} };
    let lineNo: number = 0;
    let recordIndex: number = 0;
    let fatal: Error | null = null;

    const aiMode: string = settings.AI_INLINE_MODE;
    const aiEnabled: boolean = aiMode === "mock" || aiMode === "live";
    const aiBudget: number = settings.MAX_AI_CALLS_PER_JOB;
    let aiCalls: number = 0;
    let aiLocalRecoveries: number = 0;
    let aiBudgetFlagged: boolean = false;
    const recentLines: string[] = [];

    const BATCH_SIZE: number = Math.max(1, settings.PARSE_DB_FLUSH_BATCH_SIZE);
    let parsedBatch: Record<string, unknown>[] = [];
    let rubbishBatch: Record<string, unknown>[] = [];
    let dlqBatch: Record<string, unknown>[] = [];
    const repositories: Repositories = DatabaseManager.getInstance().repositories;
    const bgFlushes: Promise<void>[] = [];

    const RAM_WATERMARK_HIGH: number = settings.RAM_FLUSH_WATERMARK;
    const RAM_WATERMARK_LOW: number = settings.RAM_FLUSH_WATERMARK * this.RAM_FLUSH_WATERMARK_LOW_RATIO;
    let overWatermark: boolean = false;
    const HOUSEKEEPING_INTERVAL_LINES: number = 1000;
    let sinceHousekeeping: number = 0;

    const drainIfReady = async (): Promise<void> =>
    {
      if (bgFlushes.length >= settings.PARSE_BG_FLUSH_QUEUE_DEPTH)
      {
        await bgFlushes.shift();
      }

      const flushTasks: Promise<void>[] = [];

      if (parsedBatch.length >= BATCH_SIZE)
      {
        const batch: Record<string, unknown>[] = parsedBatch; parsedBatch = [];
        flushTasks.push(repositories.parsedRecords.bulkCreate(batch as any).catch(e => {
          this.logger.warn("parsed_batch_flush_error", { error: String(e) });
        }));
      }

      if (rubbishBatch.length >= BATCH_SIZE)
      {
        const batch: Record<string, unknown>[] = rubbishBatch; rubbishBatch = [];
        flushTasks.push(repositories.rubbishLogs.bulkCreate(batch as any).catch(e => {
          this.logger.warn("rubbish_batch_flush_error", { error: String(e) });
        }));
      }

      if (dlqBatch.length >= BATCH_SIZE)
      {
        const batch: Record<string, unknown>[] = dlqBatch; dlqBatch = [];
        flushTasks.push(repositories.deadLetters.bulkCreate(batch as any).catch(e => {
          this.logger.warn("dlq_batch_flush_error", { error: String(e) });
        }));
      }

      if (flushTasks.length > 0)
      {
        bgFlushes.push(Promise.all(flushTasks).then(() => {}));
      }

      if (++sinceHousekeeping < HOUSEKEEPING_INTERVAL_LINES)
      {
        return;
      }

      sinceHousekeeping = 0;

      const mem = process.memoryUsage();

      if (!overWatermark && mem.rss >= RAM_WATERMARK_HIGH)
      {
        overWatermark = true;
        this.logger.warn("ram_watermark_reached", { rss: mem.rss, heap_used: mem.heapUsed, watermark: RAM_WATERMARK_HIGH });
        await flushBatches(true);
        await outputManager.flushAll();
        csvWriter.flushPending();
      }
      else if (overWatermark && mem.rss < RAM_WATERMARK_LOW)
      {
        overWatermark = false;
      }

      if (activeReceiptHandle && Date.now() - lastDeadlineExtension > this.DEADLINE_EXTEND_INTERVAL_MS)
      {
        try
        {
          this.logger.info("ack_deadline_extending", { job_id: jobId, receiptHandle: activeReceiptHandle.substring(0, 20) + "..." });
          await this.queueService.modifyAckDeadline(settings.PARSE_QUEUE_URL, activeReceiptHandle, this.ACK_DEADLINE_EXTENSION_SEC);
          lastDeadlineExtension = Date.now();
          this.logger.info("ack_deadline_extended", { job_id: jobId });
        }
        catch (err)
        {
          this.logger.error("ack_deadline_extension_failed", { job_id: jobId, error: String(err) });
        }
      }
    };

    const flushBatches = async (force = false): Promise<void> =>
    {
      const flushTasks: Promise<void>[] = [];

      if (force && parsedBatch.length > 0) {
        const batch: Record<string, unknown>[] = parsedBatch; parsedBatch = [];
        flushTasks.push(repositories.parsedRecords.bulkCreate(batch as any).catch(e => {
          this.logger.warn("parsed_batch_flush_error", { error: String(e) });
        }));
      }

      if (force && rubbishBatch.length > 0)
      {
        const batch: Record<string, unknown>[] = rubbishBatch; rubbishBatch = [];
        flushTasks.push(repositories.rubbishLogs.bulkCreate(batch as any).catch(e => {
          this.logger.warn("rubbish_batch_flush_error", { error: String(e) });
        }));
      }

      if (force && dlqBatch.length > 0)
      {
        const batch: Record<string, unknown>[] = dlqBatch; dlqBatch = [];
        flushTasks.push(repositories.deadLetters.bulkCreate(batch as any).catch(e => {
          this.logger.warn("dlq_batch_flush_error", { error: String(e) });
        }));
      }

      if (flushTasks.length > 0)
      {
        await Promise.all(flushTasks);
      }

      if (bgFlushes.length > 0)
      {
        await Promise.all(bgFlushes.splice(0));
      }
    };

    const isJsonFile: boolean = key.endsWith(".json") && !key.endsWith(".ndjson");
    let jsonRecords: string[] | null = null;

    if (isJsonFile)
    {
      try
      {
        const buf: Buffer = await this.gcsUtils.readFull(bucket, key);
        const text: string = buf.toString(detectedEncoding as BufferEncoding).replace(/\0/g, "");
        jsonRecords = this.extractJsonRecords(JSON_SAFE.parse(text));
      }
      catch (err)
      {
        this.logger.warn("json_parse_failed", { job_id: jobId, s3_url: msg.s3_url, error: String(err) });
      }
    }

    try
    {
      const lineSource: AsyncIterable<[string, number, number]> = jsonRecords
          ? (async function* () {
            for (let i = 0; i < jsonRecords!.length; i++) {
              const line = jsonRecords![i];
              yield [line, i, line.length] as [string, number, number];
            }
          })()
          : this.gcsUtils.streamLines(bucket, key, settings.FETCH_CHUNK_SIZE, detectedEncoding);

      let aiHeaderMapped: boolean = false;

      const lineSourceStart: number = Date.now();

      const enrichFromMeta = async (row: Record<string, unknown>): Promise<Record<string, unknown>> => {

        if (!aiEnabled || !row.meta || typeof row.meta !== "string")
        {
          return row;
        }

        if (row.meta.trim().length === 0)
        {
          return row;
        }

        try
        {
          const extracted = await aiClassifierServiceImpl.extractFromMeta(row.meta, fieldSpec, jobId);

          if (!extracted)
          {
            return row;
          }

          for (const [field, value] of Object.entries(extracted.row))
          {
            if (value !== null && value !== undefined)
            {
              const current: unknown = row[field];

              if (current === null || current === undefined || String(current).trim() === "")
              {
                row[field] = value;
              }
              else if (typeof current === "string" && typeof value === "string")
              {
                row[field] = `${current}, ${value}`;
              }
            }
          }

          row.meta = extracted.cleanedMeta;
        }
        catch (err)
        {
          this.logger.warn("ai_meta_enrichment_failed", { job_id: jobId, error: String(err) });
        }

        return row;
      };

      for await (const [line, byteOffset, byteLength] of lineSource)
      {
        lineNo += 1;
        this.stats.totalLinesProcessed++;

        if (lineNo % this.PARSE_PROGRESS_LOG_INTERVAL === 0)
        {
          this.logger.info("parse_progress", { job_id: jobId, line_no: lineNo, parsed: counts.parsed, dropped: counts.dropped_rubbish, failed: totalFailed(counts) });
        }
        await drainIfReady();

        if (!aiHeaderMapped && aiEnabled && !columnMap)
        {
          aiHeaderMapped = true;
          try {
            const aiMapping: Record<string, number[]> = await aiClassifierServiceImpl.mapHeaderColumns(line, fieldSpec, jobId);

            if (aiMapping)
            {
              classifier.setHeaderMap(aiMapping, line);
            }
          }
          catch (err)
          {
            this.logger.warn("ai_header_mapping_failed", { job_id: jobId, error: String(err) });
          }
        }

        let result;

        try
        {
          result = classifier.classify(line, byteOffset, byteLength);
        }
        catch (lineError)
        {
          this.logger.error("line_classification_failed", { job_id: jobId, line_no: lineNo, error: lineError instanceof Error ? lineError.message : String(lineError) });
          counts.dropped_rubbish++;
          continue;
        }

        if (result.verdict === "uncertain" && aiEnabled)
        {
          if (aiCalls < aiBudget)
          {
            const remainingBudget: number = aiBudget - aiCalls;
            this.logger.info("ai_call_initiated", { job_id: jobId, line_no: lineNo, ai_calls: aiCalls, ai_budget: aiBudget, remaining_budget: remainingBudget, context_lines: recentLines.slice(-this.AI_CONTEXT_LINES).length });

            try
            {
              const aiCallStart: number = Date.now();
              const aiResult: ClassifyResult = await classifier.classifyWithTimeout(line, recentLines.slice(-this.AI_CONTEXT_LINES), settings.AI_CLASSIFY_TIMEOUT_MS, remainingBudget);
              const used: number = aiResult.ai_calls_used ?? 0;
              aiCalls += used;
              this.stats.totalAiCalls += used;
              this.logger.info("ai_call_completed", { job_id: jobId, line_no: lineNo, ai_calls: aiCalls, ai_calls_used: used, verdict: aiResult.verdict, template_id: aiResult.template_id, duration_ms: Date.now() - aiCallStart });

              if (aiResult.verdict !== "uncertain")
              {
                aiLocalRecoveries++;
                this.stats.totalAiRecoveries++;
                result = aiResult;
              }
              else
              {
                this.logger.info("ai_call_uncertain", { job_id: jobId, line_no: lineNo, ai_calls: aiCalls });
              }
            }
            catch (aiErr)
            {
              this.logger.error("inline_ai_failed", { job_id: jobId, line_no: lineNo, ai_calls: aiCalls, error: aiErr instanceof Error ? aiErr.message : String(aiErr) });
            }
          }
          else if (!aiBudgetFlagged)
          {
            aiBudgetFlagged = true;
            this.logger.warn("ai_budget_exhausted", { job_id: jobId, line_no: lineNo, ai_calls: aiCalls, budget: aiBudget, note: "file flagged; remaining unknowns dead-lettered" });
          }
        }

        recentLines.push(line);

        if (recentLines.length > this.CONTEXT_LINES_CACHE_SIZE)
        {
          recentLines.shift();
        }

        if (lineNo <= this.DEBUG_LINE_SAMPLE_COUNT)
        {
          this.logger.debug("classification_debug", { job_id: jobId, line_no: lineNo, verdict: result.verdict, template_id: result.template_id, line_length: line.length });
        }

        switch (result.verdict)
        {
          case "parsed": {
            const sanitizedRow: Record<string, unknown> = this.sanitizeRecord(result.row || {});

            if (result.template_id === "json" || result.template_id?.startsWith("json-"))
            {
              await enrichFromMeta(sanitizedRow);
            }

            if (!classifier.rowStrongFieldsOk(sanitizedRow))
            {
              counts.dropped_rubbish++;
              break;
            }

            const idx: number = recordIndex++;
            const outputBuffer: OutputBuffer = outputManager.getBuffer(jobId, result.template_id || "default");
            const parsedAt: Date = new Date();

            const maybeFlush = outputBuffer.addRow({
              ...sanitizedRow,
              _job_id: jobId,
              _byte_offset: byteOffset,
              _byte_length: byteLength,
              _record_index: idx,
              _line_no: lineNo,
              _template_id: result.template_id,
              _template_version: result.template_version ?? 1,
              _checksum: "",
              _parsed_at: parsedAt,
              _part_id: "auto",
            });

            parsedBatch.push({
              _job_id: jobId,
              _byte_offset: byteOffset,
              _byte_length: byteLength,
              _record_index: idx,
              _line_no: lineNo,
              _template_id: result.template_id || "default",
              _template_version: result.template_version || 1,
              _checksum: "",
              _parsed_at: parsedAt,
              _part_id: "auto",
              fields: { s3_url: msg.s3_url, ...sanitizedRow }
            });

            if (maybeFlush)
            {
              await maybeFlush;
            }

            counts.parsed++;
            csvWriter.addRow(sanitizedRow, lineNo);
            break;
          }

          case "rubbish":
          {
            const sanitizedLine: string = this.sanitizeForPg(line);
            rubbishBatch.push({
              job_id: jobId,
              byte_offset: byteOffset,
              line_no: lineNo,
              raw_bytes: sanitizedLine,
              matched_template_id: result.template_id || "unknown",
            });
            counts.dropped_rubbish++;
            break;
          }

          case "uncertain":
          {
            const trimmed: string = line.trim();
            let isJsonShape: boolean = trimmed[0] === "{" || trimmed[0] === "[";

            if (!isJsonShape && trimmed[0] === "\"")
            {
              try
              {
                const unwrapped = JSON.parse(trimmed);

                if (typeof unwrapped === "string")
                {
                  const inner:string = unwrapped.trim();
                  isJsonShape = inner[0] === "{" || inner[0] === "[";
                }
              }
              catch { /* not a JSON string */ }
            }

            const isJsonLike: boolean = isJsonFile || isJsonShape;

            if (isJsonLike)
            {
              const metaRow: Record<string, unknown> = {};
              for (const f of fieldSpec) metaRow[f] = "";
              metaRow["meta"] = line;
              result = { verdict: "parsed", row: metaRow, template_id: "json-raw-meta", template_version: 1 };

              const sanitizedRow: Record<string, unknown> = this.sanitizeRecord(result.row || {});

              if (!classifier.rowStrongFieldsOk(sanitizedRow))
              {
                counts.dropped_rubbish++;
                break;
              }

              const idx: number = recordIndex++;
              const outputBuffer: OutputBuffer = outputManager.getBuffer(jobId, result.template_id || "default");

              const maybeFlush = outputBuffer.addRow({
                ...sanitizedRow,
                _job_id: jobId,
                _byte_offset: byteOffset,
                _byte_length: byteLength,
                _record_index: idx,
                _line_no: lineNo,
                _template_id: result.template_id,
                _template_version: result.template_version ?? 1,
                _checksum: "",
                _parsed_at: new Date(),
                _part_id: "auto",
              });

              parsedBatch.push({
                _job_id: jobId,
                _byte_offset: byteOffset,
                _byte_length: byteLength,
                _record_index: idx,
                _line_no: lineNo,
                _template_id: result.template_id || "default",
                _template_version: result.template_version || 1,
                _checksum: "",
                _parsed_at: new Date(),
                _part_id: "auto",
                fields: { s3_url: msg.s3_url, ...sanitizedRow }
              });

              if (maybeFlush)
              {
                await maybeFlush;
              }

              counts.parsed++;
              csvWriter.addRow(sanitizedRow, lineNo);
            }
            else
            {
              const sanitizedUncertainLine: string = this.sanitizeForPg(line);
              const failureClass = result.failure_class || FailureClass.UNCERTAIN;
              dlqBatch.push({
                dlq_id: crypto.randomUUID(),
                job_id: jobId,
                byte_offset: byteOffset,
                byte_length: byteLength,
                line_no: lineNo,
                raw_bytes: sanitizedUncertainLine,
                failure_class: failureClass,
                error: result.failure_class || "Uncertain classification",
                attempts: 0,
                status: "pending",
              });

              if (!counts.failed_by_class[failureClass])
              {
                counts.failed_by_class[failureClass] = 0;
              }

              counts.failed_by_class[failureClass]++;
            }
            break;
          }
        }
      }

      this.logger.info("lines_streamed", { job_id: jobId, line_count: lineNo, duration_ms: Date.now() - lineSourceStart });

      const finalFlushStart: number = Date.now();
      await flushBatches(true);
      this.logger.info("parse_flushed", { job_id: jobId, duration_ms: Date.now() - finalFlushStart });

      const outputFlushStart: number = Date.now();
      const outputPaths: string[] = await outputManager.flushAll();

      const csvOutputPath: string = await csvWriter.flush();

      this.logger.info("output_flushed", { job_id: jobId, csv_output_path: csvOutputPath || null, duration_ms: Date.now() - outputFlushStart });

      if (csvOutputPath)
      {
        this.logger.info("csv_output_ready", { job_id: jobId, path: csvOutputPath, rows: counts.parsed });
      }
      else
      {
        this.logger.warn("csv_output_path_missing", { job_id: jobId, parsed: counts.parsed });
      }

      const qualityCheck = await qualityGate.passesQualityGate(jobId);

      if (!qualityCheck.passes)
      {
        this.logger.warn("quality_gate_failed", { job_id: jobId, reason: qualityCheck.reason });
        await this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.FAILED, reason: qualityCheck.reason });
        return;
      }

      const failedTotal: number = totalFailed(counts);

      await this.queueService.publishEvent(makeJobEvent(EventType.PARSING_COMPLETED, jobId, "stream-parser", {
        parsed: counts.parsed,
        dropped_rubbish: counts.dropped_rubbish,
        failed: failedTotal,
        failed_by_class: counts.failed_by_class,
        part_s3_paths: outputPaths,
        dlq_count: failedTotal,
        rubbish_log_path: counts.rubbish_log_path,
        csv_output_path: csvOutputPath,
        ai_calls: aiCalls,
        ai_recoveries: aiLocalRecoveries,
      }));

      const parseDuration: number = Date.now() - parseStartTime;

      this.logger.info("parse_complete", {
        job_id: jobId,
        parsed: counts.parsed,
        dropped: counts.dropped_rubbish,
        failed: totalFailed(counts),
        duration_ms: parseDuration,
        ai_calls: aiCalls,
        ai_recoveries: aiLocalRecoveries
      });
      MetricsUtils.set("parse.lines_parsed", counts.parsed);
      MetricsUtils.set("parse.lines_dropped", counts.dropped_rubbish);
      MetricsUtils.set("parse.lines_failed", totalFailed(counts));
      MetricsUtils.set("parse.duration_ms", parseDuration);
      MetricsUtils.set("parse.ai_calls", aiCalls);
    }
    catch (exc)
    {
      fatal = exc instanceof Error ? exc : new Error(String(exc));
      this.logger.error("parse_failed", { job_id: jobId }, fatal);
      MetricsUtils.increment("parse.error", 1);
      await this.emit(jobId, EventType.ERROR_OCCURRED, { error: String(exc) });
    }
    finally
    {
      if (fatal)
      {
        try
        {
          const outputPaths: string[] = await outputManager.flushAll();

          if (outputPaths.length > 0)
          {
            this.logger.warn("partial_flush_on_fatal", { job_id: jobId, output_paths: outputPaths.length });
          }
        }
        catch (flushErr)
        {
          this.logger.error("flush_failed", { job_id: jobId, error: String(flushErr) });
        }

        await csvWriter.flush().catch(() => {});
      }

      if (fatal)
      {
        await this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.FAILED, error: String(fatal) });
      }
    }
  }

  /**
   * Main consumer loop for processing parse messages
   *
   * Continuously polls the parse queue for messages and processes them.
   * Handles graceful shutdown and message acknowledgment.
   *
   * @throws Error if database connection fails
   */

  /**
   * Process a single parse message, deleting the queue entry on
   * success and acking known bad messages to prevent retry loops.
   * @private
   */
  private async processMessage(payload: ParseMessage, receiptHandle: string): Promise<void>
  {
    try
    {
      await this.parseJob(payload, receiptHandle);
      await this.queueService.deleteMessage(settings.PARSE_QUEUE_URL, receiptHandle);
    }
    catch (exc)
    {
      const errorStr: string = String(exc);

      if (errorStr.includes("Job") && (errorStr.includes("not found") || errorStr.includes("cannot transition")))
      {
        this.logger.error("stream_parser_message_failed_ack", { job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" });
        MetricsUtils.increment("parse.message_error_ack", 1);
        await this.queueService.deleteMessage(settings.PARSE_QUEUE_URL, receiptHandle);
      }
      else
      {
        this.logger.error("stream_parser_message_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
        MetricsUtils.increment("parse.message_error", 1);
      }
    }
  }

  private async consumerLoop(): Promise<void>
  {
    await DatabaseService.getInstance().waitForDb();
    await templateRegistry.loadFromDatabase();
    this.logger.info("stream_parser_consumer_started", { concurrency: this.concurrency });

    while (this.running)
    {
      let freeSlots = this.concurrency - this.activeJobs.size;

      if (freeSlots > 0 && this.memorySoftLimit && process.memoryUsage().rss >= this.memorySoftLimit)
      {
        this.logger.warn("stream_parser_memory_high", {
          rss: process.memoryUsage().rss,
          limit: this.memorySoftLimit,
          active: this.activeJobs.size,
        });
        freeSlots = 0;
      }

      if (freeSlots > 0)
      {
        const messages = await this.queueService.receiveMessages<ParseMessage>(
            settings.PARSE_QUEUE_URL,
            (body) => JSON.parse(body) as ParseMessage,
            Math.min(freeSlots, this.MAX_PARSE_BATCH_SIZE),
            this.PARSE_QUEUE_LONG_POLL_SECONDS
        );

        if (messages.length > 0)
        {
          for (const { payload, receiptHandle } of messages)
          {
            const jobPromise = this.processMessage(payload, receiptHandle)
                .finally(() => { this.activeJobs.delete(receiptHandle); });
            this.activeJobs.set(receiptHandle, jobPromise);
          }
          continue;
        }
      }

      if (this.activeJobs.size > 0)
      {
        if (freeSlots > 0)
        {
          // Free slots but queue empty; back off briefly and re-poll.
          await new Promise((resolve) => setTimeout(resolve, this.QUEUE_POLL_BACKOFF_MS));
        }
        else
        {
          // At concurrency limit, wait for any slot to free up.
          await Promise.race(Array.from(this.activeJobs.values()));
        }
      }
      else if (this.running)
      {
        await new Promise((resolve) => setTimeout(resolve, this.QUEUE_POLL_BACKOFF_MS));
      }
    }

    this.logger.info("stream_parser_consumer_stopped");

    await Promise.all(Array.from(this.activeJobs.values()));
  }

  /**
   * Bootstraps the consumer loop when this module is loaded as the service
   * entrypoint. Guarded via STREAM_PARSER_AUTOSTART so importing this module
   * elsewhere (tests, or another service that only needs parseJob) doesn't
   * trigger a second, competing consumer loop.
   */

  public static bootstrap(): void
  {
    const instance: StreamParserService = StreamParserService.getInstance();
    instance.start().catch((err) => {
      instance.logger.error(
          "stream_parser_start_failed",
          err instanceof Error ? err : new Error(String(err))
      );
      process.exit(1);
    });
  }
}

if (process.env.STREAM_PARSER_AUTOSTART !== "false")
{
  StreamParserService.bootstrap();
}


/**
 * Private capability token. Only GcsUtils.getInstance() has a reference to
 * this function, so it's the only call site that can satisfy the
 * constructor's `enforce` check — `new GcsUtils(...)` from anywhere else
 * fails fast with InstantiationError.
 */
function Enforce(): void {}
