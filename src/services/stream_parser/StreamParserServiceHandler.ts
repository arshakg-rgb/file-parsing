import { Template } from "@shared/models/template.js";
import { settings } from "@shared/Settings.js";
import { EventType, JobEvent, makeJobEvent } from "@shared/models/events.js";
import { JobStatus, ParseMessage, FailureClass, JobCounts, totalFailed, ColumnMap } from "@shared/models/job.js";
import { receiveMessages, deleteMessage, publishEvent } from "@shared/QueueService.js";
import { parseGcsUrl, streamLines, objectSize, readRange } from "@shared/GcsUtils.js";
import { LineClassifier, parseCsvLine } from "./LineClassifier.js";
import { templateRegistry } from "@shared/TemplateRegistryService.js";
import { OutputManager } from "@shared/OutputManager.js";
import { CsvOutputWriter } from "@shared/CsvOutputWriter.js";
import { DLQManager } from "@shared/DLQManager.js";
import { TraceSystem } from "@shared/TraceSystem.js";
import { QualityGate } from "@shared/QualityGate.js";
import { AdaptiveProbing } from "@shared/AdaptiveProbing.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
import { metrics } from "@utils/response/metrics.js";
import { startHealthCheckServer } from "@utils/response/health.js";
import { waitForDb } from "@shared/DatabaseManager.js";
import MySqlManager from "@config/db/MySqlManager.js";
import jschardet from "jschardet";
import crypto from "crypto";
import { normalizeEncoding, isLikelyUtf8 } from "@utils/normalizers/encoding.js";

const _moduleLogger = createLogger("stream-parser");

/**
 * AI Rate Limiter - Token bucket implementation
 * Enforces both RPM (requests per minute) and burst limits
 * Lazy initialized to save resources when AI is disabled
 */
class AIRateLimiter {
    /**
   * Requests
   * @private
   */
  private requests: number[] = [];
    /**
   * Rpm
   * @private
   */
  private rpm: number;
    /**
   * Burst
   * @private
   */
  private burst: number;
    /**
   * Logger instance
   * @private
   */
  private logger: Logger;

    /**
   * Constructs a new AIRateLimiter instance.
   * @param rpm - The rpm
   * @param burst - The burst
   * @param logger - The logger
   */
  constructor(rpm: number, burst: number, logger: Logger) {
    this.rpm = rpm;
    this.burst = burst;
    this.logger = logger;
  }

  /**
   * Acquire a rate limit token, waiting if necessary
   * @throws Error if timeout exceeded
   */
  async acquire(): Promise<void> {
    const now = Date.now();
    const oneMinuteAgo = now - 60000;
    
    // Remove requests older than 1 minute
    this.requests = this.requests.filter(time => time > oneMinuteAgo);
    
    // Check burst limit
    if (this.requests.length >= this.burst) {
      const oldestRequest = this.requests[0];
      const waitTime = oldestRequest + 60000 - now;
      if (waitTime > 0) {
        this.logger.warn("ai_rate_limit_burst", { waitTime, currentRequests: this.requests.length, burst: this.burst });
        await new Promise(resolve => setTimeout(resolve, waitTime));
        this.requests = this.requests.filter(time => time > oneMinuteAgo);
      }
    }
    
    // Check RPM limit
    if (this.requests.length >= this.rpm) {
      const oldestRequest = this.requests[0];
      const waitTime = oldestRequest + 60000 - now;
      if (waitTime > 0) {
        this.logger.warn("ai_rate_limit_rpm", { waitTime, currentRequests: this.requests.length, rpm: this.rpm });
        await new Promise(resolve => setTimeout(resolve, waitTime));
        this.requests = this.requests.filter(time => time > oneMinuteAgo);
      }
    }
    
    this.requests.push(now);
    this.logger.debug("ai_rate_limit_acquired", { currentRequests: this.requests.length, rpm: this.rpm, burst: this.burst });
  }

  /**
   * Get current rate limiter statistics
   * @returns Current state of the rate limiter
   */
  getStats() {
    return {
      currentRequests: this.requests.length,
      rpm: this.rpm,
      burst: this.burst
    };
  }

  /**
   * Reset the rate limiter (useful for testing)
   */
  reset(): void {
    this.requests = [];
  }
}

/**
 * Stream Parser Service - Senior Level ORM-Style Implementation
 * 
 * This service handles streaming file parsing with inline AI classification.
 * Follows ORM-style patterns with:
 * - Class-based architecture with instance state
 * - Dependency injection for services
 * - Lifecycle management (initialize, start, stop)
 * - Repository-style methods for data operations
 * - Clean separation of concerns
 * 
 * @class StreamParserService
 */
export class StreamParserService {
    /**
   * Singleton instance
   * @private
   */
  private static instance: StreamParserService;
  
  // Instance state
  private running: boolean = false;
    /**
   * Current Job
   * @private
   */
  private currentJob: Promise<void> | null = null;
    /**
   * Ai Rate Limiter
   * @private
   */
  private aiRateLimiter: AIRateLimiter | null = null;
    /**
   * Template Cache
   * @private
   */
  private templateCache: Map<string, Map<string, Template>> = new Map();
    /**
   * Parse Count
   * @private
   */
  private parseCount: number = 0;
    /**
   * Last Cache Flush
   * @private
   */
  private lastCacheFlush: number = Date.now();
  
  // Statistics
  private stats = {
    totalLinesProcessed: 0,
    totalAiCalls: 0,
    totalAiRecoveries: 0,
    cacheHits: 0,
    cacheMisses: 0
  };
  
  // Dependencies (injected)
  private logger = createLogger("stream_parser");
  
  private static readonly HEADER_LABEL_RE = /^[A-Za-z][A-Za-z0-9 _.\-]*$/;

  /**
   * Infer field_spec from a delimited header row when no field_spec was supplied.
   * If allowFallback is true and the first row doesn't have clean header labels,
   * it still returns positional col_N names when the first two non-empty rows share
   * a consistent column count for a delimiter — enough to parse the file without AI.
   */
  private static inferFieldSpecFromHeader(chunk: string, allowFallback = false): string[] | null {
    const lines = chunk.split(/\r?\n/).map((l) => l.replace(/\0/g, "").trim()).filter((l) => l);
    if (lines.length === 0) return null;

    const first = lines[0];
    const second = lines[1];

    const isLabelLike = (v: string): boolean =>
      v !== "" && !v.includes("@") && v.replace(/\D/g, "").length < 7 && StreamParserService.HEADER_LABEL_RE.test(v);

    const strict = (parts: string[]): boolean => parts.every((p) => isLabelLike(p.trim()));

    let best: string[] | null = null;

    for (const delim of [",", ";", "\t", "|"]) {
      const parts = parseCsvLine(first, delim, "\"");
      if (parts.length < 2) continue;
      if (second) {
        const secondParts = parseCsvLine(second, delim, "\"");
        if (secondParts.length !== parts.length) continue;
      }
      if (!strict(parts)) continue;
      if (!best || parts.length > best.length) best = parts;
    }
    if (best) return best;

    if (!allowFallback) return null;

    for (const delim of [",", ";", "\t", "|"]) {
      const parts = parseCsvLine(first, delim, "\"");
      if (parts.length < 2) continue;
      if (second) {
        const secondParts = parseCsvLine(second, delim, "\"");
        if (secondParts.length !== parts.length) continue;
      }
      const sanitized = parts.map((p, i) => {
        const v = p.trim();
        return isLabelLike(v) ? v : `col_${i}`;
      });
      if (!best || sanitized.length > best.length) best = sanitized;
    }
    return best;
  }

  /**
   * Private constructor for singleton pattern
   */
  private constructor() {
    // Cloud Run injects PORT; always listen on it (or 8080) so startup succeeds.
    // Also honor HEALTH_CHECK_PORT if set and different.
    const ports = new Set<number>();
    const cloudRunPort = process.env.PORT ? parseInt(process.env.PORT, 10) : 8080;
    ports.add(cloudRunPort);
    if (process.env.HEALTH_CHECK_PORT) {
      const p = parseInt(process.env.HEALTH_CHECK_PORT, 10);
      if (!isNaN(p) && p !== cloudRunPort) ports.add(p);
    }
    for (const port of ports) {
      try {
        startHealthCheckServer(port);
      } catch (err) {
        this.logger.error("health_server_start_failed", { port, error: err instanceof Error ? err.message : String(err) });
      }
    }

    // Register signal handlers for graceful shutdown
    this.registerSignalHandlers();
  }
  
  /**
   * Get singleton instance
   */
  static getInstance(): StreamParserService {
    if (!StreamParserService.instance) {
      StreamParserService.instance = new StreamParserService();
    }
    return StreamParserService.instance;
  }
  
  /**
   * Register signal handlers for graceful shutdown
   */
  private registerSignalHandlers(): void {
    process.on("SIGTERM", () => this.shutdown("SIGTERM"));
    process.on("SIGINT", () => this.shutdown("SIGINT"));
  }
  
  /**
   * Graceful shutdown handler
   */
  private shutdown(signal: string): void {
    this.logger.warn("stream_parser_shutting_down", { signal });
    this.running = false;
    if (this.currentJob) {
      this.currentJob.then(() => process.exit(0)).catch(() => process.exit(1));
    } else {
      process.exit(0);
    }
  }
  
  /**
   * Initialize the service
   */
  async initialize(): Promise<void> {
    await waitForDb();
    await templateRegistry.loadFromDatabase();
    this.logger.info("stream_parser_initialized", {
      ai_inline_mode: settings.AI_INLINE_MODE,
      ai_max_calls_per_job: settings.MAX_AI_CALLS_PER_JOB,
      ai_rate_limit_rpm: settings.AI_RATE_LIMIT_RPM,
      vertex_model: settings.VERTEX_MODEL,
      vertex_location: settings.VERTEX_LOCATION,
      bedrock_model_id: settings.BEDROCK_MODEL_ID ? (settings.BEDROCK_MODEL_ID === "mock" ? "mock" : "set") : "unset",
      mock_mode_forced_by: settings.AI_INLINE_MODE === "mock" ? "AI_INLINE_MODE" : (settings.BEDROCK_MODEL_ID === "mock" ? "BEDROCK_MODEL_ID" : "none"),
    });
  }
  
  /**
   * Start the consumer loop
   */
  async start(): Promise<void> {
    if (this.running) {
      this.logger.warn("stream_parser_already_running");
      return;
    }
    
    this.running = true;
    await this.initialize();
    this.logger.info("stream_parser_started");
    
    await this.consumerLoop();
  }
  
  /**
   * Stop the service gracefully
   */
  async stop(): Promise<void> {
    this.running = false;
    this.logger.info("stream_parser_stopping");
  }
  
  /**
   * Get service statistics
   */
  getStats() {
    return {
      ...this.stats,
      parseCount: this.parseCount,
      aiRateLimiter: this.aiRateLimiter?.getStats() || null,
      templateCacheSize: this.templateCache.size
    };
  }

  /**
   * Sanitize text for PostgreSQL storage
   * - Strip null bytes (Postgres text/JSON columns reject \u0000)
   * - Escape lone/invalid \u sequences that aren't valid unicode
   * 
   * @param str - Input string to sanitize
   * @returns Sanitized string safe for PostgreSQL
   */
  private sanitizeForPg(str: string): string {
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
  private sanitizeValue(value: unknown): unknown {
    if (typeof value === "string") return this.sanitizeForPg(value);
    if (Array.isArray(value)) return value.map(v => this.sanitizeValue(v));
    if (value instanceof Date) return value;
    if (typeof value === "object" && value !== null) return this.sanitizeRecord(value as Record<string, unknown>);
    return value;
  }

  /**
   * Sanitize all string values in a record recursively
   * Handles nested objects, arrays, and Date objects correctly
   * 
   * @param record - Record to sanitize
   * @returns Sanitized record
   */
  private sanitizeRecord(record: Record<string, unknown>): Record<string, unknown> {
    const sanitized: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(record)) {
      sanitized[key] = this.sanitizeValue(value);
    }
    return sanitized;
  }

  /**
   * Get or create the AI rate limiter instance (lazy initialization)
   * @returns AIRateLimiter instance
   */
  private getAIRateLimiter(): AIRateLimiter {
    if (!this.aiRateLimiter) {
      this.aiRateLimiter = new AIRateLimiter(settings.AI_RATE_LIMIT_RPM, settings.AI_RATE_LIMIT_BURST, this.logger);
    }
    return this.aiRateLimiter;
  }

  /**
   * Emit a job event to the event system
   * 
   * @param jobId - Job identifier
   * @param eventType - Type of event to emit
   * @param data - Event payload data
   */
  private emit(jobId: string, eventType: EventType, data: Record<string, unknown>): void {
    publishEvent(makeJobEvent(eventType, jobId, "stream_parser", data));
  }

  /**
   * Parse a file job with streaming line-by-line processing
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
   * @throws Error if fatal error occurs during parsing
   */
  async parseJob(msg: ParseMessage): Promise<void> {
    const parseStartTime = Date.now();
    this.parseCount++;
    
    await templateRegistry.loadFromDatabase();

    const jobId = msg.job_id;
    this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.PARSING });
    this.logger.info("parse_start", { job_id: jobId, s3_url: msg.s3_url, size: msg.size });
    metrics.increment("parse.start", 1);

    const [bucket, key] = parseGcsUrl(msg.s3_url);
    
    // Parse field_spec if it's a JSON string
    let fieldSpec: string[] = [];
    if (typeof msg.field_spec === "string") {
      try {
        fieldSpec = JSON.parse(msg.field_spec);
      } catch {
        this.logger.warn("field_spec_parse_failed", { job_id: jobId, field_spec: msg.field_spec });
        fieldSpec = [];
      }
    } else {
      fieldSpec = msg.field_spec;
    }

    const columnMap = (msg as unknown as Record<string, unknown>).column_map as ColumnMap | undefined;
    let probeLooksTabular = false;

    const fileSize = msg.size || (await objectSize(bucket, key));

    // If no field_spec was supplied, try to infer it from a delimited header in the first line.
    // This lets headered CSV/TSV files parse even when the detect/AI step is unavailable.
    if (!fieldSpec || fieldSpec.length === 0) {
      try {
        const firstEnd = Math.min(settings.PROBE_WINDOW_MIN_BYTES - 1, fileSize - 1);
        const firstBuffer = await readRange(bucket, key, 0, firstEnd);
        const hasQuote = firstBuffer.includes(0x22);
        const hasDelimiter = [0x2c, 0x09, 0x3b, 0x7c].some((d) => firstBuffer.includes(d));
        probeLooksTabular = hasQuote && hasDelimiter;
        const firstChunk = firstBuffer.toString("utf-8").replace(/\0/g, "");
        const inferred = StreamParserService.inferFieldSpecFromHeader(firstChunk, probeLooksTabular);
        if (inferred && inferred.length > 0) {
          fieldSpec = inferred;
          this.logger.info("field_spec_inferred_from_header", { job_id: jobId, field_spec: fieldSpec, source: probeLooksTabular ? "probe" : "strict" });
        }
      } catch (err) {
        this.logger.warn("field_spec_inference_failed", { job_id: jobId, error: String(err) });
      }
    }

    // Adaptive probing to detect file structure
    const probing = AdaptiveProbing.getInstance();
    const probeCount = probing.calculateProbeCount(fileSize);
    const probeOffsets = probing.generateProbeOffsets(fileSize, probeCount);
    
    this.logger.info("adaptive_probing", { job_id: jobId, probe_count: probeCount, file_size: fileSize });
    metrics.increment("parse.probing_start", 1, { probe_count: String(probeCount) });

    let detectedEncoding = "utf-8";
    let avgRowWidth = 0;
    let maxRowWidth = 0;

    // Execute probes to detect encoding and row characteristics
    for (const offset of probeOffsets) {
      const endOffset = Math.min(offset + settings.PROBE_WINDOW_MIN_BYTES - 1, fileSize - 1);
      try {
        const buffer = await readRange(bucket, key, offset, endOffset);
        // Prefer UTF-8 when the probe window validates as UTF-8 (jschardet misdetects
        // UTF-8-with-multibyte as ISO-8859-x/windows-125x). Otherwise take a
        // high-confidence guess, normalized to a label decode() can handle via TextDecoder.
        if (isLikelyUtf8(buffer)) {
          detectedEncoding = "utf-8";
        } else {
          const detected = jschardet.detect(buffer);
          if (detected.encoding && detected.confidence > 0.9) {
            detectedEncoding = normalizeEncoding(detected.encoding);
          }
        }
        
        // Analyze row widths
        const content = buffer.toString("utf-8").replace(/\0/g, ""); // Remove null bytes
        const lines = content.split("\n").filter(line => line.trim());
        if (lines.length > 0) {
          const widths = lines.map(l => l.length);
          avgRowWidth = Math.max(avgRowWidth, widths.reduce((a, b) => a + b, 0) / widths.length);
          maxRowWidth = Math.max(maxRowWidth, ...widths);
        }
      } catch (err) {
        this.logger.warn("probe_failed", { job_id: jobId, offset, error: String(err) });
      }
    }

    this.logger.info("probing_complete", { 
      job_id: jobId, 
      encoding: detectedEncoding, 
      avg_row_width: avgRowWidth,
      max_row_width: maxRowWidth 
    });

    const recordTemplates = templateRegistry.getAllRecordTemplates();
    const rubbishTemplates = templateRegistry.getAllRubbishTemplates();
    const classifier = new LineClassifier(jobId, fieldSpec, recordTemplates, rubbishTemplates, columnMap);
    const outputManager = new OutputManager();
    const csvWriter = new CsvOutputWriter(jobId, fieldSpec);
    const dlqManager = DLQManager.getInstance();
    const traceSystem = TraceSystem.getInstance();
    const qualityGate = QualityGate.getInstance();

    const counts: JobCounts = { parsed: 0, dropped_rubbish: 0, failed_by_class: {} };
    let lineNo = 0;
    let recordIndex = 0;
    let fatal: Error | null = null;

    // Inline AI (design step 4): when the local ordered classifier can't decide, ask the model
    // once, cache its verdict as a template, and reuse it locally thereafter. Bounded per job.
    const aiMode = settings.AI_INLINE_MODE; // "off" | "mock" | "live"
    const aiEnabled = aiMode === "mock" || aiMode === "live";
    const aiBudget = settings.MAX_AI_CALLS_PER_JOB;
    let aiCalls = 0;
    let aiLocalRecoveries = 0; // unknowns the AI resolved (record or rubbish)
    let aiBudgetFlagged = false;
    let uncertainCount = 0; // cumulative uncertain lines before the circuit breaker trips
    let circuitBreakerTripped = false;
    const recentLines: string[] = []; // small context window for the model

    // Batched DB writes: fire-and-forget so the parse loop never waits for DB.
    // All three tables flush in parallel. Background errors are logged but never crash the job.
    const BATCH_SIZE = 2000;
    let parsedBatch: Record<string, unknown>[] = [];
    let rubbishBatch: Record<string, unknown>[] = [];
    let dlqBatch: Record<string, unknown>[] = [];
    const repositories = MySqlManager.getInstance().repositories;
    const bgFlushes: Promise<void>[] = [];

    const drainIfReady = async (): Promise<void> => {
      // Backpressure: if too many DB flushes are in-flight, drain the oldest before continuing
      if (bgFlushes.length >= 4) {
        await bgFlushes.shift();
      }
      const flushTasks: Promise<void>[] = [];
      if (parsedBatch.length >= BATCH_SIZE) {
        const batch = parsedBatch; parsedBatch = [];
        flushTasks.push(repositories.parsedRecords.bulkCreate(batch as any).catch(e => {
          this.logger.warn("parsed_batch_flush_error", { error: String(e) });
        }));
      }
      if (rubbishBatch.length >= BATCH_SIZE) {
        const batch = rubbishBatch; rubbishBatch = [];
        flushTasks.push(repositories.rubbishLogs.bulkCreate(batch as any).catch(e => {
          this.logger.warn("rubbish_batch_flush_error", { error: String(e) });
        }));
      }
      if (dlqBatch.length >= BATCH_SIZE) {
        const batch = dlqBatch; dlqBatch = [];
        flushTasks.push(repositories.deadLetters.bulkCreate(batch as any).catch(e => {
          this.logger.warn("dlq_batch_flush_error", { error: String(e) });
        }));
      }
      if (flushTasks.length > 0) {
        bgFlushes.push(Promise.all(flushTasks).then(() => {}));
      }
    };

    const flushBatches = async (force = false): Promise<void> => {
      const flushTasks: Promise<void>[] = [];
      if (force && parsedBatch.length > 0) {
        const batch = parsedBatch; parsedBatch = [];
        flushTasks.push(repositories.parsedRecords.bulkCreate(batch as any).catch(e => {
          this.logger.warn("parsed_batch_flush_error", { error: String(e) });
        }));
      }
      if (force && rubbishBatch.length > 0) {
        const batch = rubbishBatch; rubbishBatch = [];
        flushTasks.push(repositories.rubbishLogs.bulkCreate(batch as any).catch(e => {
          this.logger.warn("rubbish_batch_flush_error", { error: String(e) });
        }));
      }
      if (force && dlqBatch.length > 0) {
        const batch = dlqBatch; dlqBatch = [];
        flushTasks.push(repositories.deadLetters.bulkCreate(batch as any).catch(e => {
          this.logger.warn("dlq_batch_flush_error", { error: String(e) });
        }));
      }
      if (flushTasks.length > 0) await Promise.all(flushTasks);
      // Drain all background flushes too
      if (bgFlushes.length > 0) {
        await Promise.all(bgFlushes.splice(0));
      }
    };

    try {
      const hasColumnMap = !!columnMap && Object.keys(columnMap).length > 0;
      const looksTabular = fieldSpec.length > 0 || hasColumnMap || probeLooksTabular;
      const quotedNewlineLimit = looksTabular ? settings.CSV_MAX_QUOTED_NEWLINES : undefined;
      for await (const [line, byteOffset, byteLength] of streamLines(bucket, key, settings.FETCH_CHUNK_SIZE, detectedEncoding, quotedNewlineLimit)) {
        lineNo += 1;
        this.stats.totalLinesProcessed++;
        
        if (lineNo % 10000 === 0) {
          this.logger.info("parse_progress", { jobId, lineNo, parsed: counts.parsed, dropped: counts.dropped_rubbish, failed: totalFailed(counts) });
        }
        await drainIfReady();

        // Designed ordered classifier for EVERY line: length/binary gate -> learned record
        // templates -> structural recognizers (JSON / key-value, field_spec-only) -> rubbish
        // templates -> validated CSV. Junk is declined, not force-parsed.
        let result;
        if (circuitBreakerTripped) {
          result = { verdict: "uncertain", failure_class: FailureClass.UNCERTAIN };
        } else {
          try {
            result = classifier.classify(line, byteOffset, byteLength);
          } catch (lineError) {
            this.logger.error("line_classification_failed", { jobId, lineNo, error: lineError instanceof Error ? lineError.message : String(lineError) });
            counts.dropped_rubbish++;
            continue; // Skip this line and continue with next
          }
        }

        if (!circuitBreakerTripped && result.verdict === "uncertain") {
          uncertainCount++;
        }
        if (!circuitBreakerTripped && lineNo >= settings.UNCERTAIN_CIRCUIT_BREAKER_WINDOW && (uncertainCount / lineNo) > settings.UNCERTAIN_CIRCUIT_BREAKER_THRESHOLD) {
          circuitBreakerTripped = true;
          this.logger.warn("uncertain_rate_circuit_breaker_tripped", { job_id: jobId, line_no: lineNo, uncertain: uncertainCount, rate: uncertainCount / lineNo });
        }

        // Design step 4: a line the local classifier can't place (verdict "uncertain") is sent
        // to the AI ONCE — it returns a record template (parse it), a rubbish signature (drop it),
        // or "uncertain" (dead-letter for human review). The verdict is cached as a template so the
        // next matching line is handled locally with no further AI call. Bounded by a per-job
        // budget; when exhausted the file is flagged and remaining unknowns dead-lettered as before.
        if (result.verdict === "uncertain" && aiEnabled && !circuitBreakerTripped) {
          if (aiCalls < aiBudget) {
            aiCalls++;
            this.stats.totalAiCalls++;
            this.logger.info("ai_call_initiated", { job_id: jobId, line_no: lineNo, ai_call: aiCalls, ai_budget: aiBudget, context_lines: recentLines.slice(-3).length });
            try {
              await this.getAIRateLimiter().acquire();
              const aiResult = await classifier.classifyWithTimeout(line, recentLines.slice(-3), settings.AI_CLASSIFY_TIMEOUT_MS);
              this.logger.info("ai_call_completed", { job_id: jobId, line_no: lineNo, ai_call: aiCalls, verdict: aiResult.verdict, template_id: aiResult.template_id });
              if (aiResult.verdict !== "uncertain") {
                aiLocalRecoveries++;
                this.stats.totalAiRecoveries++;
                result = aiResult;
              } else {
                this.logger.info("ai_call_uncertain", { job_id: jobId, line_no: lineNo, ai_call: aiCalls });
              }
            } catch (aiErr) {
              this.logger.error("inline_ai_failed", { job_id: jobId, line_no: lineNo, ai_call: aiCalls, error: aiErr instanceof Error ? aiErr.message : String(aiErr) });
            }
          } else if (!aiBudgetFlagged) {
            aiBudgetFlagged = true;
            this.logger.warn("ai_budget_exhausted", { job_id: jobId, line_no: lineNo, ai_calls: aiCalls, budget: aiBudget, note: "file flagged; remaining unknowns dead-lettered" });
          }
        }

        // Keep a small rolling context window for the model (bounded memory).
        recentLines.push(line);
        if (recentLines.length > 5) recentLines.shift();

        if (lineNo <= 5) {
          this.logger.debug("classification_debug", { jobId, lineNo, verdict: result.verdict, template_id: result.template_id, line_length: line.length });
        }

        switch (result.verdict) {
          case "parsed": {
            // Sanitize row data before storage
            const sanitizedRow = this.sanitizeRecord(result.row || {});

            // Write-time guard: never emit a row whose email/phone is populated but invalid, no
            // matter which template produced it. Learned/AI templates can occasionally force junk
            // (e.g. a binary line) into a field; this choke-point drops it as rubbish.
            if (!classifier.rowStrongFieldsOk(sanitizedRow)) {
              counts.dropped_rubbish++;
              break;
            }

            // Add to output buffer (one record index shared by the parquet row and its trace)
            const idx = recordIndex++;
            const outputBuffer = outputManager.getBuffer(jobId, result.template_id || "default");
            outputBuffer.addRow({
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

            counts.parsed++;
            csvWriter.addRow(sanitizedRow, lineNo); // human-readable CSV mirror (best-effort)
            break;
          }

          case "rubbish": {
            // Sanitize line before storage
            const sanitizedLine = this.sanitizeForPg(line);
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

          case "uncertain": {
            // Sanitize line before storage
            const sanitizedUncertainLine = this.sanitizeForPg(line);
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
            if (!counts.failed_by_class[failureClass]) counts.failed_by_class[failureClass] = 0;
            counts.failed_by_class[failureClass]++;
            break;
          }
        }
      }

      await flushBatches(true);

      // Flush remaining output
      const outputPaths = await outputManager.flushAll();
      // Write the human-readable per-job CSV mirror (best-effort; Parquet stays authoritative)
      const csvOutputPath = await csvWriter.flush();
      if (csvOutputPath) {
        this.logger.info("csv_output_ready", { jobId, path: csvOutputPath, rows: counts.parsed });
      } else {
        this.logger.warn("csv_output_path_missing", { job_id: jobId, parsed: counts.parsed });
      }

      // Apply quality gate
      const qualityCheck = await qualityGate.passesQualityGate(jobId);
      if (!qualityCheck.passes) {
        this.logger.warn("quality_gate_failed", { job_id: jobId, reason: qualityCheck.reason });
        this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.FAILED, reason: qualityCheck.reason });
        return;
      }

      // Send to load service
      const failedTotal = totalFailed(counts);
      await publishEvent(makeJobEvent(EventType.PARSING_COMPLETED, jobId, "stream_parser", {
        parsed: counts.parsed,
        dropped_rubbish: counts.dropped_rubbish,
        failed: failedTotal,
        failed_by_class: counts.failed_by_class,
        part_s3_paths: outputPaths,
        dlq_count: failedTotal,
        rubbish_log_path: counts.rubbish_log_path,
        csv_output_path: csvOutputPath,
      }));

      const parseDuration = Date.now() - parseStartTime;
      this.logger.info("parse_complete", { 
        job_id: jobId, 
        parsed: counts.parsed, 
        dropped: counts.dropped_rubbish, 
        failed: totalFailed(counts),
        duration_ms: parseDuration,
        ai_calls: aiCalls,
        ai_recoveries: aiLocalRecoveries
      });
      metrics.set("parse.lines_parsed", counts.parsed);
      metrics.set("parse.lines_dropped", counts.dropped_rubbish);
      metrics.set("parse.lines_failed", totalFailed(counts));
      metrics.set("parse.duration_ms", parseDuration);
      metrics.set("parse.ai_calls", aiCalls);
    } catch (exc) {
      fatal = exc instanceof Error ? exc : new Error(String(exc));
      this.logger.error("parse_failed", { job_id: jobId }, fatal);
      metrics.increment("parse.error", 1);
      this.emit(jobId, EventType.ERROR_OCCURRED, { error: String(exc) });
    } finally {
      // Best-effort flush to preserve partial progress only on fatal errors
      if (fatal) {
        try {
          const outputPaths = await outputManager.flushAll();
          if (outputPaths.length > 0) {
            this.logger.warn("partial_flush_on_fatal", { job_id: jobId, output_paths: outputPaths.length });
          }
        } catch (flushErr) {
          this.logger.error("flush_failed", { job_id: jobId, error: String(flushErr) });
        }
        // Release the CSV temp file (no-op if already flushed on the success path).
        await csvWriter.flush().catch(() => {});
      }

      if (fatal) {
        this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.FAILED, error: String(fatal) });
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
  private async consumerLoop(): Promise<void> {
    await waitForDb();
    await templateRegistry.loadFromDatabase();
    this.logger.info("stream_parser_consumer_started");
    
    while (this.running) {
      const messages = await receiveMessages<ParseMessage>(
        settings.PARSE_QUEUE_URL,
        (body) => JSON.parse(body) as ParseMessage,
        1,
        5
      );
      
      for (const { payload, receiptHandle } of messages) {
        this.currentJob = this.parseJob(payload);
        try {
          await this.currentJob;
          await deleteMessage(settings.PARSE_QUEUE_URL, receiptHandle);
        } catch (exc) {
          const errorStr = String(exc);
          // Ack bad messages to prevent infinite retry loop
          if (errorStr.includes("Job") && (errorStr.includes("not found") || errorStr.includes("cannot transition"))) {
            this.logger.error("stream_parser_message_failed_ack", { job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" });
            metrics.increment("parse.message_error_ack", 1);
            await deleteMessage(settings.PARSE_QUEUE_URL, receiptHandle);
          } else {
            this.logger.error("stream_parser_message_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
            metrics.increment("parse.message_error", 1);
          }
        } finally {
          this.currentJob = null;
        }
      }
    }
    
    this.logger.info("stream_parser_consumer_stopped");
  }
}

// Backward compatibility: export singleton instance and function wrappers
const streamParserService = StreamParserService.getInstance();

// Backward compatibility wrappers
export async function parseJob(msg: ParseMessage): Promise<void> {
  return streamParserService.parseJob(msg);
}

// Auto-start the service when module is loaded
streamParserService.start().catch(err => {
  _moduleLogger.error("stream_parser_start_failed", { error: String(err) });
  process.exit(1);
});
