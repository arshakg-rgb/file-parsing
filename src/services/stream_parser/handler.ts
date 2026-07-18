import Config from "../../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../../config/ServiceManager.js";
import { InstantiationError } from "../../errors/InstantiationError.js";
import FirestoreCacheUtils from "../../utils/cache/FirestoreCacheUtils.js";
import MySqlManager from "../../config/db/MySqlManager.js";
import { EventType, JobEvent, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus, ParseMessage, FailureClass, JobCounts, totalFailed } from "../../shared/models/job.js";
import { receiveMessages, deleteMessage, publishEvent } from "../../shared/queueUtils.js";
import { LineClassifier } from "./classifier.js";
import { templateRegistry } from "../../shared/templateRegistry.js";
import { OutputManager } from "../../shared/parquetWriter.js";
import { CsvOutputWriter } from "../../shared/csvOutputWriter.js";
import { DLQManager } from "../../shared/dlqManager.js";
import { TraceSystem } from "../../shared/traceSystem.js";
import { QualityGate } from "../../shared/qualityGate.js";
import { AdaptiveProbing } from "../../shared/probing.js";
import { createLogger } from "../../utils/logger/logger.js";
import { metrics } from "../../utils/response/metrics.js";
import { startHealthCheckServer } from "../../utils/response/health.js";
import { AIRateLimiter } from "../../utils/AIRateLimiter.js";
import jschardet from "jschardet";
import { normalizeEncoding, isLikelyUtf8 } from "../../utils/normalizers/encoding.js";

class StreamParserService extends ServiceManager {
  protected static instance: StreamParserService;
  private logger: any;
  private gcsUtils: FirestoreCacheUtils;
  private dbManager: MySqlManager;
  private aiRateLimiter: AIRateLimiter;
  private running = true;
  private currentJob: Promise<void> | null = null;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate StreamParserService directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("stream_parser");
    this.gcsUtils = FirestoreCacheUtils.getInstance();
    this.dbManager = MySqlManager.getInstance();
    
    const config = this.getConfig();
    this.aiRateLimiter = new AIRateLimiter(config.settings.AI_RATE_LIMIT_RPM, config.settings.AI_RATE_LIMIT_BURST);
    
    if (process.env.HEALTH_CHECK_PORT) {
      startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
    }
    
    this.setupShutdownHandlers();
  }

  public static getInstance(): StreamParserService {
    if (!StreamParserService.instance) {
      StreamParserService.instance = new StreamParserService(Enforce);
    }
    return StreamParserService.instance;
  }

  public getLogger(): any {
    return this.logger;
  }

  public getGcsUtils(): FirestoreCacheUtils {
    return this.gcsUtils;
  }

  public getDbManager(): MySqlManager {
    return this.dbManager;
  }

  private setupShutdownHandlers(): void {
    const shutdown = (signal: string) => {
      this.logger.warn("stream_parser_shutting_down", { signal });
      this.running = false;
      if (this.currentJob) {
        this.currentJob.then(() => process.exit(0)).catch(() => process.exit(1));
      } else {
        process.exit(0);
      }
    };
    process.on("SIGTERM", () => shutdown("SIGTERM"));
    process.on("SIGINT", () => shutdown("SIGINT"));
  }

  private sanitizeForPg(str: string): string {
    return str
      .replace(/\u0000/g, '')
      .replace(/\\u(?![0-9a-fA-F]{4})/g, '\\\\u');
  }

  private sanitizeValue(value: any): any {
    if (typeof value === 'string') return this.sanitizeForPg(value);
    if (Array.isArray(value)) return value.map((v: any) => this.sanitizeValue(v));
    if (value instanceof Date) return value;
    if (typeof value === 'object' && value !== null) return this.sanitizeRecord(value);
    return value;
  }

  private sanitizeRecord(record: Record<string, any>): Record<string, any> {
    const sanitized: Record<string, any> = {};
    for (const [key, value] of Object.entries(record)) {
      sanitized[key] = this.sanitizeValue(value);
    }
    return sanitized;
  }

  private emit(jobId: string, eventType: EventType, data: Record<string, any>) {
    publishEvent(makeJobEvent(eventType, jobId, "stream_parser", data));
  }

  public async parseJob(msg: ParseMessage): Promise<void> {
    await templateRegistry.loadFromDatabase();

    const jobId = msg.job_id;
    this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.PARSING });
    this.logger.info("parse_start", { job_id: jobId, s3_url: msg.s3_url, size: msg.size });
    metrics.increment("parse.start", 1);

    const [bucket, key] = this.gcsUtils.parseGcsUrl(msg.s3_url);
    
    let fieldSpec: string[] = [];
    if (typeof msg.field_spec === 'string') {
      try {
        fieldSpec = JSON.parse(msg.field_spec);
      } catch {
        fieldSpec = [];
      }
    } else {
      fieldSpec = msg.field_spec;
    }

    const fileSize = msg.size || (await this.gcsUtils.objectSize(bucket, key));
    const probing = new AdaptiveProbing();
    const probeCount = probing.calculateProbeCount(fileSize);
    const probeOffsets = probing.generateProbeOffsets(fileSize, probeCount);
    
    this.logger.info("adaptive_probing", { job_id: jobId, probe_count: probeCount, file_size: fileSize });
    metrics.increment("parse.probing_start", 1, { probe_count: String(probeCount) });

    let detectedEncoding = "utf-8";
    let avgRowWidth = 0;
    let maxRowWidth = 0;
    const config = this.getConfig();

    for (const offset of probeOffsets) {
      const endOffset = Math.min(offset + config.settings.PROBE_WINDOW_MIN_BYTES - 1, fileSize - 1);
      try {
        const buffer = await this.gcsUtils.readRange(bucket, key, offset, endOffset);
        if (isLikelyUtf8(buffer)) {
          detectedEncoding = "utf-8";
        } else {
          const detected = jschardet.detect(buffer);
          if (detected.encoding && detected.confidence > 0.9) {
            detectedEncoding = normalizeEncoding(detected.encoding);
          }
        }
        
        const content = buffer.toString('utf-8').replace(/\0/g, '');
        const lines = content.split('\n').filter(line => line.trim());
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
    const classifier = new LineClassifier(jobId, fieldSpec, recordTemplates, rubbishTemplates);
    const outputManager = new OutputManager();
    const csvWriter = new CsvOutputWriter(jobId, fieldSpec);
    const dlqManager = new DLQManager();
    const traceSystem = new TraceSystem();
    const qualityGate = new QualityGate();

    const counts: JobCounts = { parsed: 0, dropped_rubbish: 0, failed_by_class: {} };
    let lineNo = 0;
    let recordIndex = 0;
    let fatal: any = null;

    try {
      for await (const [line, byteOffset, byteLength] of this.gcsUtils.streamLines(bucket, key, config.settings.FETCH_CHUNK_SIZE, detectedEncoding)) {
        lineNo += 1;
        if (lineNo % 10000 === 0) {
          console.log("parse_progress", { jobId, lineNo, parsed: counts.parsed, dropped: counts.dropped_rubbish, failed: totalFailed(counts) });
        }

        let result;
        try {
          result = classifier.classify(line, byteOffset, byteLength);
        } catch (lineError) {
          console.error("line_classification_failed", { jobId, lineNo, error: lineError instanceof Error ? lineError.message : String(lineError) });
          counts.dropped_rubbish++;
          continue;
        }

        if (lineNo <= 5) {
          console.log("classification_debug", { jobId, lineNo, verdict: result.verdict, template_id: result.template_id, line_length: line.length });
        }

        switch (result.verdict) {
          case "parsed":
            const sanitizedRow = this.sanitizeRecord(result.row || {});
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

            try {
              await traceSystem.createTrace({
                s3_url: msg.s3_url,
                byte_offset: byteOffset,
                byte_length: byteLength,
                record_index: idx,
                line_no: lineNo,
                job_id: jobId,
                template_id: result.template_id || "default",
                template_version: result.template_version || 1,
                checksum: "",
                parsed_at: new Date(),
                part_id: "auto",
                row_data: sanitizedRow
              });
              counts.parsed++;
              csvWriter.addRow(sanitizedRow, lineNo);
            } catch (traceErr) {
              console.error("trace_write_failed", { jobId, lineNo, error: traceErr instanceof Error ? traceErr.message : String(traceErr) });
              counts.dropped_rubbish++;
            }
            break;

          case "rubbish":
            const sanitizedLine = this.sanitizeForPg(line);
            try {
              await traceSystem.logRubbishDrop(
                jobId,
                byteOffset,
                lineNo,
                sanitizedLine,
                result.template_id || "unknown"
              );
              counts.dropped_rubbish++;
            } catch (rubbishErr) {
              console.error("rubbish_log_failed", { jobId, lineNo, error: rubbishErr instanceof Error ? rubbishErr.message : String(rubbishErr) });
              counts.dropped_rubbish++;
            }
            break;

          case "uncertain":
            const sanitizedUncertainLine = this.sanitizeForPg(line);
            const failureClass = result.failure_class || FailureClass.UNCERTAIN;
            try {
              const dlqId = await dlqManager.addEntry(
                jobId,
                byteOffset,
                byteLength,
                lineNo,
                sanitizedUncertainLine,
                failureClass,
                result.failure_class || "Uncertain classification"
              );
              if (dlqId) {
                if (!counts.failed_by_class[failureClass]) counts.failed_by_class[failureClass] = 0;
                counts.failed_by_class[failureClass]++;
              }
            } catch (dlqErr) {
              console.error("dlq_add_failed", { jobId, lineNo, error: dlqErr instanceof Error ? dlqErr.message : String(dlqErr) });
              counts.dropped_rubbish++;
            }
            break;
        }
      }

      const outputPaths = await outputManager.flushAll();
      const csvOutputPath = await csvWriter.flush();
      if (csvOutputPath) console.log("csv_output_ready", { jobId, path: csvOutputPath, rows: counts.parsed });

      const qualityCheck = await qualityGate.passesQualityGate(jobId);
      if (!qualityCheck.passes) {
        this.logger.warn("quality_gate_failed", { job_id: jobId, reason: qualityCheck.reason });
        this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.FAILED, reason: qualityCheck.reason });
        return;
      }

      await publishEvent(makeJobEvent(EventType.PARSING_COMPLETED, jobId, "stream_parser", {
        parsed: counts.parsed,
        dropped_rubbish: counts.dropped_rubbish,
        failed: totalFailed(counts),
        part_s3_paths: outputPaths,
        dlq_count: counts.dlq_count || 0,
        rubbish_log_path: counts.rubbish_log_path,
      }));

      this.logger.info("parse_complete", { job_id: jobId, parsed: counts.parsed, dropped: counts.dropped_rubbish, failed: totalFailed(counts) });
      metrics.set("parse.lines_parsed", counts.parsed);
      metrics.set("parse.lines_dropped", counts.dropped_rubbish);
      metrics.set("parse.lines_failed", totalFailed(counts));
    } catch (exc) {
      fatal = exc;
      this.logger.error("parse_failed", { job_id: jobId }, exc instanceof Error ? exc : new Error(String(exc)));
      metrics.increment("parse.error", 1);
      this.emit(jobId, EventType.ERROR_OCCURRED, { error: String(exc) });
    } finally {
      if (fatal) {
        try {
          const outputPaths = await outputManager.flushAll();
          if (outputPaths.length > 0) {
            this.logger.warn("partial_flush_on_fatal", { job_id: jobId, output_paths: outputPaths.length });
          }
        } catch (flushErr) {
          this.logger.error("flush_failed", { job_id: jobId, error: String(flushErr) });
        }
        await csvWriter.flush().catch(() => {});
      }

      if (fatal) {
        this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.FAILED, error: String(fatal) });
      }
    }
  }

  public async consumerLoop(): Promise<void> {
    await this.dbManager.initialize();
    await templateRegistry.loadFromDatabase();
    this.logger.info("stream_parser_consumer_started");
    const config = this.getConfig();
    while (this.running) {
      const messages = await receiveMessages<ParseMessage>(
        config.settings.PARSE_QUEUE_URL,
        (body) => JSON.parse(body) as ParseMessage,
        1,
        5
      );
      for (const { payload, receiptHandle } of messages) {
        this.currentJob = this.parseJob(payload);
        try {
          await this.currentJob;
          await deleteMessage(config.settings.PARSE_QUEUE_URL, receiptHandle);
        } catch (exc) {
          const errorStr = String(exc);
          if (errorStr.includes("Job") && (errorStr.includes("not found") || errorStr.includes("cannot transition"))) {
            this.logger.error("stream_parser_message_failed_ack", { job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" });
            metrics.increment("parse.message_error_ack", 1);
            await deleteMessage(config.settings.PARSE_QUEUE_URL, receiptHandle);
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


export default StreamParserService;

// Backward compatibility wrappers
const streamParserService = StreamParserService.getInstance();

export async function parseJob(msg: ParseMessage): Promise<void> {
  return streamParserService.parseJob(msg);
}

export async function consumerLoop(): Promise<void> {
  return streamParserService.consumerLoop();
}
