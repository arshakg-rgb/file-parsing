import { settings } from "../../shared/config.js";
import { EventType, JobEvent, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus, ParseMessage, FailureClass, JobCounts, totalFailed } from "../../shared/models/job.js";
import { receiveMessages, deleteMessage, publishEvent } from "../../shared/queueUtils.js";
import { parseGcsUrl, streamLines, objectSize, readRange } from "../../shared/gcsUtils.js";
import { LineClassifier } from "./classifier.js";
import { templateRegistry } from "../../shared/templateRegistry.js";
import { OutputManager } from "../../shared/parquetWriter.js";
import { DLQManager } from "../../shared/dlqManager.js";
import { TraceSystem } from "../../shared/traceSystem.js";
import { QualityGate } from "../../shared/qualityGate.js";
import { AdaptiveProbing } from "../../shared/probing.js";
import { createLogger } from "../../shared/logger.js";
import { metrics } from "../../shared/metrics.js";
import { startHealthCheckServer } from "../../shared/health.js";
import { waitForDb } from "../../shared/db.js";
import jschardet from "jschardet";

const logger = createLogger("stream_parser");

// AI Rate Limiter
class AIRateLimiter {
  private requests: number[] = [];
  private rpm: number;
  private burst: number;

  constructor(rpm: number, burst: number) {
    this.rpm = rpm;
    this.burst = burst;
  }

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
        logger.warn("ai_rate_limit_burst", { waitTime, currentRequests: this.requests.length, burst: this.burst });
        await new Promise(resolve => setTimeout(resolve, waitTime));
        this.requests = this.requests.filter(time => time > oneMinuteAgo);
      }
    }
    
    // Check RPM limit
    if (this.requests.length >= this.rpm) {
      const oldestRequest = this.requests[0];
      const waitTime = oldestRequest + 60000 - now;
      if (waitTime > 0) {
        logger.warn("ai_rate_limit_rpm", { waitTime, currentRequests: this.requests.length, rpm: this.rpm });
        await new Promise(resolve => setTimeout(resolve, waitTime));
        this.requests = this.requests.filter(time => time > oneMinuteAgo);
      }
    }
    
    this.requests.push(now);
    logger.debug("ai_rate_limit_acquired", { currentRequests: this.requests.length, rpm: this.rpm, burst: this.burst });
  }
}

const aiRateLimiter = new AIRateLimiter(settings.AI_RATE_LIMIT_RPM, settings.AI_RATE_LIMIT_BURST);

if (process.env.HEALTH_CHECK_PORT) {
  startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
}

function emit(jobId: string, eventType: EventType, data: Record<string, any>) {
  publishEvent(makeJobEvent(eventType, jobId, "stream_parser", data));
}

let running = true;
let currentJob: Promise<void> | null = null;

function shutdown(signal: string) {
  logger.warn("stream_parser_shutting_down", { signal });
  running = false;
  if (currentJob) {
    currentJob.then(() => process.exit(0)).catch(() => process.exit(1));
  } else {
    process.exit(0);
  }
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

export async function parseJob(msg: ParseMessage): Promise<void> {
  await templateRegistry.loadFromDatabase();

  const jobId = msg.job_id;
  emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.PARSING });
  logger.info("parse_start", { job_id: jobId, s3_url: msg.s3_url, size: msg.size });
  metrics.increment("parse.start", 1);

  const [bucket, key] = parseGcsUrl(msg.s3_url);
  
  // Parse field_spec if it's a JSON string
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

  // Adaptive probing to detect file structure
  const fileSize = msg.size || (await objectSize(bucket, key));
  const probing = new AdaptiveProbing();
  const probeCount = probing.calculateProbeCount(fileSize);
  const probeOffsets = probing.generateProbeOffsets(fileSize, probeCount);
  
  logger.info("adaptive_probing", { job_id: jobId, probe_count: probeCount, file_size: fileSize });
  metrics.increment("parse.probing_start", 1, { probe_count: String(probeCount) });

  let detectedEncoding = "utf-8";
  let avgRowWidth = 0;
  let maxRowWidth = 0;

  // Execute probes to detect encoding and row characteristics
  for (const offset of probeOffsets) {
    const endOffset = Math.min(offset + settings.PROBE_WINDOW_MIN_BYTES - 1, fileSize - 1);
    try {
      const buffer = await readRange(bucket, key, offset, endOffset);
      const detected = jschardet.detect(buffer);
      if (detected.encoding && detected.confidence > 0.9) {
        detectedEncoding = detected.encoding;
      }
      
      // Analyze row widths
      const content = buffer.toString('utf-8').replace(/\0/g, ''); // Remove null bytes
      const lines = content.split('\n').filter(line => line.trim());
      if (lines.length > 0) {
        const widths = lines.map(l => l.length);
        avgRowWidth = Math.max(avgRowWidth, widths.reduce((a, b) => a + b, 0) / widths.length);
        maxRowWidth = Math.max(maxRowWidth, ...widths);
      }
    } catch (err) {
      logger.warn("probe_failed", { job_id: jobId, offset, error: String(err) });
    }
  }

  logger.info("probing_complete", { 
    job_id: jobId, 
    encoding: detectedEncoding, 
    avg_row_width: avgRowWidth,
    max_row_width: maxRowWidth 
  });

  const recordTemplates = templateRegistry.getAllRecordTemplates();
  const rubbishTemplates = templateRegistry.getAllRubbishTemplates();
  const classifier = new LineClassifier(jobId, fieldSpec, recordTemplates, rubbishTemplates);
  const outputManager = new OutputManager();
  const dlqManager = new DLQManager();
  const traceSystem = new TraceSystem();
  const qualityGate = new QualityGate();

  const counts: JobCounts = { parsed: 0, dropped_rubbish: 0, failed_by_class: {} };
  let lineNo = 0;
  let recordIndex = 0;
  let fatal: any = null;

  try {
    for await (const [line, byteOffset, byteLength] of streamLines(bucket, key, settings.FETCH_CHUNK_SIZE, detectedEncoding)) {
      lineNo += 1;
      if (lineNo % 10000 === 0) {
        console.log("parse_progress", { jobId, lineNo, parsed: counts.parsed, dropped: counts.dropped_rubbish, failed: totalFailed(counts) });
      }

      // Classify line using ordered classifier
      const result = classifier.classify(line, byteOffset, byteLength);

      switch (result.verdict) {
        case "parsed":
          // Add to output buffer
          const outputBuffer = outputManager.getBuffer(jobId, result.template_id || "default");
          outputBuffer.addRow({
            ...result.row,
            _job_id: jobId,
            _byte_offset: byteOffset,
            _byte_length: byteLength,
            _record_index: recordIndex++,
            _line_no: lineNo,
            _template_id: result.template_id,
            _template_version: result.template_version ?? 1,
            _checksum: "",
            _parsed_at: new Date(),
            _part_id: "auto",
          });
          
          // Create trace record
          await traceSystem.createTrace({
            s3_url: msg.s3_url,
            byte_offset: byteOffset,
            byte_length: byteLength,
            record_index: recordIndex,
            line_no: lineNo,
            job_id: jobId,
            template_id: result.template_id || "default",
            template_version: result.template_version || 1,
            checksum: "",
            parsed_at: new Date(),
            part_id: "auto",
            row_data: result.row // Store actual parsed row data
          });
          
          counts.parsed++;
          break;

        case "rubbish":
          counts.dropped_rubbish++;
          await traceSystem.logRubbishDrop(
            jobId,
            byteOffset,
            lineNo,
            line,
            result.template_id || "unknown"
          );
          break;

        case "uncertain":
          // Add to DLQ for retry
          const failureClass = result.failure_class || FailureClass.UNCERTAIN;
          await dlqManager.addEntry(
            jobId,
            byteOffset,
            byteLength,
            lineNo,
            line,
            failureClass,
            result.failure_class || "Uncertain classification"
          );
          if (!counts.failed_by_class[failureClass]) counts.failed_by_class[failureClass] = 0;
          counts.failed_by_class[failureClass]++;
          break;
      }
    }

    // Flush any remaining output
    const outputPaths = await outputManager.flushAll();

    // Apply quality gate
    const qualityCheck = await qualityGate.passesQualityGate(jobId);
    if (!qualityCheck.passes) {
      logger.warn("quality_gate_failed", { job_id: jobId, reason: qualityCheck.reason });
      emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.FAILED, reason: qualityCheck.reason });
      return;
    }

    // Send to load service
    await publishEvent(makeJobEvent(EventType.PARSING_COMPLETED, jobId, "stream_parser", {
      parsed: counts.parsed,
      dropped_rubbish: counts.dropped_rubbish,
      failed: totalFailed(counts),
      part_s3_paths: outputPaths,
      dlq_count: counts.dlq_count || 0,
      rubbish_log_path: counts.rubbish_log_path,
    }));

    logger.info("parse_complete", { job_id: jobId, parsed: counts.parsed, dropped: counts.dropped_rubbish, failed: totalFailed(counts) });
    metrics.set("parse.lines_parsed", counts.parsed);
    metrics.set("parse.lines_dropped", counts.dropped_rubbish);
    metrics.set("parse.lines_failed", totalFailed(counts));
  } catch (exc) {
    fatal = exc;
    logger.error("parse_failed", { job_id: jobId }, exc instanceof Error ? exc : new Error(String(exc)));
    metrics.increment("parse.error", 1);
    emit(jobId, EventType.ERROR_OCCURRED, { error: String(exc) });
  } finally {
    if (fatal) {
      emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.FAILED, error: String(fatal) });
    }
  }
}

export async function consumerLoop(): Promise<void> {
  await waitForDb();
  await templateRegistry.loadFromDatabase();
  logger.info("stream_parser_consumer_started");
  while (running) {
    const messages = await receiveMessages<ParseMessage>(
      settings.PARSE_QUEUE_URL,
      (body) => JSON.parse(body) as ParseMessage,
      1,
      5
    );
    for (const { payload, receiptHandle } of messages) {
      currentJob = parseJob(payload);
      try {
        await currentJob;
        await deleteMessage(settings.PARSE_QUEUE_URL, receiptHandle);
      } catch (exc) {
        const errorStr = String(exc);
        // Ack bad messages to prevent infinite retry loop
        if (errorStr.includes("Job") && (errorStr.includes("not found") || errorStr.includes("cannot transition"))) {
          logger.error("stream_parser_message_failed_ack", { job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" });
          metrics.increment("parse.message_error_ack", 1);
          await deleteMessage(settings.PARSE_QUEUE_URL, receiptHandle);
        } else {
          logger.error("stream_parser_message_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
          metrics.increment("parse.message_error", 1);
        }
      } finally {
        currentJob = null;
      }
    }
  }
  logger.info("stream_parser_consumer_stopped");
}

consumerLoop();
