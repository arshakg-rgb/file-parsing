import { settings } from "../../shared/config.js";
import { EventType, JobEvent, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus, ParseMessage, FailureClass, JobCounts, totalFailed } from "../../shared/models/job.js";
import { receiveMessages, deleteMessage, publishEvent } from "../../shared/queueUtils.js";
import { parseGcsUrl, streamLines, objectSize, readRange } from "../../shared/gcsUtils.js";
import { LineClassifier } from "./classifier.js";
import { templateRegistry } from "../../shared/templateRegistry.js";
import { OutputManager } from "../../shared/parquetWriter.js";
import { CsvOutputWriter } from "../../shared/csvOutputWriter.js";
import { DLQManager } from "../../shared/dlqManager.js";
import { TraceSystem } from "../../shared/traceSystem.js";
import { QualityGate } from "../../shared/qualityGate.js";
import { AdaptiveProbing } from "../../shared/probing.js";
import { createLogger } from "../../shared/logger.js";
import { metrics } from "../../shared/metrics.js";
import { startHealthCheckServer } from "../../shared/health.js";
import { waitForDb } from "../../shared/db.js";
import jschardet from "jschardet";
import { normalizeEncoding, isLikelyUtf8 } from "../../shared/encoding.js";

const logger = createLogger("stream_parser");

/**
 * Sanitize text for PostgreSQL storage
 * - Strip null bytes (Postgres text/JSON columns reject \u0000)
 * - Escape lone/invalid \u sequences that aren't valid unicode
 */
function sanitizeForPg(str: string): string {
  return str
    .replace(/\u0000/g, '')
    .replace(/\\u(?![0-9a-fA-F]{4})/g, '\\\\u');
}

/**
 * Sanitize any value recursively - single source of truth for type handling
 */
function sanitizeValue(value: any): any {
  if (typeof value === 'string') return sanitizeForPg(value);
  if (Array.isArray(value)) return value.map(sanitizeValue);
  if (value instanceof Date) return value;
  if (typeof value === 'object' && value !== null) return sanitizeRecord(value);
  return value;
}

/**
 * Sanitize all string values in a record recursively
 * Handles nested objects, arrays, and Date objects correctly
 */
function sanitizeRecord(record: Record<string, any>): Record<string, any> {
  const sanitized: Record<string, any> = {};
  for (const [key, value] of Object.entries(record)) {
    sanitized[key] = sanitizeValue(value);
  }
  return sanitized;
}

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
  const columnMap = (msg as any).column_map || undefined;
  const classifier = new LineClassifier(jobId, fieldSpec, recordTemplates, rubbishTemplates, columnMap);
  const outputManager = new OutputManager();
  const csvWriter = new CsvOutputWriter(jobId, fieldSpec);
  const dlqManager = new DLQManager();
  const traceSystem = new TraceSystem();
  const qualityGate = new QualityGate();

  const counts: JobCounts = { parsed: 0, dropped_rubbish: 0, failed_by_class: {} };
  let lineNo = 0;
  let recordIndex = 0;
  let fatal: any = null;

  // Inline AI (design step 4): when the local ordered classifier can't decide, ask the model
  // once, cache its verdict as a template, and reuse it locally thereafter. Bounded per job.
  const aiMode = settings.AI_INLINE_MODE; // "off" | "mock" | "live"
  const aiEnabled = aiMode === "mock" || aiMode === "live";
  const aiBudget = settings.MAX_AI_CALLS_PER_JOB;
  let aiCalls = 0;
  let aiLocalRecoveries = 0; // unknowns the AI resolved (record or rubbish)
  let aiBudgetFlagged = false;
  let localMatches = 0; // lines the LOCAL classifier placed without AI (match-rate monitor)
  const recentLines: string[] = []; // small context window for the model
  const MATCH_RATE_FLOOR = 0.1; // design: flag the file if local-hit ratio collapses

  try {
    for await (const [line, byteOffset, byteLength] of streamLines(bucket, key, settings.FETCH_CHUNK_SIZE, detectedEncoding)) {
      lineNo += 1;
      if (lineNo % 10000 === 0) {
        console.log("parse_progress", { jobId, lineNo, parsed: counts.parsed, dropped: counts.dropped_rubbish, failed: totalFailed(counts) });
      }

      // Designed ordered classifier for EVERY line: length/binary gate -> learned record
      // templates -> structural recognizers (JSON / key-value, field_spec-only) -> rubbish
      // templates -> validated CSV. Junk is declined, not force-parsed.
      let result;
      try {
        result = classifier.classify(line, byteOffset, byteLength);
      } catch (lineError) {
        console.error("line_classification_failed", { jobId, lineNo, error: lineError instanceof Error ? lineError.message : String(lineError) });
        counts.dropped_rubbish++;
        continue; // Skip this line and continue with next
      }

      // Match-rate monitor (design): a non-uncertain local verdict = a local hit (template or
      // structural recognizer matched, no AI needed). The collapse of this ratio is what flags
      // a pathological file at end-of-parse rather than quietly hammering AI.
      if (result.verdict !== "uncertain") localMatches++;

      // Design step 4: a line the local classifier can't place (verdict "uncertain") is sent
      // to the AI ONCE — it returns a record template (parse it), a rubbish signature (drop it),
      // or "uncertain" (dead-letter for human review). The verdict is cached as a template so the
      // next matching line is handled locally with no further AI call. Bounded by a per-job
      // budget; when exhausted the file is flagged and remaining unknowns dead-letter as before.
      if (result.verdict === "uncertain" && aiEnabled) {
        if (aiCalls < aiBudget) {
          aiCalls++;
          try {
            const aiResult = await classifier.classifyWithTimeout(line, recentLines.slice(-3), settings.AI_CLASSIFY_TIMEOUT_MS);
            if (aiResult.verdict !== "uncertain") {
              aiLocalRecoveries++;
              result = aiResult;
            }
          } catch (aiErr) {
            console.error("inline_ai_failed", { jobId, lineNo, error: aiErr instanceof Error ? aiErr.message : String(aiErr) });
          }
        } else if (!aiBudgetFlagged) {
          aiBudgetFlagged = true;
          console.log("ai_budget_exhausted", { jobId, lineNo, ai_calls: aiCalls, budget: aiBudget, note: "file flagged; remaining unknowns dead-lettered" });
        }
      }

      // Keep a small rolling context window for the model (bounded memory).
      recentLines.push(line);
      if (recentLines.length > 5) recentLines.shift();

      if (lineNo <= 5) {
        console.log("classification_debug", { jobId, lineNo, verdict: result.verdict, template_id: result.template_id, line_length: line.length });
      }

      switch (result.verdict) {
        case "parsed":
          // Sanitize row data before storage
          const sanitizedRow = sanitizeRecord(result.row || {});

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

          // Create trace record - wrap in try/catch
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
              row_data: sanitizedRow // Store sanitized row data
            });
            counts.parsed++;
            csvWriter.addRow(sanitizedRow, lineNo); // human-readable CSV mirror (best-effort)
          } catch (traceErr) {
            console.error("trace_write_failed", { jobId, lineNo, error: traceErr instanceof Error ? traceErr.message : String(traceErr) });
            counts.dropped_rubbish++;
          }
          break;

        case "rubbish":
          // Sanitize line before storage
          const sanitizedLine = sanitizeForPg(line);
          
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
          // Sanitize line before storage
          const sanitizedUncertainLine = sanitizeForPg(line);
          
          // Add to DLQ for retry - wrap in try/catch
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

    // Flush any remaining output
    const outputPaths = await outputManager.flushAll();
    // Write the human-readable per-job CSV mirror (best-effort; Parquet stays authoritative)
    const csvOutputPath = await csvWriter.flush();
    if (csvOutputPath) console.log("csv_output_ready", { jobId, path: csvOutputPath, rows: counts.parsed });

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

    const localHitRate = lineNo > 0 ? localMatches / lineNo : 1;
    const matchRateFlagged = lineNo >= 50 && localHitRate < MATCH_RATE_FLOOR;
    if (matchRateFlagged) {
      logger.warn("match_rate_collapsed", { job_id: jobId, local_hit_rate: Number(localHitRate.toFixed(3)), lines: lineNo, ai_calls: aiCalls, note: "templates rarely matched — file flagged for review" });
    }
    logger.info("parse_complete", { job_id: jobId, parsed: counts.parsed, dropped: counts.dropped_rubbish, failed: totalFailed(counts), ai_calls: aiCalls, ai_recoveries: aiLocalRecoveries, local_hit_rate: Number(localHitRate.toFixed(3)), match_rate_flagged: matchRateFlagged });
    metrics.set("parse.lines_parsed", counts.parsed);
    metrics.set("parse.lines_dropped", counts.dropped_rubbish);
    metrics.set("parse.lines_failed", totalFailed(counts));
  } catch (exc) {
    fatal = exc;
    logger.error("parse_failed", { job_id: jobId }, exc instanceof Error ? exc : new Error(String(exc)));
    metrics.increment("parse.error", 1);
    emit(jobId, EventType.ERROR_OCCURRED, { error: String(exc) });
  } finally {
    // Best-effort flush to preserve partial progress only on fatal errors
    if (fatal) {
      try {
        const outputPaths = await outputManager.flushAll();
        if (outputPaths.length > 0) {
          logger.warn("partial_flush_on_fatal", { job_id: jobId, output_paths: outputPaths.length });
        }
      } catch (flushErr) {
        logger.error("flush_failed", { job_id: jobId, error: String(flushErr) });
      }
      // Release the CSV temp file (no-op if already flushed on the success path).
      await csvWriter.flush().catch(() => {});
    }

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
