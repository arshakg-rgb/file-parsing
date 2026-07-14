import { settings } from "../../shared/config.js";
import { EventType, JobEvent, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus, ParseMessage, FailureClass, JobCounts, totalFailed } from "../../shared/models/job.js";
import { isRecord } from "../../shared/models/template.js";
import { receiveMessages, deleteMessage, publishEvent } from "../../shared/queueUtils.js";
import { parseGcsUrl, streamLines } from "../../shared/gcsUtils.js";
import * as templateRegistry from "../ai_classifier/templateRegistry.js";
import { LineClassifier } from "./classifier.js";
import { MatchRateMonitor } from "./matchRate.js";
import { ParquetWriterPool, RubbishLogWriter, DLQWriter } from "./parquetWriter.js";
import { createLogger } from "../../shared/logger.js";
import { metrics } from "../../shared/metrics.js";
import { startHealthCheckServer } from "../../shared/health.js";

const logger = createLogger("stream_parser");

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
  await templateRegistry.warmCache();

  const jobId = msg.job_id;
  emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.PARSING });
  logger.info("parse_start", { job_id: jobId, s3_url: msg.s3_url, size: msg.size });
  metrics.increment("parse.start", 1);

  const [bucket, key] = parseGcsUrl(msg.s3_url);

  const seedIds = new Set(msg.seed_template_ids || []);
  const allTemplates = templateRegistry.listAll();
  const seedTemplates = allTemplates.filter((t) => seedIds.has(t.template_id));
  const recordTemplates = allTemplates.filter((t) => isRecord(t));
  const rubbishTemplates = allTemplates.filter((t) => !isRecord(t));

  console.log("parse_templates", { jobId, allTemplates: allTemplates.length, recordTemplates: recordTemplates.length, rubbishTemplates: rubbishTemplates.length, fieldSpec: msg.field_spec });
  logger.debug("parse_templates", { job_id: jobId, all_templates: allTemplates.length, record_templates: recordTemplates.length, rubbish_templates: rubbishTemplates.length });

  const classifier = new LineClassifier(jobId, msg.field_spec, [...recordTemplates, ...seedTemplates.filter(isRecord)], rubbishTemplates);
  const monitor = new MatchRateMonitor();
  const parquetWriter = new ParquetWriterPool(jobId, settings.DATA_BUCKET, `outputs/${jobId}`);
  const rubbishWriter = new RubbishLogWriter(jobId, settings.DATA_BUCKET, `outputs/${jobId}`);
  const dlqWriter = new DLQWriter(jobId);

  const counts: JobCounts = { parsed: 0, dropped_rubbish: 0, failed_by_class: {} };
  const contextLines: string[] = [];
  let lineNo = 0;
  let fatal: any = null;

  try {
    for await (const [line, byteOffset, byteLength] of streamLines(bucket, key, settings.FETCH_CHUNK_SIZE, "utf-8")) {
      lineNo += 1;
      if (lineNo % 10000 === 0) {
        console.log("parse_progress", { jobId, lineNo, parsed: counts.parsed, dropped: counts.dropped_rubbish, failed: totalFailed(counts) });
      }

      let result = classifier.classify(line, byteOffset, byteLength);

      if (result.verdict === "uncertain") {
        try {
          const aiResult = await classifier.classifyWithTimeout(line, contextLines, 5000);
          if (aiResult.verdict === "parsed" || aiResult.verdict === "rubbish") {
            result = aiResult;
          }
        } catch (err) {
          console.error("ai_classification_failed", { jobId, byteOffset, error: String(err) });
        }
      }

      contextLines.push(line);
      if (contextLines.length > 10) contextLines.shift();

      if (result.verdict === "parsed") {
        const template = allTemplates.find((t) => t.template_id === result.template_id);
        parquetWriter.write(
          result.row || {},
          result.template_id || "unknown",
          template?.version || 1,
          byteOffset,
          byteLength,
          lineNo,
          line
        );
        counts.parsed += 1;
        monitor.record(result.template_id || "", true);
      } else if (result.verdict === "rubbish") {
        counts.dropped_rubbish += 1;
        rubbishWriter.write(byteOffset, lineNo, line, result.template_id || "unknown");
        monitor.record(result.template_id || "", true);
      } else {
        const failureClass = result.failure_class || FailureClass.UNCERTAIN;
        counts.failed_by_class[failureClass] = (counts.failed_by_class[failureClass] || 0) + 1;
        await dlqWriter.write(byteOffset, byteLength, lineNo, line, failureClass, `verdict: ${result.verdict}`);
        monitor.record("", false);
      }

      monitor.checkWindow();

      if (parquetWriter.bufferedBytes >= settings.RAM_FLUSH_WATERMARK) {
        try {
          await parquetWriter.flush();
        } catch (flushErr) {
          console.error("parquet_flush_failed", { jobId, lineNo, error: String(flushErr) });
        }
      }
    }
  } catch (exc) {
    fatal = exc;
    logger.error("parse_loop_error", { job_id: jobId, line_no: lineNo }, exc instanceof Error ? exc : new Error(String(exc)));
    metrics.increment("parse.loop_error", 1);
  }

  let rubbishLogPath: string | null = null;
  try {
    await parquetWriter.flush();
    rubbishLogPath = await rubbishWriter.flush();
  } catch (flushErr) {
    fatal = flushErr;
    logger.error("parse_flush_error", { job_id: jobId }, flushErr instanceof Error ? flushErr : new Error(String(flushErr)));
    metrics.increment("parse.flush_error", 1);
  }

  if (fatal) {
    logger.error("parse_failed", { job_id: jobId }, fatal instanceof Error ? fatal : new Error(String(fatal)));
    metrics.increment("parse.failed", 1);
    emit(jobId, EventType.ERROR_OCCURRED, { error: String(fatal) });
    return;
  }

  const outputPaths = parquetWriter.allPartPaths;
  const failedTotal = totalFailed(counts);

  emit(jobId, EventType.PARSING_COMPLETED, {
    job_id: jobId,
    parsed: counts.parsed,
    dropped_rubbish: counts.dropped_rubbish,
    failed: failedTotal,
    part_s3_paths: outputPaths,
    dlq_count: dlqWriter.getCounter(),
    rubbish_log_path: rubbishLogPath,
  });

  logger.info("parse_complete", { job_id: jobId, parsed: counts.parsed, dropped_rubbish: counts.dropped_rubbish, parts: outputPaths.length });
  metrics.set("parse.lines_parsed", counts.parsed);
  metrics.set("parse.lines_dropped", counts.dropped_rubbish);
  metrics.set("parse.parts_written", outputPaths.length);
}

export async function consumerLoop(): Promise<void> {
  await templateRegistry.warmCache();
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
        logger.error("stream_parser_message_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
        metrics.increment("parse.message_error", 1);
      } finally {
        currentJob = null;
      }
    }
  }
  logger.info("stream_parser_consumer_stopped");
}

consumerLoop();
