import { settings } from "../../shared/config.js";
import { DLQMessage, DLQStatus, FailureClass, JobStatus, LoadMessage } from "../../shared/models/job.js";
import { receiveMessages, deleteMessage, sendMessage } from "../../shared/queueUtils.js";
import { pool } from "../../shared/db.js";
import { ClassifyResult, LineClassifier } from "../stream_parser/classifier.js";
import * as templateRegistry from "../ai_classifier/templateRegistry.js";
import { createLogger } from "../../shared/logger.js";
import { metrics } from "../../shared/metrics.js";
import { startHealthCheckServer } from "../../shared/health.js";

const logger = createLogger("retry");

if (process.env.HEALTH_CHECK_PORT) {
  startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
}

const ALT_ENCODINGS = ["utf-8", "latin-1", "cp1252", "iso-8859-1", "utf-16"];

export async function handleDlqEntry(msg: DLQMessage): Promise<void> {
  await templateRegistry.warmCache();

  logger.info("retry_attempt", {
    job_id: msg.job_id,
    byte_offset: msg.byte_offset,
    failure_class: msg.failure_class,
    attempts: msg.attempts + 1,
  });
  metrics.increment("retry.attempt", 1, { failure_class: msg.failure_class });

  if (msg.dlq_id) {
    const row = await getDeadLetter(msg.dlq_id);
    if (!row || row.status !== "pending") {
      logger.debug("retry_skip_non_pending", { dlq_id: msg.dlq_id, status: row?.status });
      return;
    }
  }

  if (msg.attempts >= settings.RETRY_MAX_ATTEMPTS) {
    await markForReview(msg);
    return;
  }

  const rawBytes = Buffer.from(msg.raw_bytes, "base64");
  let recovered: ClassifyResult | null = null;

  if (msg.failure_class === FailureClass.ENCODING_ERROR) {
    recovered = await retryEncoding(rawBytes, msg);
  } else if ([FailureClass.TRANSFORM_ERROR, FailureClass.EXTRACTION_ERROR].includes(msg.failure_class)) {
    recovered = await retryAfterTemplateUpdate(rawBytes, msg);
  } else if (msg.failure_class === FailureClass.TYPE_MISMATCH) {
    recovered = await retryBroadCoercion(rawBytes, msg);
  } else if (msg.failure_class === FailureClass.UNCERTAIN) {
    await markForReview(msg);
    return;
  }

  if (recovered && recovered.verdict === "parsed" && recovered.row) {
    await emitRecovered(msg, recovered);
    logger.info("line_recovered", { job_id: msg.job_id, byte_offset: msg.byte_offset });
    metrics.increment("retry.recovered", 1, { failure_class: msg.failure_class });
  } else {
    const delay = msg.attempts >= 1 ? settings.RETRY_DELAYED_DELAY_SECONDS : 0;
    await reEnqueue(msg, delay);
  }
}

async function retryEncoding(rawBytes: Buffer, msg: DLQMessage): Promise<ClassifyResult | null> {
  for (const enc of ALT_ENCODINGS) {
    try {
      const line = new TextDecoder(enc, { fatal: true }).decode(rawBytes);
      return await classifyLine(line, msg);
    } catch {
      continue;
    }
  }
  return null;
}

async function retryAfterTemplateUpdate(rawBytes: Buffer, msg: DLQMessage): Promise<ClassifyResult | null> {
  const line = rawBytes.toString("utf-8", 0, rawBytes.length);
  return await classifyLine(line, msg);
}

async function retryBroadCoercion(rawBytes: Buffer, msg: DLQMessage): Promise<ClassifyResult | null> {
  const line = rawBytes.toString("utf-8", 0, rawBytes.length);
  const result = await classifyLine(line, msg);
  if (result && result.verdict === "parsed") return result;
  const fieldSpec = await getFieldSpec(msg.job_id);
  return { verdict: "parsed", row: Object.fromEntries(fieldSpec.map((f) => [f, null])), template_id: "coerced" };
}

async function classifyLine(line: string, msg: DLQMessage): Promise<ClassifyResult | null> {
  const fieldSpec = await getFieldSpec(msg.job_id);
  const allTemplates = templateRegistry.listAll();
  const classifier = new LineClassifier(
    msg.job_id,
    fieldSpec,
    allTemplates.filter((t) => t.kind === "record"),
    allTemplates.filter((t) => t.kind === "rubbish")
  );
  const result = classifier.classify(line, msg.byte_offset, msg.byte_length);
  if (result.verdict === "parsed") return result;

  const ai = await classifier.classifyWithAI(line, []);
  if (ai.verdict === "parsed") return ai;
  return null;
}

async function getFieldSpec(jobId: string): Promise<string[]> {
  const result = await pool.query<{ field_spec: string[] }>("SELECT field_spec FROM parse_jobs WHERE job_id = $1", [jobId]);
  return result.rows[0]?.field_spec || [];
}

interface DeadLetterRow {
  dlq_id: string;
  status: string;
}

async function getDeadLetter(dlqId: string): Promise<DeadLetterRow | undefined> {
  const result = await pool.query<DeadLetterRow>("SELECT dlq_id, status FROM dead_letters WHERE dlq_id = $1", [dlqId]);
  return result.rows[0];
}

async function updateDeadLetterStatus(dlqId: string | undefined, status: string, attempts?: number): Promise<void> {
  if (!dlqId) return;
  const fields = ["status = $2", "updated_at = NOW()"];
  const values: any[] = [dlqId, status];
  if (attempts !== undefined) {
    fields.push("attempts = $3");
    values.push(attempts);
  }
  await pool.query(`UPDATE dead_letters SET ${fields.join(", ")} WHERE dlq_id = $1`, values);
}

async function markForReview(msg: DLQMessage): Promise<void> {
  await updateDeadLetterStatus(msg.dlq_id, "review");
  logger.warn("line_marked_for_review", {
    job_id: msg.job_id,
    byte_offset: msg.byte_offset,
    failure_class: msg.failure_class,
    attempts: msg.attempts,
  });
  metrics.increment("retry.marked_for_review", 1, { failure_class: msg.failure_class });
}

async function emitRecovered(msg: DLQMessage, result: ClassifyResult): Promise<void> {
  await updateDeadLetterStatus(msg.dlq_id, "recovered");
  const loadMsg: LoadMessage = {
    job_id: msg.job_id,
    recovered_row: result.row,
    byte_offset: msg.byte_offset,
    byte_length: msg.byte_length,
    line_no: msg.line_no,
    template_id: result.template_id,
    template_version: result.template_version,
  };
  await sendMessage(settings.LOAD_QUEUE_URL, loadMsg, 0, msg.job_id);
}

async function reEnqueue(msg: DLQMessage, delaySeconds: number): Promise<void> {
  const nextAttempts = msg.attempts + 1;
  await updateDeadLetterStatus(msg.dlq_id, "pending", nextAttempts);
  const updated: DLQMessage = { ...msg, attempts: nextAttempts, status: "pending" };
  await sendMessage(settings.DLQ_QUEUE_URL, updated, delaySeconds, msg.job_id);
  logger.info("line_re_enqueued", {
    job_id: msg.job_id,
    attempts: updated.attempts,
    delay_s: delaySeconds,
  });
  metrics.increment("retry.re_enqueued", 1);
}

export async function consumerLoop(): Promise<void> {
  logger.info("retry_consumer_started");
  while (true) {
    const messages = await receiveMessages<DLQMessage>(
      settings.DLQ_QUEUE_URL,
      (body) => JSON.parse(body) as DLQMessage,
      10
    );
    for (const { payload, receiptHandle } of messages) {
      try {
        await handleDlqEntry(payload);
        await deleteMessage(settings.DLQ_QUEUE_URL, receiptHandle);
      } catch (exc) {
        logger.error("retry_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
        metrics.increment("retry.error", 1);
      }
    }
  }
}

consumerLoop();
