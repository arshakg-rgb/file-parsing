import { settings } from "../../shared/config.js";
import { EventType, JobEvent, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus, SourceType, IngestMessage } from "../../shared/models/job.js";
import { receiveMessages, deleteMessage, sendRaw, publishEvent } from "../../shared/queueUtils.js";
import { parseGcsUrl, objectSize, readRange } from "../../shared/gcsUtils.js";
import { getJob, pool, waitForDb } from "../../shared/db.js";
import { detectArchiveType, extractArchiveToS3, fetchUrlToS3, listS3Prefix, BombError } from "./normalizer.js";
import { SSRFError } from "./ssrf_guard.js";
import { createLogger } from "../../shared/logger.js";
import { metrics } from "../../shared/metrics.js";
import { startHealthCheckServer } from "../../shared/health.js";

const logger = createLogger("ingest");

if (process.env.HEALTH_CHECK_PORT) {
  startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
}

const _passwordCache = new Map<string, Buffer>();
const _passwordAttempts = new Map<string, number>();

class PasswordError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PasswordError";
  }
}

function emit(jobId: string, eventType: EventType, data: Record<string, any>) {
  publishEvent(makeJobEvent(eventType, jobId, "ingest", data));
}

function transition(jobId: string, newStatus: JobStatus, error?: string) {
  emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: newStatus, ...(error ? { error } : {}) });
}

export async function handleIngest(msg: IngestMessage): Promise<void> {
  const jobId = msg.job_id;
  transition(jobId, JobStatus.INGESTING);
  logger.info("ingest_start", { job_id: jobId, source_type: msg.source_type });
  metrics.increment("ingest.start", 1, { source_type: msg.source_type });

  try {
    const resolved = await resolveSource(msg);
    if (!resolved) {
      if (msg.source_type === SourceType.S3 && msg.source_ref.endsWith("/")) {
        transition(jobId, JobStatus.DONE);
      }
      return;
    }
    const { s3Url, size } = resolved;

    try {
      await pool.query("UPDATE parse_jobs SET s3_url = $1, size = $2, updated_at = NOW() WHERE job_id = $3", [s3Url, size, jobId]);
    } catch (e) {
      logger.warn("ingest_s3_url_update_failed", { job_id: jobId, error: String(e) });
    }

    const [bucket, key] = parseGcsUrl(s3Url);
    const header = await readRange(bucket, key, 0, 511);
    const archiveType = detectArchiveType(header);

    if (archiveType) {
      await handleArchive(jobId, s3Url, archiveType, msg, size);
      return;
    }

    await sendRaw(settings.CLASSIFY_QUEUE_URL, {
      job_id: jobId,
      s3_url: s3Url,
      size,
      field_spec: msg.field_spec,
    });
    logger.info("ingest_forwarded_to_classify", { job_id: jobId, s3_url: s3Url });
    metrics.increment("ingest.forwarded", 1, { target: "classify" });
  } catch (exc) {
    if (exc instanceof SSRFError) {
      logger.error("ssrf_blocked", { job_id: jobId }, exc);
      metrics.increment("ingest.ssrf_blocked", 1);
      transition(jobId, JobStatus.FAILED, `SSRF blocked: ${exc}`);
    } else if (exc instanceof PasswordError) {
      const attempts = _passwordAttempts.get(jobId) || 0;
      if (attempts >= settings.ARCHIVE_PASSWORD_MAX_ATTEMPTS) {
        logger.error("archive_password_exhausted", { job_id: jobId, attempts });
        metrics.increment("ingest.password_exhausted", 1);
        transition(jobId, JobStatus.FAILED, `password_unavailable: ${exc}`);
      } else {
        _passwordAttempts.set(jobId, attempts + 1);
        logger.info("archive_password_required", { job_id: jobId, attempts: attempts + 1 });
        transition(jobId, JobStatus.AWAITING_PASSWORD);
      }
    } else {
      logger.error("ingest_error", { job_id: jobId }, exc instanceof Error ? exc : new Error(String(exc)));
      metrics.increment("ingest.error", 1);
      transition(jobId, JobStatus.FAILED, String(exc));
    }
  }
}

async function resolveSource(msg: IngestMessage): Promise<{ s3Url: string; size: number } | null> {
  if (msg.source_type === SourceType.S3) {
    const url = msg.source_ref;
    if (url.endsWith("/")) {
      const objects = await listS3Prefix(url);
      for (const [objUrl, objSize] of objects) {
        await publishEvent(makeJobEvent(EventType.ENTRY_DISCOVERED, msg.job_id, "ingest", {
          parent_job_id: msg.job_id,
          batch_id: msg.batch_id || msg.job_id,
          entry_s3_url: objUrl,
          entry_name: objUrl,
          entry_size: objSize,
          field_spec: msg.field_spec,
          source_type: SourceType.S3,
        }));
      }
      logger.info("s3_prefix_fanout", { job_id: msg.job_id, count: objects.length });
      metrics.increment("ingest.prefix_fanout", objects.length);
      return null;
    }
    const [bucket, key] = parseGcsUrl(url);
    const size = await objectSize(bucket, key);
    return { s3Url: url, size };
  } else if (msg.source_type === SourceType.URL) {
    const [s3Url, size] = await fetchUrlToS3(msg.job_id, msg.source_ref);
    return { s3Url, size };
  } else if ([SourceType.UPLOAD, SourceType.ARCHIVE_ENTRY].includes(msg.source_type)) {
    const [bucket, key] = parseGcsUrl(msg.source_ref);
    logger.debug("upload_source_debug", { job_id: msg.job_id, bucket, key, source_ref: msg.source_ref });
    const size = await objectSize(bucket, key);
    return { s3Url: msg.source_ref, size };
  }
  throw new Error(`Unknown source_type: ${msg.source_type}`);
}

async function handleArchive(
  jobId: string,
  s3Url: string,
  archiveType: string,
  msg: IngestMessage,
  compressedSize: number
): Promise<void> {
  const password = msg.password ?? _passwordCache.get(jobId)?.toString();
  try {
    const entries = await extractArchiveToS3(jobId, s3Url, archiveType, msg.field_spec, msg.batch_id || jobId, password);
    if (!entries.length) {
      logger.warn("archive_empty", { job_id: jobId, s3_url: s3Url });
      metrics.increment("ingest.archive_empty", 1);
      transition(jobId, JobStatus.FAILED, "Archive contained no extractable files");
      return;
    }
    for (const entry of entries) {
      await publishEvent(makeJobEvent(EventType.ENTRY_DISCOVERED, jobId, "ingest", entry));
    }
    logger.info("archive_extracted", { job_id: jobId, entries: entries.length });
    metrics.increment("ingest.archive_extracted", entries.length);
    transition(jobId, JobStatus.DONE);
  } catch (exc) {
    const errStr = String(exc).toLowerCase();
    if (errStr.includes("password") || errStr.includes("encrypted") || errStr.includes("bad password")) {
      throw new PasswordError(String(exc));
    }
    if (exc instanceof BombError) {
      logger.error("archive_bomb_detected", { job_id: jobId }, exc);
      metrics.increment("ingest.archive_bomb", 1);
      transition(jobId, JobStatus.FAILED, `Archive bomb: ${exc}`);
      return;
    }
    logger.error("archive_extraction_failed", { job_id: jobId }, exc instanceof Error ? exc : new Error(String(exc)));
    metrics.increment("ingest.archive_error", 1);
    transition(jobId, JobStatus.FAILED, String(exc));
  }
}

export async function handlePassword(jobId: string, password: string): Promise<void> {
  _passwordCache.set(jobId, Buffer.from(password));
  logger.info("password_received", { job_id: jobId });

  const row = await getJob(jobId);
  if (!row) {
    logger.error("password_job_not_found", { job_id: jobId });
    return;
  }

  await sendRaw(settings.INGEST_QUEUE_URL, {
    job_id: jobId,
    source_type: row.source_type,
    source_ref: row.source_ref,
    field_spec: Array.isArray(row.field_spec) ? row.field_spec : [],
    batch_id: row.batch_id,
    password,
  });
}

export async function consumerLoop(): Promise<void> {
  while (true) {
    try {
      await waitForDb();
      logger.info("ingest_consumer_started");
      
      while (true) {
        const messages = await receiveMessages<IngestMessage>(
          settings.INGEST_QUEUE_URL,
          (body) => JSON.parse(body) as IngestMessage,
          5
        );
        for (const { payload, receiptHandle } of messages) {
          try {
            if ((payload as any).action === "provide_password") {
              await handlePassword(payload.job_id, (payload as any).password);
            } else {
              await handleIngest(payload);
            }
            await deleteMessage(settings.INGEST_QUEUE_URL, receiptHandle);
          } catch (exc) {
            const errorStr = String(exc);
            // Ack bad messages to prevent infinite retry loop
            if ((errorStr.includes("Job") && errorStr.includes("not found")) || errorStr.includes("cannot transition")) {
              logger.error("ingest_message_failed_ack", { job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" });
              metrics.increment("ingest.message_error_ack", 1);
              await deleteMessage(settings.INGEST_QUEUE_URL, receiptHandle);
            } else {
              logger.error("ingest_message_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
              metrics.increment("ingest.message_error", 1);
            }
          }
        }
      }
    } catch (dbError) {
      logger.error("database_connection_lost", { error: String(dbError) }, dbError instanceof Error ? dbError : new Error(String(dbError)));
      metrics.increment("ingest.db_connection_lost", 1);
      // Wait before retrying to avoid tight loop
      await new Promise(r => setTimeout(r, 10000));
    }
  }
}

consumerLoop();
