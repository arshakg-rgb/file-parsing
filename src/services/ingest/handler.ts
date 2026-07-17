import Config from "../../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../../config/ServiceManager.js";
import { InstantiationError } from "../../errors/InstantiationError.js";
import FirestoreCacheUtils from "../../utils/cache/FirestoreCacheUtils.js";
import { EventType, JobEvent, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus, SourceType, IngestMessage } from "../../shared/models/job.js";
import { receiveMessages, deleteMessage, sendRaw, publishEvent } from "../../shared/queueUtils.js";
import { detectArchiveType, extractArchiveToS3, fetchUrlToS3, listS3Prefix, BombError } from "./normalizer.js";
import { SSRFError } from "./ssrf_guard.js";
import { createLogger } from "../../shared/logger.js";
import { metrics } from "../../shared/metrics.js";
import { startHealthCheckServer } from "../../shared/health.js";
import MySqlManager from "../../config/db/MySqlManager.js";
import path from "path";

class IngestService extends ServiceManager {
  protected static instance: IngestService;
  private logger: any;
  private gcsUtils: FirestoreCacheUtils;
  private dbManager: MySqlManager;
  private passwordCache: Map<string, Buffer>;
  private passwordAttempts: Map<string, number>;
  private EXTRACTION_TIMEOUT_MS = 50 * 60 * 1000;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate IngestService directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("ingest");
    this.gcsUtils = FirestoreCacheUtils.getInstance();
    this.dbManager = MySqlManager.getInstance();
    this.passwordCache = new Map<string, Buffer>();
    this.passwordAttempts = new Map<string, number>();
    
    if (process.env.HEALTH_CHECK_PORT) {
      startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
    }
  }

  public static getInstance(): IngestService {
    if (!ServiceManager.instance) {
      ServiceManager.instance = new IngestService(Enforce);
    }
    return ServiceManager.instance as IngestService;
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

  private withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
    let timer: NodeJS.Timeout;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
  }

  private emit(jobId: string, eventType: EventType, data: Record<string, any>) {
    publishEvent(makeJobEvent(eventType, jobId, "ingest", data));
  }

  private transition(jobId: string, newStatus: JobStatus, error?: string) {
    this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: newStatus, ...(error ? { error } : {}) });
  }

  public async handleIngest(msg: IngestMessage): Promise<void> {
    const jobId = msg.job_id;
    
    this.logger.info("ingest_start", { job_id: jobId, source_type: msg.source_type });
    metrics.increment("ingest.start", 1, { source_type: msg.source_type });

    try {
      this.transition(jobId, JobStatus.INGESTING);
      
      const resolved = await this.resolveSource(msg);
      if (!resolved) {
        if (msg.source_type === SourceType.S3 && msg.source_ref.endsWith("/")) {
          this.transition(jobId, JobStatus.DONE);
        }
        return;
      }
      const { s3Url, size } = resolved;

      try {
        await this.dbManager.pool.query("UPDATE parse_jobs SET s3_url = $1, size = $2, updated_at = NOW() WHERE job_id = $3", [s3Url, size, jobId]);
      } catch (e) {
        this.logger.warn("ingest_s3_url_update_failed", { job_id: jobId, error: String(e) });
      }

      const [bucket, key] = this.gcsUtils.parseGcsUrl(s3Url);
      const header = await this.gcsUtils.readRange(bucket, key, 0, 511);
      const archiveType = detectArchiveType(header);

      if (archiveType) {
        await this.handleArchive(jobId, s3Url, archiveType, msg, size);
        return;
      }

      const config = this.getConfig();
      await sendRaw(config.settings.CLASSIFY_QUEUE_URL, {
        job_id: jobId,
        s3_url: s3Url,
        size,
        field_spec: msg.field_spec,
      });
      this.logger.info("ingest_forwarded_to_classify", { job_id: jobId, s3_url: s3Url });
      metrics.increment("ingest.forwarded", 1, { target: "classify" });
    } catch (exc) {
      if (exc instanceof SSRFError) {
        this.logger.error("ssrf_blocked", { job_id: jobId }, exc);
        metrics.increment("ingest.ssrf_blocked", 1);
        this.transition(jobId, JobStatus.FAILED, `SSRF blocked: ${exc}`);
      } else if (exc instanceof PasswordError) {
        const attempts = this.passwordAttempts.get(jobId) || 0;
        const config = this.getConfig();
        if (attempts >= config.settings.ARCHIVE_PASSWORD_MAX_ATTEMPTS) {
          this.logger.error("archive_password_exhausted", { job_id: jobId, attempts });
          metrics.increment("ingest.password_exhausted", 1);
          this.transition(jobId, JobStatus.FAILED, `password_unavailable: ${exc}`);
        } else {
          this.passwordAttempts.set(jobId, attempts + 1);
          this.logger.info("archive_password_required", { job_id: jobId, attempts: attempts + 1 });
          this.transition(jobId, JobStatus.AWAITING_PASSWORD);
        }
      } else {
        this.logger.error("ingest_error", { job_id: jobId }, exc instanceof Error ? exc : new Error(String(exc)));
        metrics.increment("ingest.error", 1);
        this.transition(jobId, JobStatus.FAILED, String(exc));
      }
    }
  }

  private async resolveSource(msg: IngestMessage): Promise<{ s3Url: string; size: number } | null> {
    if (msg.source_type === SourceType.S3) {
      const url = msg.source_ref;
      if (url.endsWith("/")) {
        const objects = await listS3Prefix(url);
        for (const [objUrl, objSize] of objects) {
          await this.emit(msg.job_id, EventType.ENTRY_DISCOVERED, {
            parent_job_id: msg.job_id,
            batch_id: msg.batch_id || msg.job_id,
            entry_s3_url: objUrl,
            entry_name: objUrl,
            entry_size: objSize,
            field_spec: msg.field_spec || [],
            source_type: SourceType.S3,
          });
        }
        this.logger.info("s3_prefix_fanout", { job_id: msg.job_id, count: objects.length });
        metrics.increment("ingest.prefix_fanout", objects.length);
        return null;
      }
      const [bucket, key] = this.gcsUtils.parseGcsUrl(url);
      const size = await this.gcsUtils.objectSize(bucket, key);
      return { s3Url: url, size };
    } else if (msg.source_type === SourceType.URL) {
      const [s3Url, size] = await fetchUrlToS3(msg.job_id, msg.source_ref);
      return { s3Url, size };
    } else if ([SourceType.UPLOAD, SourceType.ARCHIVE_ENTRY].includes(msg.source_type)) {
      const [bucket, key] = this.gcsUtils.parseGcsUrl(msg.source_ref);
      this.logger.debug("upload_source_debug", { job_id: msg.job_id, bucket, key, source_ref: msg.source_ref });
      
      let size = 0;
      let attempts = 0;
      const maxAttempts = 5;
      while (attempts < maxAttempts) {
        try {
          size = await this.gcsUtils.objectSize(bucket, key);
          break;
        } catch (err) {
          attempts++;
          if (attempts >= maxAttempts) throw err;
          this.logger.warn("upload_size_check_retry", { job_id: msg.job_id, attempt: attempts, error: String(err) });
          await new Promise(r => setTimeout(r, 1000));
        }
      }
      
      const config = this.getConfig();
      if (msg.source_type === SourceType.UPLOAD && bucket === config.settings.DATA_BUCKET && key.startsWith("uploads/")) {
        const dstKey = key.replace("uploads/", "ingested/");
        
        try {
          await this.gcsUtils.copyObject(bucket, key, bucket, dstKey);
          const ingestedUrl = `gs://${bucket}/${dstKey}`;
          this.logger.info("upload_copied_to_ingested", { job_id: msg.job_id, source_ref: msg.source_ref, ingested_url: ingestedUrl });
          return { s3Url: ingestedUrl, size };
        } catch (copyError) {
          this.logger.error("upload_copy_failed", { job_id: msg.job_id, source_ref: msg.source_ref, error: String(copyError) }, copyError instanceof Error ? copyError : new Error(String(copyError)));
          metrics.increment("ingest.copy_failed", 1);
          throw new Error(`Failed to copy file from uploads to ingested: ${String(copyError)}`);
        }
      }
      
      return { s3Url: msg.source_ref, size };
    }
    throw new Error(`Unknown source_type: ${msg.source_type}`);
  }

  private async handleArchive(
    jobId: string,
    s3Url: string,
    archiveType: string,
    msg: IngestMessage,
    compressedSize: number
  ): Promise<void> {
    const password = msg.password ?? this.passwordCache.get(jobId)?.toString();
    try {
      const entries = await this.withTimeout(
        extractArchiveToS3(jobId, s3Url, archiveType, msg.field_spec, msg.batch_id || jobId, password),
        this.EXTRACTION_TIMEOUT_MS,
        `extractArchiveToS3(${jobId})`
      );
      if (!entries.length) {
        this.logger.warn("archive_empty", { job_id: jobId, s3_url: s3Url });
        metrics.increment("ingest.archive_empty", 1);
        this.transition(jobId, JobStatus.FAILED, "Archive contained no extractable files");
        return;
      }
      
      let hasPending = false;
      for (const entry of entries) {
        if (entry.pending) {
          hasPending = true;
          this.logger.info("archive_entry_pending", { job_id: jobId, entry_name: entry.entry_name, entry_size: entry.entry_size });
          metrics.increment("ingest.entry_pending", 1);
          await this.dbManager.createPendingArchiveEntry(jobId, entry.entry_name, entry.entry_size);
        } else {
          await this.emit(jobId, EventType.ENTRY_DISCOVERED, entry);
        }
      }
      
      this.logger.info("archive_extracted", { job_id: jobId, entries: entries.length, pending: hasPending });
      metrics.increment("ingest.archive_extracted", entries.length);
      
      if (hasPending) {
        this.logger.info("archive_has_pending_entries", { job_id: jobId });
      } else {
        this.transition(jobId, JobStatus.DONE);
      }
    } catch (exc) {
      const errStr = String(exc).toLowerCase();
      if (errStr.includes("password") || errStr.includes("encrypted") || errStr.includes("bad password")) {
        throw new PasswordError(String(exc));
      }
      if (exc instanceof BombError) {
        this.logger.error("archive_bomb_detected", { job_id: jobId }, exc);
        metrics.increment("ingest.archive_bomb", 1);
        this.transition(jobId, JobStatus.FAILED, `Archive bomb: ${exc}`);
        return;
      }
      this.logger.error("archive_extraction_failed", { job_id: jobId }, exc instanceof Error ? exc : new Error(String(exc)));
      metrics.increment("ingest.archive_error", 1);
      this.transition(jobId, JobStatus.FAILED, String(exc));
    }
  }

  public async handlePassword(jobId: string, password: string): Promise<void> {
    this.passwordCache.set(jobId, Buffer.from(password));
    this.logger.info("password_received", { job_id: jobId });

    const row = await this.dbManager.getJob(jobId);
    if (!row) {
      this.logger.error("password_job_not_found", { job_id: jobId });
      return;
    }

    const config = this.getConfig();
    await sendRaw(config.settings.INGEST_QUEUE_URL, {
      job_id: jobId,
      source_type: row.source_type,
      source_ref: row.source_ref,
      field_spec: Array.isArray(row.field_spec) ? row.field_spec : [],
      batch_id: row.batch_id,
      password,
    });
  }

  public async consumerLoop(): Promise<void> {
    while (true) {
      try {
        await this.dbManager.initialize();
        const config = this.getConfig();
        this.logger.info("ingest_consumer_started", { queue_url: config.settings.INGEST_QUEUE_URL, queue_backend: config.settings.QUEUE_BACKEND });
        
        while (true) {
          this.logger.info("ingest_waiting_for_messages");
          const messages = await receiveMessages<IngestMessage>(
            config.settings.INGEST_QUEUE_URL,
            (body) => JSON.parse(body) as IngestMessage,
            5
          );
          this.logger.info("ingest_messages_received", { count: messages.length });
          for (const { payload, receiptHandle } of messages) {
            try {
              if ((payload as any).action === "provide_password") {
                await this.handlePassword(payload.job_id, (payload as any).password);
              } else {
                await this.handleIngest(payload);
              }
              await deleteMessage(config.settings.INGEST_QUEUE_URL, receiptHandle);
            } catch (exc) {
              const errorStr = String(exc);
              if ((errorStr.includes("Job") && errorStr.includes("not found")) || errorStr.includes("cannot transition")) {
                this.logger.error("ingest_message_failed_ack", { job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" });
                metrics.increment("ingest.message_error_ack", 1);
                await deleteMessage(config.settings.INGEST_QUEUE_URL, receiptHandle);
              } else {
                this.logger.error("ingest_message_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
                metrics.increment("ingest.message_error", 1);
              }
            }
          }
        }
      } catch (dbError) {
        this.logger.error("database_connection_lost", { error: String(dbError) }, dbError instanceof Error ? dbError : new Error(String(dbError)));
        metrics.increment("ingest.db_connection_lost", 1);
        await this.dbManager.initialize();
        await new Promise(r => setTimeout(r, 5000));
      }
    }
  }
}

class PasswordError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PasswordError";
  }
}


export default IngestService;

// Backward compatibility wrappers
const ingestService = IngestService.getInstance();

export async function handleIngest(msg: IngestMessage): Promise<void> {
  return ingestService.handleIngest(msg);
}

export async function handlePassword(jobId: string, password: string): Promise<void> {
  return ingestService.handlePassword(jobId, password);
}

export async function consumerLoop(): Promise<void> {
  return ingestService.consumerLoop();
}
