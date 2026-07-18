import { settings } from "../../shared/config.js";
import { EventType, JobEvent, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus, SourceType, IngestMessage } from "../../shared/models/job.js";
import { receiveMessages, deleteMessage, sendRaw, publishEvent } from "../../shared/queueUtils.js";
import { parseGcsUrl, objectSize, readRange, copyObject } from "../../shared/gcsUtils.js";
import { getJob, pool, waitForDb, createPendingArchiveEntry, getPendingEntryCount } from "../../shared/db.js";
import { detectArchiveType, extractArchiveToS3, fetchUrlToS3, listS3Prefix, BombError } from "./normalizer.js";
import { SSRFError } from "./ssrf_guard.js";
import { createLogger } from "../../utils/logger/logger.js";
import { metrics } from "../../utils/response/metrics.js";
import { startHealthCheckServer } from "../../utils/response/health.js";

/**
 * Custom error for password-related failures
 */
class PasswordError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PasswordError";
  }
}

/**
 * Ingest Service - Senior Level ORM-Style Implementation
 * 
 * This service handles job ingestion from various sources (S3, URL, Upload, Archive Entry).
 * It manages archive extraction, password handling, and forwarding to classification.
 * Follows ORM-style patterns with:
 * - Class-based architecture with instance state
 * - Dependency injection for services
 * - Lifecycle management (initialize, start, stop)
 * - Repository-style methods for data operations
 * - Clean separation of concerns
 * 
 * @class IngestService
 */
export class IngestService {
  private static instance: IngestService;
  
  // Instance state
  private running: boolean = false;
  private totalIngests: number = 0;
  private totalArchivesExtracted: number = 0;
  private totalPasswordsProvided: number = 0;
  private passwordCache: Map<string, Buffer> = new Map();
  private passwordAttempts: Map<string, number> = new Map();
  
  // Statistics
  private stats = {
    s3PrefixFanouts: 0,
    urlFetches: 0,
    uploadCopies: 0,
    archiveExtractions: 0,
    passwordErrors: 0,
    ssrfBlocks: 0,
    archiveBombs: 0
  };
  
  // Dependencies (injected)
  private logger = createLogger("ingest");
  
  // Constants
  private readonly EXTRACTION_TIMEOUT_MS = 50 * 60 * 1000; // 50 minutes under 3600s Cloud Run ceiling
  
  /**
   * Private constructor for singleton pattern
   */
  private constructor() {
    // Initialize health check server if port is configured
    if (process.env.HEALTH_CHECK_PORT) {
      startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
    }
  }
  
  /**
   * Get singleton instance
   */
  static getInstance(): IngestService {
    if (!IngestService.instance) {
      IngestService.instance = new IngestService();
    }
    return IngestService.instance;
  }
  
  /**
   * Initialize the service
   */
  async initialize(): Promise<void> {
    await waitForDb();
    this.logger.info("ingest_initialized");
  }
  
  /**
   * Start the consumer loop
   */
  async start(): Promise<void> {
    if (this.running) {
      this.logger.warn("ingest_already_running");
      return;
    }
    
    this.running = true;
    await this.initialize();
    this.logger.info("ingest_started");
    
    await this.consumerLoop();
  }
  
  /**
   * Stop the service gracefully
   */
  async stop(): Promise<void> {
    this.running = false;
    this.logger.info("ingest_stopping");
  }
  
  /**
   * Get service statistics
   */
  getStats() {
    return {
      ...this.stats,
      totalIngests: this.totalIngests,
      totalArchivesExtracted: this.totalArchivesExtracted,
      totalPasswordsProvided: this.totalPasswordsProvided,
      passwordCacheSize: this.passwordCache.size,
      passwordAttemptsSize: this.passwordAttempts.size
    };
  }

  /**
   * Wrap a promise with a timeout
   * 
   * @param promise - The promise to wrap
   * @param ms - Timeout in milliseconds
   * @param label - Label for error message
   * @returns Promise that rejects if timeout expires
   */
  private withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
    let timer: NodeJS.Timeout;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
  }

  /**
   * Emit a job event to the event system
   * 
   * @param jobId - Job identifier
   * @param eventType - Type of event to emit
   * @param data - Event payload data
   */
  private emit(jobId: string, eventType: EventType, data: Record<string, any>): void {
    publishEvent(makeJobEvent(eventType, jobId, "ingest", data));
  }

  /**
   * Transition a job to a new status
   * 
   * @param jobId - Job identifier
   * @param newStatus - New job status
   * @param error - Optional error message
   */
  private transition(jobId: string, newStatus: JobStatus, error?: string): void {
    this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: newStatus, ...(error ? { error } : {}) });
  }

  /**
   * Main ingest handler - processes jobs from various sources
   * 
   * This function handles the ingestion of files from S3, URLs, uploads, or archive entries.
   * It performs the following steps:
   * 1. Checks current job status to avoid duplicate processing
   * 2. Resolves the source to an S3 URL
   * 3. Detects if the file is an archive
   * 4. Extracts archives or forwards to classification
   * 
   * @param msg - Ingest message containing job details
   * @throws Error if ingestion fails
   */
  async handleIngest(msg: IngestMessage): Promise<void> {
    const ingestStartTime = Date.now();
    this.totalIngests++;
    
    const jobId = msg.job_id;
    
    // Check current status before transitioning to avoid duplicate events
    const currentJob = await pool.query("SELECT status FROM parse_jobs WHERE job_id = $1", [jobId]);
    const currentStatus = currentJob.rows[0]?.status;
    
    if (currentStatus === JobStatus.INGESTING) {
      this.logger.info("ingest_already_ingesting", { job_id: jobId });
      return;
    }
    
    if (currentStatus === JobStatus.FAILED) {
      this.logger.info("ingest_job_failed", { job_id: jobId });
      return;
    }
    
    this.transition(jobId, JobStatus.INGESTING);
    this.logger.info("ingest_start", { job_id: jobId, source_type: msg.source_type });
    metrics.increment("ingest.start", 1, { source_type: msg.source_type });

    try {
      const resolved = await this.resolveSource(msg);
      if (!resolved) {
        if (msg.source_type === SourceType.S3 && msg.source_ref.endsWith("/")) {
          this.transition(jobId, JobStatus.DONE);
        }
        return;
      }
      const { s3Url, size } = resolved;

      try {
        await pool.query("UPDATE parse_jobs SET s3_url = $1, size = $2, updated_at = NOW() WHERE job_id = $3", [s3Url, size, jobId]);
      } catch (e) {
        this.logger.warn("ingest_s3_url_update_failed", { job_id: jobId, error: String(e) });
      }

      const [bucket, key] = parseGcsUrl(s3Url);
      const header = await readRange(bucket, key, 0, 511);
      const archiveType = detectArchiveType(header);

      if (archiveType) {
        await this.handleArchive(jobId, s3Url, archiveType, msg, size);
        return;
      }

      await sendRaw(settings.CLASSIFY_QUEUE_URL, {
        job_id: jobId,
        s3_url: s3Url,
        size,
        field_spec: msg.field_spec,
        column_map: msg.column_map,
      });
      this.logger.info("ingest_forwarded_to_classify", { job_id: jobId, s3_url: s3Url });
      metrics.increment("ingest.forwarded", 1, { target: "classify" });
    } catch (exc) {
      if (exc instanceof SSRFError) {
        this.logger.error("ssrf_blocked", { job_id: jobId }, exc);
        this.stats.ssrfBlocks++;
        metrics.increment("ingest.ssrf_blocked", 1);
        this.transition(jobId, JobStatus.FAILED, `SSRF blocked: ${exc}`);
      } else if (exc instanceof PasswordError) {
        this.stats.passwordErrors++;
        const attempts = this.passwordAttempts.get(jobId) || 0;
        if (attempts >= settings.ARCHIVE_PASSWORD_MAX_ATTEMPTS) {
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
    } finally {
      const ingestDuration = Date.now() - ingestStartTime;
      metrics.set("ingest.duration_ms", ingestDuration);
    }
  }

  /**
   * Resolve source reference to S3 URL and size
   * 
   * Handles different source types:
   * - S3: Direct S3 URL or prefix fanout
   * - URL: Fetch from URL to S3
   * - UPLOAD/ARCHIVE_ENTRY: Copy from uploads bucket
   * 
   * @param msg - Ingest message containing source reference
   * @returns S3 URL and size, or null for prefix fanout
   * @throws Error if source type is unknown or resolution fails
   */
  private async resolveSource(msg: IngestMessage): Promise<{ s3Url: string; size: number } | null> {
    if (msg.source_type === SourceType.S3) {
      const url = msg.source_ref;
      if (url.endsWith("/")) {
        this.stats.s3PrefixFanouts++;
        const objects = await listS3Prefix(url);
        for (const [objUrl, objSize] of objects) {
          await publishEvent(makeJobEvent(EventType.ENTRY_DISCOVERED, msg.job_id, "ingest", {
            parent_job_id: msg.job_id,
            batch_id: msg.batch_id || msg.job_id,
            entry_s3_url: objUrl,
            entry_name: objUrl,
            entry_size: objSize,
            field_spec: msg.field_spec || [],
            source_type: SourceType.S3,
          }));
        }
        this.logger.info("s3_prefix_fanout", { job_id: msg.job_id, count: objects.length });
        metrics.increment("ingest.prefix_fanout", objects.length);
        return null;
      }
      const [bucket, key] = parseGcsUrl(url);
      const size = await objectSize(bucket, key);
      return { s3Url: url, size };
    } else if (msg.source_type === SourceType.URL) {
      this.stats.urlFetches++;
      const [s3Url, size] = await fetchUrlToS3(msg.job_id, msg.source_ref);
      return { s3Url, size };
    } else if ([SourceType.UPLOAD, SourceType.ARCHIVE_ENTRY].includes(msg.source_type)) {
      const [bucket, key] = parseGcsUrl(msg.source_ref);
      this.logger.debug("upload_source_debug", { job_id: msg.job_id, bucket, key, source_ref: msg.source_ref });
      
      // Retry objectSize check for GCS consistency
      let size = 0;
      let attempts = 0;
      const maxAttempts = 5;
      while (attempts < maxAttempts) {
        try {
          size = await objectSize(bucket, key);
          break;
        } catch (err) {
          attempts++;
          if (attempts >= maxAttempts) throw err;
          this.logger.warn("upload_size_check_retry", { job_id: msg.job_id, attempt: attempts, error: String(err) });
          await new Promise(r => setTimeout(r, 1000));
        }
      }
      
      // Copy from uploads to ingested bucket for upload jobs
      if (msg.source_type === SourceType.UPLOAD && bucket === settings.DATA_BUCKET && key.startsWith("uploads/")) {
        this.stats.uploadCopies++;
        const dstKey = key.replace("uploads/", "ingested/");
        
        try {
          await copyObject(bucket, key, bucket, dstKey);
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

  /**
   * Handle archive extraction
   * 
   * Extracts files from archives (ZIP, RAR, etc.) and publishes events for each entry.
   * Handles password-protected archives and archive bomb detection.
   * 
   * @param jobId - Job identifier
   * @param s3Url - S3 URL of the archive
   * @param archiveType - Type of archive (zip, rar, etc.)
   * @param msg - Original ingest message
   * @param compressedSize - Size of compressed archive
   * @throws PasswordError if password is required and not available
   * @throws Error if extraction fails
   */
  private async handleArchive(
    jobId: string,
    s3Url: string,
    archiveType: string,
    msg: IngestMessage,
    compressedSize: number
  ): Promise<void> {
    this.totalArchivesExtracted++;
    this.stats.archiveExtractions++;
    
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
          // Create pending entry record in database
          await createPendingArchiveEntry(jobId, entry.entry_name, entry.entry_size);
        } else {
          await publishEvent(makeJobEvent(EventType.ENTRY_DISCOVERED, jobId, "ingest", entry));
        }
      }
      
      this.logger.info("archive_extracted", { job_id: jobId, entries: entries.length, pending: hasPending });
      metrics.increment("ingest.archive_extracted", entries.length);
      
      // If there are pending entries, transition to INGESTING (stays there until async entries complete)
      // If no pending entries, transition to DONE
      if (hasPending) {
        this.logger.info("archive_has_pending_entries", { job_id: jobId });
        // Keep in INGESTING status - already set at start of handleIngest
      } else {
        this.transition(jobId, JobStatus.DONE);
      }
    } catch (exc) {
      const errStr = String(exc).toLowerCase();
      if (errStr.includes("password") || errStr.includes("encrypted") || errStr.includes("bad password")) {
        throw new PasswordError(String(exc));
      }
      if (exc instanceof BombError) {
        this.stats.archiveBombs++;
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

  /**
   * Handle password provision for encrypted archives
   * 
   * Caches the password and re-queues the job for processing.
   * 
   * @param jobId - Job identifier
   * @param password - Password for the archive
   */
  async handlePassword(jobId: string, password: string): Promise<void> {
    this.totalPasswordsProvided++;
    this.passwordCache.set(jobId, Buffer.from(password));
    this.logger.info("password_received", { job_id: jobId });

    const row = await getJob(jobId);
    if (!row) {
      this.logger.error("password_job_not_found", { job_id: jobId });
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

  /**
   * Main consumer loop for processing ingest messages
   * 
   * Continuously polls the ingest queue for messages and processes them.
   * Handles graceful shutdown, database reconnection, and message acknowledgment.
   * 
   * @throws Error if database connection fails
   */
  private async consumerLoop(): Promise<void> {
    while (this.running) {
      try {
        await waitForDb();
        this.logger.info("ingest_consumer_started", { queue_url: settings.INGEST_QUEUE_URL, queue_backend: settings.QUEUE_BACKEND });
        
        while (this.running) {
          this.logger.info("ingest_waiting_for_messages");
          const messages = await receiveMessages<IngestMessage>(
            settings.INGEST_QUEUE_URL,
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
              await deleteMessage(settings.INGEST_QUEUE_URL, receiptHandle);
            } catch (exc) {
              const errorStr = String(exc);
              // Ack bad messages to prevent infinite retry loop
              if ((errorStr.includes("Job") && errorStr.includes("not found")) || errorStr.includes("cannot transition")) {
                this.logger.error("ingest_message_failed_ack", { job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" });
                metrics.increment("ingest.message_error_ack", 1);
                await deleteMessage(settings.INGEST_QUEUE_URL, receiptHandle);
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
        // Wait for database to be available again before retrying
        await waitForDb();
        // Additional wait to avoid tight loop
        await new Promise(r => setTimeout(r, 5000));
      }
    }
    
    this.logger.info("ingest_consumer_stopped");
  }
}

// Backward compatibility: export singleton instance and function wrappers
const ingestService = IngestService.getInstance();

// Backward compatibility wrappers
export async function handleIngest(msg: IngestMessage): Promise<void> {
  return ingestService.handleIngest(msg);
}

export async function handlePassword(jobId: string, password: string): Promise<void> {
  return ingestService.handlePassword(jobId, password);
}


// Auto-start the service when module is loaded
ingestService.start().catch(err => {
  console.error("ingest_start_failed", { error: String(err) });
  process.exit(1);
});
