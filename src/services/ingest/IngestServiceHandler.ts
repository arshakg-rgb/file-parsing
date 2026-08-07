import pino from "pino";
import { settings } from "@shared/Settings.js";
import { mapWithConcurrency } from "@utils/concurrency.js";
import { EventType, makeJobEvent } from "@shared/models/events.js";
import { JobStatus, SourceType, IngestMessage } from "@shared/models/job.js";
import { BombError } from "@errors/BombError.js";
import { createLogger } from "@utils/logger/Log.js";
import {PasswordError} from "@errors/PasswordError";
import {
  GCS_OR_S3_URL_PATTERN,
  HTTP_URL_PATTERN,
  OBJECT_SIZE_MAX_ATTEMPTS, OBJECT_SIZE_RETRY_DELAY_MS,
  PASSWORD_ERROR_KEYWORDS,
  UPLOAD_LIKE_SOURCE_TYPES
} from "@service/ingest/io/IIngest";
import {IParseJob} from "@config/db/models";
import {IngestServiceImpl} from "@service/ingest/IngestServiceImpl";
import { InstantiationError } from "@errors/InstantiationError.js";
import {SSRFError} from "@errors/SSRFError";
import HealthService from "@utils/response/Health";
import { MetricsUtils } from "@utils/response/Metrics";
import {DatabaseService} from "@shared/DatabaseManager";
import {GcsUtils} from "@shared/GcsUtils";
import {QueueService} from "@shared/QueueService";
import {QueueConsumerPool} from "@shared/QueueConsumerPool.js";


export class IngestService
{
  /**
   * Singleton instance
   * @private
   */

  private static instance: IngestService;

  private ingestServiceImpl: IngestServiceImpl;

  private running: boolean = false;

  /**
   *  Total Ingests @private
   */

  private totalIngests: number = 0;

  /**
    Total Archives Extracted @private
   *  */

  private totalArchivesExtracted: number = 0;

  /**
   * Total Passwords Provided @private
   */

  private totalPasswordsProvided: number = 0;

  /**
   *  Password Cache @private
   */

  private passwordCache: Map<string, Buffer> = new Map();

  /**
   *  Password Attempts @private
   */

  private passwordAttempts: Map<string, number> = new Map();

  /**
   *  Active Ingests @private
   */

  private activeIngests: Set<string> = new Set();

  private stats = {
    s3PrefixFanouts: 0,
    urlFetches: 0,
    uploadCopies: 0,
    archiveExtractions: 0,
    passwordErrors: 0,
    ssrfBlocks: 0,
    archiveBombs: 0,
  };

  private logger: pino.Logger = createLogger(module);

  private readonly EXTRACTION_TIMEOUT_MS = 50 * 60 * 1000;

  private gcsUtils: GcsUtils;

  private queueService: QueueService;

  /**
   * Private constructor to enforce a Singleton pattern.
   *
   * @param enforce - Function to enforce a Singleton pattern.
   * @param ingestServiceImpl - The ingest service instance.
   * @param gcsUtils - The gcs utils instance.
   * @throws Error if instantiation is attempted directly.
   */

  private constructor(enforce: () => void, ingestServiceImpl: IngestServiceImpl, gcsUtils: GcsUtils, queueService: QueueService)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use IngestService.getInstance() instead of new.");
    }

    this.gcsUtils = gcsUtils;
    this.ingestServiceImpl = ingestServiceImpl;
    this.queueService = queueService;

    this.startHealthCheckServers();
  }

  /**
   * Gets the singleton instance of IngestService.
   *
   * @returns The singleton instance of IngestService.
   */

  static getInstance(): IngestService
  {
    if (!IngestService.instance)
    {
      IngestService.instance = new IngestService(Enforce, IngestServiceImpl.getInstance(), GcsUtils.getInstance(), QueueService.getInstance());
    }

    return IngestService.instance;
  }

  /**
   * Start health check servers on the Cloud Run-injected PORT (or 8080), and
   * additionally on HEALTH_CHECK_PORT if configured and different.
   * @private
   */

  private startHealthCheckServers(): void
  {
    const ports = new Set<number>();
    const cloudRunPort: number = process.env.PORT ? parseInt(process.env.PORT, 10) : 8080;
    ports.add(cloudRunPort);

    if (process.env.HEALTH_CHECK_PORT)
    {
      const p: number = parseInt(process.env.HEALTH_CHECK_PORT, 10);

      if (!isNaN(p) && p !== cloudRunPort)
      {
        ports.add(p);
      }
    }

    for (const port of ports)
    {
      try
      {
        HealthService.startHealthCheckServer(port);
      }
      catch (err)
      {
        this.logger.error("health_server_start_failed", { port, error: err instanceof Error ? err.message : String(err) });
      }
    }
  }

  /**
   * Initialize the service
   */

  async initialize(): Promise<void>
  {
    await DatabaseService.getInstance().waitForDb();
    this.logger.info("ingest_initialized");
  }

  /**
   * Start the consumer loop
   */

  async start(): Promise<void>
  {
    if (this.running)
    {
      this.logger.warn("ingest_already_running");
      return;
    }

    this.running = true;
    await this.initialize();
    this.logger.info("ingest_started");

    await this.consumerLoop();
  }

  /**
   * Wrap a promise with a timeout
   *
   * @param promise - The promise to wrap
   * @param ms - Timeout in milliseconds
   * @param label - Label for error message
   * @returns Promise that rejects if timeout expires
   */

  private async withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T>
  {
    let timer: NodeJS.Timeout;
    const timeout = new Promise<never>((_, reject) => {timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);});
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
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
    await this.queueService.publishEvent(makeJobEvent(eventType, jobId, "ingest", data));
  }

  /**
   * Transition a job to a new status
   *
   * @param jobId - Job identifier
   * @param newStatus - New job status
   * @param error - Optional error message
   */

  private async transition(jobId: string, newStatus: JobStatus, error?: string): Promise<void>
  {
    await this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: newStatus, ...(error ? { error } : {}) });
  }

  /**
   * Assert that a source_ref matches an expected URL scheme, throwing a
   * descriptive error otherwise.
   *
   * @param url - The URL to validate
   * @param pattern - The expected scheme pattern
   * @param description - Human-readable description of the expected scheme, for the error message
   * @private
   */

  private assertUrlScheme(url: string, pattern: RegExp, description: string): void
  {
    if (!pattern.test(url))
    {
      throw new Error(`source_ref must be ${description}: ${url}`);
    }
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

  async handleIngest(msg: IngestMessage): Promise<void>
  {
    const ingestStartTime: number = Date.now();
    this.totalIngests++;

    const jobId: string = msg.job_id;

    if (this.activeIngests.has(jobId))
    {
      this.logger.info("ingest_already_in_progress", { job_id: jobId });
      return;
    }

    this.activeIngests.add(jobId);

    try
    {
      const moved = await DatabaseService.getInstance().repositories.jobs.tryTransitionStatus(
          jobId,
          JobStatus.INGESTING,
          [JobStatus.CREATED, JobStatus.NEEDS_PASSWORD],
          {
            timings: { ingesting_at: new Date().toISOString() },
          }
      );

      if (!moved)
      {
        this.logger.info("ingest_unexpected_status", { job_id: jobId });
        return;
      }

      this.logger.info("ingest_start", { job_id: jobId, source_type: msg.source_type });
      MetricsUtils.increment("ingest.start", 1, { source_type: msg.source_type });

      try {
      const resolved: { s3Url: string; size: number } = await this.resolveSource(msg);

      if (!resolved)
      {
        if (msg.source_type === SourceType.S3 && msg.source_ref.endsWith("/"))
        {
          await this.transition(jobId, JobStatus.COMPLETED);
        }
        return;
      }

      const { s3Url, size } = resolved;

      try
      {
        await DatabaseService.getInstance().repositories.jobs.updateS3Url(jobId, s3Url, size);
      }
      catch (e)
      {
        this.logger.warn("ingest_s3_url_update_failed", { job_id: jobId, error: String(e) });
      }

      const [bucket, key] = this.gcsUtils.parseGcsUrl(s3Url);

      if (size === 0)
      {
        throw new Error("Source file is empty");
      }

      const header: Buffer = await this.gcsUtils.readRange(bucket, key, 0, Math.min(511, size - 1));
      const archiveType: string = this.ingestServiceImpl.detectArchiveType(header);

      if (archiveType)
      {
        await this.handleArchive(jobId, s3Url, archiveType, msg);
        return;
      }

      await this.queueService.sendRaw(settings.CLASSIFY_QUEUE_URL,
      {
        job_id: jobId,
        s3_url: s3Url,
        size,
        field_spec: msg.field_spec,
        column_map: msg.column_map,
      });

      this.logger.info("ingest_forwarded_to_classify", { job_id: jobId, s3_url: s3Url });
      MetricsUtils.increment("ingest.forwarded", 1, { target: "classify" });
    }
    catch (exc)
    {
      await this.handleIngestError(exc, jobId);
    }
    }
    finally
    {
      this.activeIngests.delete(jobId);
      const ingestDuration: number = Date.now() - ingestStartTime;
      MetricsUtils.set("ingest.duration_ms", ingestDuration);
    }
  }

  /**
   * Handle an error raised during ingest, transitioning the job and recording
   * MetricsUtils appropriately for the error type (SSRF block, password required, or
   * generic failure).
   *
   * @param exc - The caught error
   * @param jobId - Job identifier
   * @private
   */

  private async handleIngestError(exc: unknown, jobId: string): Promise<void>
  {
    if (exc instanceof SSRFError)
    {
      this.logger.error("ssrf_blocked", { job_id: jobId }, exc);
      this.stats.ssrfBlocks++;
      MetricsUtils.increment("ingest.ssrf_blocked", 1);
      await this.transition(jobId, JobStatus.FAILED, `SSRF blocked: ${exc}`);
      return;
    }

    if (exc instanceof PasswordError)
    {
      this.stats.passwordErrors++;
      const attempts: number = this.passwordAttempts.get(jobId) || 0;

      if (attempts >= settings.ARCHIVE_PASSWORD_MAX_ATTEMPTS)
      {
        this.logger.error("archive_password_exhausted", { job_id: jobId, attempts });
        MetricsUtils.increment("ingest.password_exhausted", 1);
        await this.transition(jobId, JobStatus.FAILED, `password_unavailable: ${exc}`);
      }
      else
      {
        this.passwordAttempts.set(jobId, attempts + 1);
        this.logger.info("archive_password_required", { job_id: jobId, attempts: attempts + 1 });
        await this.transition(jobId, JobStatus.NEEDS_PASSWORD);
      }

      return;
    }

    this.logger.error("ingest_error", { job_id: jobId }, exc instanceof Error ? exc : new Error(String(exc)));
    MetricsUtils.increment("ingest.error", 1);
    await this.transition(jobId, JobStatus.FAILED, String(exc));
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

  private async resolveSource(msg: IngestMessage): Promise<{ s3Url: string; size: number } | null>
  {
    if (msg.source_type === SourceType.S3)
    {
      return this.resolveS3Source(msg);
    }

    if (msg.source_type === SourceType.URL)
    {
      return this.resolveUrlSource(msg);
    }

    if (UPLOAD_LIKE_SOURCE_TYPES.includes(msg.source_type))
    {
      return this.resolveUploadSource(msg);
    }

    throw new Error(`Unknown source_type: ${msg.source_type}`);
  }

  /**
   * Resolve an S3/GCS source: either a direct object URL, or a prefix that
   * fans out into one ENTRY_DISCOVERED event per object (returning null, since
   * there is no single object to ingest).
   *
   * @param msg - Ingest message
   * @private
   */

  private async resolveS3Source(msg: IngestMessage): Promise<{ s3Url: string; size: number } | null>
  {
    const url: string = msg.source_ref;
    this.assertUrlScheme(url, GCS_OR_S3_URL_PATTERN, "a gs:// or s3:// URL");

    if (url.endsWith("/"))
    {
      this.stats.s3PrefixFanouts++;
      const objects: [string, number][] = await this.ingestServiceImpl.listS3Prefix(url);

      await mapWithConcurrency(objects, 25, async ([objUrl, objSize]) => {
        await this.queueService.publishEvent(
            makeJobEvent(EventType.ENTRY_DISCOVERED, msg.job_id, "ingest", {
              parent_job_id: msg.job_id,
              batch_id: msg.batch_id || msg.job_id,
              entry_s3_url: objUrl,
              entry_name: objUrl,
              entry_size: objSize,
              field_spec: msg.field_spec || [],
              source_type: SourceType.S3,
            })
        );
      });
      this.logger.info("s3_prefix_fanout", { job_id: msg.job_id, count: objects.length });
      MetricsUtils.increment("ingest.prefix_fanout", objects.length);
      return null;
    }

    const [bucket, key] = this.gcsUtils.parseGcsUrl(url);
    const size: number = await this.gcsUtils.objectSize(bucket, key);
    return { s3Url: url, size };
  }

  /**
   * Resolve a URL source by fetching it into S3.
   *
   * @param msg - Ingest message
   * @private
   */

  private async resolveUrlSource(msg: IngestMessage): Promise<{ s3Url: string; size: number }>
  {
    this.stats.urlFetches++;
    this.assertUrlScheme(msg.source_ref, HTTP_URL_PATTERN, "an http(s) URL for url sources");
    const [s3Url, size] = await this.ingestServiceImpl.fetchUrlToS3(msg.job_id, msg.source_ref);

    return { s3Url, size };
  }

  /**
   * Resolve an UPLOAD or ARCHIVE_ENTRY source: retries the (eventually
   * consistent) object size lookup, then copies uploads into the ingested
   * bucket when applicable.
   *
   * @param msg - Ingest message
   * @private
   */

  private async resolveUploadSource(msg: IngestMessage): Promise<{ s3Url: string; size: number }>
  {
    this.assertUrlScheme(msg.source_ref, GCS_OR_S3_URL_PATTERN, "a gs:// or s3:// URL");
    const [bucket, key] = this.gcsUtils.parseGcsUrl(msg.source_ref);
    this.logger.debug("upload_source_debug", { job_id: msg.job_id, bucket, key, source_ref: msg.source_ref });

    const size: number = await this.retryObjectSize(bucket, key, msg.job_id);

    if (msg.source_type === SourceType.UPLOAD && bucket === settings.DATA_BUCKET && key.startsWith("uploads/"))
    {
      return this.copyUploadToIngested(msg, bucket, key, size);
    }

    return { s3Url: msg.source_ref, size };
  }

  /**
   * Retry an objectSize lookup to tolerate GCS's eventual consistency shortly
   * after an upload completes.
   *
   * @param bucket - GCS bucket
   * @param key - GCS object key
   * @param jobId - Job identifier, for logging
   * @returns The resolved object size
   * @throws The last error if all attempts are exhausted
   * @private
   */

  private async retryObjectSize(bucket: string, key: string, jobId: string): Promise<number>
  {
    let attempts: number = 0;

    while (true)
    {
      try
      {
        return await this.gcsUtils.objectSize(bucket, key);
      }
      catch (err)
      {
        attempts++;

        if (attempts >= OBJECT_SIZE_MAX_ATTEMPTS)
        {
          throw err;
        }

        this.logger.warn("upload_size_check_retry", { job_id: jobId, attempt: attempts, error: String(err) });
        await new Promise((r) => setTimeout(r, OBJECT_SIZE_RETRY_DELAY_MS));
      }
    }
  }

  /**
   * Copy an uploaded object from the uploads/ prefix to the ingested/ prefix.
   *
   * @param msg - Ingest message
   * @param bucket - GCS bucket
   * @param key - Source object key
   * @param size - Object size, passed through on success
   * @private
   */

  private async copyUploadToIngested(msg: IngestMessage, bucket: string, key: string, size: number): Promise<{ s3Url: string; size: number }>
  {
    this.stats.uploadCopies++;
    const dstKey: string = key.replace("uploads/", "ingested/");

    try
    {
      await this.gcsUtils.copyObject(bucket, key, bucket, dstKey);
      const ingestedUrl = `gs://${bucket}/${dstKey}`;
      this.logger.info("upload_copied_to_ingested", { job_id: msg.job_id, source_ref: msg.source_ref, ingested_url: ingestedUrl });
      return { s3Url: ingestedUrl, size };
    }
    catch (copyError)
    {
      this.logger.error(
          "upload_copy_failed",
          { job_id: msg.job_id, source_ref: msg.source_ref, error: String(copyError) },
          copyError instanceof Error ? copyError : new Error(String(copyError))
      );

      MetricsUtils.increment("ingest.copy_failed", 1);

      throw new Error(`Failed to copy file from uploads to ingested: ${String(copyError)}`);
    }
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
   * @throws PasswordError if password is required and not available
   * @throws Error if extraction fails
   */

  private async handleArchive(jobId: string, s3Url: string, archiveType: string, msg: IngestMessage,): Promise<void>
  {
    this.totalArchivesExtracted++;
    this.stats.archiveExtractions++;

    const password: string = msg.password ?? this.passwordCache.get(jobId)?.toString();
    try
    {
      const entries: Record<string, unknown>[] = await this.withTimeout(
          this.ingestServiceImpl.extractArchiveToS3(jobId, s3Url, archiveType, msg.field_spec, msg.batch_id || jobId, password),
          this.EXTRACTION_TIMEOUT_MS,
          `extractArchiveToS3(${jobId})`
      );

      if (!entries.length)
      {
        this.logger.warn("archive_empty", { job_id: jobId, s3_url: s3Url });
        MetricsUtils.increment("ingest.archive_empty", 1);
        await this.transition(jobId, JobStatus.FAILED, "Archive contained no extractable files");
        return;
      }

      const results = await mapWithConcurrency(entries, 25, (entry) =>
        this.processArchiveEntry(jobId, entry)
      );
      const hasPending: boolean = results.some(Boolean);

      this.logger.info("archive_extracted", { job_id: jobId, entries: entries.length, pending: hasPending });
      MetricsUtils.increment("ingest.archive_extracted", entries.length);

      if (hasPending)
      {
        this.logger.info("archive_has_pending_entries", { job_id: jobId });
      }
      else
      {
        await this.transition(jobId, JobStatus.COMPLETED);
      }
    }
    catch (exc)
    {
      await this.handleArchiveError(exc, jobId);
    }
  }

  /**
   * Process a single archive entry: either record it as a pending async entry,
   * or publish an ENTRY_DISCOVERED event for it immediately.
   *
   * @param jobId - Job identifier
   * @param entry - Raw entry data returned by extractArchiveToS3
   * @returns true if the entry is pending (async), false if it was published immediately
   * @private
   */

  private async processArchiveEntry(jobId: string, entry: Record<string, unknown>): Promise<boolean>
  {
    const entryName = entry.entry_name as string;
    const entrySize = entry.entry_size as number;
    const isPending = entry.pending as boolean;

    if (isPending)
    {
      this.logger.info("archive_entry_pending", { job_id: jobId, entry_name: entryName, entry_size: entrySize });
      MetricsUtils.increment("ingest.entry_pending", 1);
      await DatabaseService.getInstance().createPendingArchiveEntry(jobId, entryName, entrySize);
      return true;
    }

    await this.queueService.publishEvent(makeJobEvent(EventType.ENTRY_DISCOVERED, jobId, "ingest", entry));
    return false;
  }

  /**
   * Handle an error raised during archive extraction: distinguishes password
   * errors (re-thrown for the caller to convert into an AWAITING_PASSWORD
   * transition), archive bombs, and generic failures.
   *
   * @param exc - The caught error
   * @param jobId - Job identifier
   * @throws PasswordError if the failure looks password/encryption related
   * @private
   */

  private async handleArchiveError(exc: unknown, jobId: string): Promise<void>
  {
    const errStr: string = String(exc).toLowerCase();

    if (PASSWORD_ERROR_KEYWORDS.some((keyword) => errStr.includes(keyword)))
    {
      throw new PasswordError(String(exc));
    }

    if (exc instanceof BombError)
    {
      this.stats.archiveBombs++;
      this.logger.error("archive_bomb_detected", { job_id: jobId }, exc);
      MetricsUtils.increment("ingest.archive_bomb", 1);
      await this.transition(jobId, JobStatus.FAILED, `Archive bomb: ${exc}`);
      return;
    }

    this.logger.error("archive_extraction_failed", { job_id: jobId }, exc instanceof Error ? exc : new Error(String(exc)));
    MetricsUtils.increment("ingest.archive_error", 1);
    await this.transition(jobId, JobStatus.FAILED, String(exc));
  }

  /**
   * Handle password provision for encrypted archives
   *
   * Caches the password and re-queues the job for processing.
   *
   * @param jobId - Job identifier
   * @param password - Password for the archive
   */

  async handlePassword(jobId: string, password: string): Promise<void>
  {
    this.totalPasswordsProvided++;
    this.passwordCache.set(jobId, Buffer.from(password));
    this.logger.info("password_received", { job_id: jobId });

    const row: IParseJob = await DatabaseService.getInstance().getJob(jobId);

    if (!row)
    {
      this.logger.error("password_job_not_found", { job_id: jobId });
      return;
    }

    await this.queueService.sendRaw(settings.INGEST_QUEUE_URL, {
      job_id: jobId,
      source_type: row.source_type,
      source_ref: row.source_ref,
      field_spec: Array.isArray(row.field_spec) ? row.field_spec : [],
      batch_id: row.batch_id,
      password,
    });
  }

  /**
   * Process a single message from the ingest queue: either a password
   * submission or a regular ingest job. Acknowledges "unrecoverable" errors
   * (job not found, invalid transition) to avoid an infinite retry loop,
   * while leaving other errors unacknowledged for redelivery.
   *
   * @param payload - Parsed message payload
   * @param receiptHandle - Queue receipt handle, for deletion
   * @private
   */

  private async handleQueueMessage(payload: IngestMessage, receiptHandle: string): Promise<void>
  {
    try
    {
      const payloadRecord = payload as unknown as Record<string, unknown>;

      if (payloadRecord.action === "provide_password")
      {
        await this.handlePassword(payload.job_id, payloadRecord.password as string);
      }
      else
      {
        await this.handleIngest(payload);
      }
      await this.queueService.deleteMessage(settings.INGEST_QUEUE_URL, receiptHandle);
    }
    catch (exc)
    {
      const errorStr: string = String(exc);

      if ((errorStr.includes("Job") && errorStr.includes("not found")) || errorStr.includes("cannot transition"))
      {
        this.logger.error("ingest_message_failed_ack", { job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" });
        MetricsUtils.increment("ingest.message_error_ack", 1);
        await this.queueService.deleteMessage(settings.INGEST_QUEUE_URL, receiptHandle);
      }
      else
      {
        this.logger.error("ingest_message_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
        MetricsUtils.increment("ingest.message_error", 1);
      }
    }
  }

  /**
   * Main consumer loop for processing ingest messages
   *
   * Continuously polls the ingest queue for messages and processes them.
   * Handles graceful shutdown, database reconnection, and message acknowledgment.
   *
   * @throws Error if database connection fails
   */

  private async consumerLoop(): Promise<void>
  {
    while (this.running)
    {
      try
      {
        await DatabaseService.getInstance().waitForDb();
        this.logger.info("ingest_consumer_started", { queue_url: settings.INGEST_QUEUE_URL, queue_backend: settings.QUEUE_BACKEND });

        const pool = new QueueConsumerPool<IngestMessage>(this.queueService, this.logger, {
          queueUrl: settings.INGEST_QUEUE_URL,
          parser: (body) => JSON.parse(body) as IngestMessage,
          concurrency: settings.QUEUE_CONCURRENCY,
          memorySoftLimit: settings.QUEUE_MEMORY_SOFT_LIMIT_MB * 1024 * 1024,
          isRunning: () => this.running,
        });

        await pool.run((payload, receiptHandle) => this.handleQueueMessage(payload, receiptHandle));
      }
      catch (dbError)
      {
        this.logger.error("database_connection_lost", { error: String(dbError) }, dbError instanceof Error ? dbError : new Error(String(dbError)));
        MetricsUtils.increment("ingest.db_connection_lost", 1);
        await DatabaseService.getInstance().waitForDb();
        await new Promise((r) => setTimeout(r, 5000));
      }
    }

    this.logger.info("ingest_consumer_stopped");
  }
}

IngestService.getInstance()
    .start()
    .catch((err) => {
      console.error("ingest_start_failed", { error: String(err) });
      process.exit(1);
    });

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void
{
}
