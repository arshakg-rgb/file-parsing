import crypto from "crypto";
import fs from "fs/promises";
import { createWriteStream } from "fs";
import { pipeline } from "stream/promises";
import path from "path";
import { spawn } from "child_process";
import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import FirestoreCacheUtils from "@utils/cache/FirestoreCacheUtils.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
import { startHealthCheckServer } from "@utils/response/health.js";
import { ArchiveEntryConsumerService } from "@service/archive_entry_consumer/ArchiveEntryConsumerService.js";
import { IArchiveEntryConsumer, ArchiveEntryRequest, ArchiveEntryResponse } from "@service/archive_entry_consumer/io/IArchiveEntryConsumer.js";
import { settings } from "@shared/Settings.js";
import { EventType, makeJobEvent } from "@shared/models/events.js";
import { publishEvent } from "@shared/QueueService.js";
import { readRange, gcsClient } from "@shared/GcsUtils.js";
import { markPendingEntryProcessing, markPendingEntryCompleted, markPendingEntryFailed, createPendingArchiveEntry } from "@shared/DatabaseManager.js";
import { extractArchiveToS3, detectArchiveType } from "@service/ingest/normalizer.js";

/**
 * ArchiveEntryConsumerServiceImpl is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class ArchiveEntryConsumerServiceImpl extends ServiceManager implements ArchiveEntryConsumerService {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: ArchiveEntryConsumerServiceImpl;
    /**
   * Logger instance
   * @private
   */
  private logger: Logger;
    /**
   * Gcs Utils
   * @private
   */
  private gcsUtils: FirestoreCacheUtils;
    /**
   * Password Cache
   * @private
   */
  private passwordCache: Map<string, Buffer>;
    /**
   * Password Attempts
   * @private
   */
  private passwordAttempts: Map<string, number>;
    /**
   * M A X_ R E T R I E S
   * @private
   */
  private MAX_RETRIES = 3;
    /**
   * R E T R Y_ D E L A Y_ M S
   * @private
   */
  private RETRY_DELAY_MS = 5 * 60 * 1000;
    /**
   * M A X_ T O T A L_ U N C O M P R E S S E D
   * @private
   */
  private MAX_TOTAL_UNCOMPRESSED = 10 * 1024 * 1024 * 1024;
    /**
   * C O N C U R R E N T_ M E S S A G E S
   * @private
   */
  private CONCURRENT_MESSAGES = 3;

    /**
   * Constructs a new ArchiveEntryConsumerServiceImpl instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate ArchiveEntryConsumerServiceImpl directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("archive-entry-consumer");
    this.gcsUtils = FirestoreCacheUtils.getInstance();
    this.passwordCache = new Map<string, Buffer>();
    this.passwordAttempts = new Map<string, number>();
    
    if (process.env.HEALTH_CHECK_PORT) {
      startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
    }
  }

    /**
   * Gets the single instance of the ArchiveEntryConsumerServiceImpl class.
   * @returns The single instance of the class
   */
  public static getInstance(): ArchiveEntryConsumerServiceImpl {
    if (!ArchiveEntryConsumerServiceImpl.instance) {
      ArchiveEntryConsumerServiceImpl.instance = new ArchiveEntryConsumerServiceImpl(Enforce);
    }
    return ArchiveEntryConsumerServiceImpl.instance;
  }

    /**
   * Gets logger
   * @returns The logger result
   */
  public getLogger(): Logger {
    return this.logger;
  }

    /**
   * Gets gcs utils
   * @returns The firestore cache utils result
   */
  public getGcsUtils(): FirestoreCacheUtils {
    return this.gcsUtils;
  }

    /**
   * Processes entry
   * @param req - The HTTP request object
   * @returns A promise that resolves to the result
   */
  public async processEntry(req: ArchiveEntryRequest): Promise<ArchiveEntryResponse> {
    const { job_id: jobId, batchId, archive_s3_url: archiveS3Url, entry_name: entryName,
      entry_size: entrySize, field_spec: fieldSpec, password, archive_type: archiveType, nesting_depth: nestingDepth } = req;

    this.logger.info("archive_entry_processing", { job_id: jobId, entry_name: entryName, nesting_depth: nestingDepth, archive_type: archiveType });

    await markPendingEntryProcessing(jobId, entryName);

    try {
      const { s3Url, size } = await this.extractSingleRarEntry(jobId, archiveS3Url, entryName, password, fieldSpec);

      const [bucket, key] = this.gcsUtils.parseGcsUrl(s3Url);

      // Detect if the extracted entry is itself an archive.
      let detectedType: string | null = null;
      if (nestingDepth < settings.ARCHIVE_MAX_NESTING_DEPTH) {
        try {
          const header = await readRange(bucket, key, 0, 511);
          detectedType = detectArchiveType(header);
        } catch (e) {
          this.logger.warn("archive_entry_nested_detection_failed", { job_id: jobId, entry_name: entryName, error: String(e) });
        }
      }

      if (detectedType) {
        this.logger.info("archive_entry_nested_detected", { job_id: jobId, entry_name: entryName, detected_type: detectedType, depth: nestingDepth });
        try {
          const nestedEntries = await extractArchiveToS3(jobId, s3Url, detectedType, fieldSpec, batchId, password, nestingDepth + 1);
          // Remove the intermediate nested archive file from GCS.
          await gcsClient().bucket(bucket).file(key).delete().catch(() => {});
          for (const entry of nestedEntries) {
            if (entry.pending) {
              await createPendingArchiveEntry(jobId, entry.entry_name as string, entry.entry_size as number);
            } else {
              await publishEvent(makeJobEvent(EventType.ENTRY_DISCOVERED, jobId, "archive-entry-consumer", entry as Record<string, unknown>));
            }
          }
        } catch (err) {
          this.logger.error("archive_entry_nested_failed", { job_id: jobId, entry_name: entryName, error: String(err) });
          // Fall back: treat the extracted file as a regular parseable entry.
          await publishEvent(makeJobEvent(EventType.ENTRY_DISCOVERED, jobId, "archive-entry-consumer", {
            parent_job_id: jobId, batch_id: batchId, entry_s3_url: s3Url, entry_name: entryName, entry_size: size, field_spec: fieldSpec,
          }));
        }
      } else {
        await publishEvent(makeJobEvent(EventType.ENTRY_DISCOVERED, jobId, "archive-entry-consumer", {
          parent_job_id: jobId, batch_id: batchId, entry_s3_url: s3Url, entry_name: entryName, entry_size: size, field_spec: fieldSpec,
        }));
      }

      await markPendingEntryCompleted(jobId, entryName);
      this.logger.info("archive_entry_completed", { job_id: jobId, entry_name: entryName });
      return { success: true };
    } catch (err) {
      const errMsg = err instanceof Error ? err.message : String(err);
      this.logger.error("archive_entry_failed", { job_id: jobId, entry_name: entryName, error: errMsg });
      await markPendingEntryFailed(jobId, entryName, errMsg);
      return { success: false, error: errMsg };
    }
  }

  /**
   * Extract single RAR entry from archive
   */
  public async extractSingleRarEntry(
    jobId: string,
    archiveS3Url: string,
    entryName: string,
    password: string | undefined,
    fieldSpec: string[]
  ): Promise<{ s3Url: string; size: number }> {
    const [bucket, archiveKey] = this.gcsUtils.parseGcsUrl(archiveS3Url);
    const mountPath = process.env.RAR_TEMP_MOUNT || "/mnt/scratch";
    const tmpPath = path.join(mountPath, `${crypto.randomUUID()}.rar`);
  
    this.logger.info("archive_entry_download_start", { job_id: jobId, archive_s3_url: archiveS3Url, tmp_path: tmpPath });
  
    // Download archive to local mount
    const fileStream = this.gcsUtils.getStorage().bucket(bucket).file(archiveKey).createReadStream();
    const writeStream = createWriteStream(tmpPath);
  
    fileStream.on("error", (err) => {
      this.logger.error("archive_entry_download_stream_error", { job_id: jobId, error: err.message });
    });
  
    writeStream.on("error", (err) => {
      this.logger.error("archive_entry_download_write_error", { job_id: jobId, error: err.message });
    });
  
    await pipeline(fileStream, writeStream);
    this.logger.info("archive_entry_download_complete", { job_id: jobId, tmp_path: tmpPath });
  
    try {
      // Extract single entry using unrar
      const safeEntryName = entryName.replace(/[#\s]+/g, "_");
      const entryKey = `ingested/${jobId}/entries/${safeEntryName}`;
      const entryFile = this.gcsUtils.getStorage().bucket(bucket).file(entryKey);
      const writeStream = entryFile.createWriteStream();
      
      const extractArgs = ["p", "-inul", tmpPath, entryName];
      if (password) {
        extractArgs.push("-p" + password);
      }
      
      this.logger.info("archive_entry_extract_start", { job_id: jobId, entry_name: entryName, extract_args: extractArgs });
      const extractProcess = spawn("unrar", extractArgs);
      
      extractProcess.stdout.pipe(writeStream);
      
      // Capture stderr for debugging
      let stderrOutput = "";
      extractProcess.stderr.on("data", (data) => {
        stderrOutput += data.toString();
        this.logger.error("archive_entry_extract_stderr", { job_id: jobId, entry_name: entryName, stderr: data.toString() });
      });
      
      await new Promise<void>((resolve, reject) => {
        writeStream.on("finish", resolve);
        writeStream.on("error", (err) => {
          this.logger.error("archive_entry_extract_write_error", { job_id: jobId, entry_name: entryName, error: err.message });
          reject(err);
        });
        extractProcess.on("error", (err) => {
          this.logger.error("archive_entry_extract_spawn_error", { job_id: jobId, entry_name: entryName, error: err.message });
          reject(err);
        });
        extractProcess.on("close", (code) => {
          if (code !== 0) {
            this.logger.error("archive_entry_extract_failed", { job_id: jobId, entry_name: entryName, code, stderr: stderrOutput });
            reject(new Error(`unrar extraction failed with code ${code}: ${stderrOutput}`));
          } else {
            resolve();
          }
        });
      });
      
      this.logger.info("archive_entry_extract_complete", { job_id: jobId, entry_name: entryName });
      
      // Get the size of the extracted file
      const [meta] = await entryFile.getMetadata();
      const size = Number((meta as { size?: string | number }).size ?? 0);
      
      const s3Url = `gs://${bucket}/${entryKey}`;
      return { s3Url, size };
    } finally {
      // Clean up temporary archive file
      try {
        await fs.unlink(tmpPath);
        this.logger.info("archive_entry_cleanup", { job_id: jobId, tmp_path: tmpPath });
      } catch (err) {
        this.logger.warn("archive_entry_cleanup_failed", { job_id: jobId, error: String(err) });
      }
    }
  }
}

export default ArchiveEntryConsumerServiceImpl;
