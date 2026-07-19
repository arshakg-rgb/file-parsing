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
    // This is a placeholder - the actual implementation would be more complex
    // For now, we'll just delegate to the existing extractSingleRarEntry method
    const result = await this.extractSingleRarEntry(
      req.job_id,
      req.s3_url,
      req.entry_path,
      req.password ? req.password.toString() : undefined,
      []
    );
    return { success: true };
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
