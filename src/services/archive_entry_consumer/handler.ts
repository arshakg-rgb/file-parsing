import crypto from "crypto";
import fs from "fs/promises";
import { createWriteStream } from "fs";
import { pipeline } from "stream/promises";
import os from "os";
import path from "path";
import { spawn } from "child_process";
import Config from "../../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../../config/ServiceManager.js";
import { InstantiationError } from "../../errors/InstantiationError.js";
import FirestoreCacheUtils from "../../utils/cache/FirestoreCacheUtils.js";
import { receiveMessages, deleteMessage, publishEvent, sendRaw } from "../../shared/queueUtils.js";
import { EventType, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus } from "../../shared/models/job.js";
import { markPendingEntryCompleted, markPendingEntryFailed, markPendingEntryProcessing, getPendingEntryCount, getPendingEntryTotalSize, getJob } from "../../shared/db.js";
import { createLogger } from "../../shared/logger.js";
import { metrics } from "../../shared/metrics.js";
import { detectArchiveType, extractArchiveToS3, BombError } from "../ingest/normalizer.js";
import { startHealthCheckServer } from "../../shared/health.js";
import MySqlManager from "../../config/db/MySqlManager.js";

class ArchiveEntryConsumerService extends ServiceManager {
  protected static instance: ArchiveEntryConsumerService;
  private logger: any;
  private gcsUtils: FirestoreCacheUtils;
  private passwordCache: Map<string, Buffer>;
  private passwordAttempts: Map<string, number>;
  private MAX_RETRIES = 3;
  private RETRY_DELAY_MS = 5 * 60 * 1000;
  private MAX_TOTAL_UNCOMPRESSED = 10 * 1024 * 1024 * 1024;
  private CONCURRENT_MESSAGES = 3;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate ArchiveEntryConsumerService directly. Use getInstance()");
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

  public static getInstance(): ArchiveEntryConsumerService {
    if (!ServiceManager.instance) {
      ServiceManager.instance = new ArchiveEntryConsumerService(Enforce);
    }
    return ServiceManager.instance as ArchiveEntryConsumerService;
  }

  public getLogger(): any {
    return this.logger;
  }

  public getGcsUtils(): FirestoreCacheUtils {
    return this.gcsUtils;
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
    const mountPath = process.env.RAR_TEMP_MOUNT || '/mnt/scratch';
    const tmpPath = path.join(mountPath, `${crypto.randomUUID()}.rar`);
  
    this.logger.info("archive_entry_download_start", { job_id: jobId, archive_s3_url: archiveS3Url, tmp_path: tmpPath });
  
    // Download archive to local mount
    const fileStream = this.gcsUtils.getStorage().bucket(bucket).file(archiveKey).createReadStream();
    const writeStream = createWriteStream(tmpPath);
  
    fileStream.on('error', (err) => {
      this.logger.error("archive_entry_download_stream_error", { job_id: jobId, error: err.message });
    });
  
    writeStream.on('error', (err) => {
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
      
      const extractArgs = ['p', '-inul', tmpPath, entryName];
      if (password) {
        extractArgs.push('-p' + password);
      }
      
      this.logger.info("archive_entry_extract_start", { job_id: jobId, entry_name: entryName, extract_args: extractArgs });
      const extractProcess = spawn('unrar', extractArgs);
      
      extractProcess.stdout.pipe(writeStream);
      
      // Capture stderr for debugging
      let stderrOutput = '';
      extractProcess.stderr.on('data', (data) => {
        stderrOutput += data.toString();
        this.logger.error("archive_entry_extract_stderr", { job_id: jobId, entry_name: entryName, stderr: data.toString() });
      });
      
      await new Promise<void>((resolve, reject) => {
        writeStream.on('finish', resolve);
        writeStream.on('error', (err) => {
          this.logger.error("archive_entry_extract_write_error", { job_id: jobId, entry_name: entryName, error: err.message });
          reject(err);
        });
        extractProcess.on('error', (err) => {
          this.logger.error("archive_entry_extract_spawn_error", { job_id: jobId, entry_name: entryName, error: err.message });
          reject(err);
        });
        extractProcess.on('close', (code) => {
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
      const size = Number((meta as any).size ?? 0);
      
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

interface ArchiveEntryMessage {
  job_id: string;
  batch_id: string;
  archive_s3_url: string;
  entry_name: string;
  entry_size: number;
  field_spec: string[];
  password?: string;
  archive_type: string;
  nesting_depth?: number;
}


export default ArchiveEntryConsumerService;

// Backward compatibility wrappers
const archiveService = ArchiveEntryConsumerService.getInstance();

export async function extractSingleRarEntry(
  jobId: string,
  archiveS3Url: string,
  entryName: string,
  password: string | undefined,
  fieldSpec: string[]
): Promise<{ s3Url: string; size: number }> {
  return archiveService.extractSingleRarEntry(jobId, archiveS3Url, entryName, password, fieldSpec);
}
