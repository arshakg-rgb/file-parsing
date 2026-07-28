import crypto from "crypto";
import fs from "fs/promises";
import { createWriteStream } from "fs";
import { pipeline } from "stream/promises";
import path from "path";
import { spawn } from "child_process";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import FirestoreCacheUtils from "@utils/cache/FirestoreCacheUtils.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
import { startHealthCheckServer } from "@utils/response/health.js";
import { ArchiveEntryConsumerService } from "@service/archive_entry_consumer/ArchiveEntryConsumerService.js";
import { ArchiveEntryRequest, ArchiveEntryResponse, LogEvent, NestedArchiveEntry } from "@service/archive_entry_consumer/io/IArchiveEntryConsumer.js";
import { settings } from "@shared/Settings.js";
import { EventType, makeJobEvent } from "@shared/models/events.js";
import { publishEvent } from "@shared/QueueService.js";
import { readRange, gcsClient } from "@shared/GcsUtils.js";
import { markPendingEntryProcessing, markPendingEntryCompleted, markPendingEntryFailed, createPendingArchiveEntry } from "@shared/DatabaseManager.js";
import { extractArchiveToS3, detectArchiveType } from "@service/ingest/normalizer.js";
import {Readable} from "node:stream";

/**
 * ArchiveEntryConsumerServiceImpl is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */

class ArchiveEntryConsumerServiceImpl extends ServiceManager implements ArchiveEntryConsumerService
{
    /**
     * Singleton instance
     * @private
     */

    protected static instance: ArchiveEntryConsumerServiceImpl;

    /**
     * Logger instance
     * @private
     */

    private readonly logger: Logger;

    /**
     * Gcs Utils
     * @private
     */

    private readonly gcsUtils: FirestoreCacheUtils;

    /**
     * Local scratch mount used for temporary archive downloads. Read once at
     * construction rather than re-reading process.env on every extraction call.
     * @private
     */

    private readonly rarMountPath: string;

    /**
     * Constructs a new ArchiveEntryConsumerServiceImpl instance.
     * @param enforce - A function to enforce the Singleton pattern
     * @throws Error if instantiated directly
     */

    protected constructor(enforce: () => void)
    {
      if (enforce !== Enforce)
      {
        throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Cannot instantiate ArchiveEntryConsumerServiceImpl directly. Use getInstance()");
      }

      super(enforce);

      this.logger = createLogger("archive-entry-consumer");
      this.gcsUtils = FirestoreCacheUtils.getInstance();
      this.rarMountPath = process.env.RAR_TEMP_MOUNT || "/mnt/scratch";

      if (process.env.HEALTH_CHECK_PORT)
      {
        startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
      }
    }

    /**
     * Gets the single instance of the ArchiveEntryConsumerServiceImpl class.
     * @returns The single instance of the class
     */

    public static getInstance(): ArchiveEntryConsumerServiceImpl
    {
      if (!ArchiveEntryConsumerServiceImpl.instance)
      {
        ArchiveEntryConsumerServiceImpl.instance = new ArchiveEntryConsumerServiceImpl(Enforce);
      }

      return ArchiveEntryConsumerServiceImpl.instance;
    }

    /**
     * Gets logger
     * @returns The logger result
     */

    public getLogger(): Logger
    {
      return this.logger;
    }

    /**
     * Processes entry
     * @param req - The HTTP request object
     * @returns A promise that resolves to the result
     */

    public async processEntry(req: ArchiveEntryRequest): Promise<ArchiveEntryResponse>
    {
      const { job_id: jobId, batchId, archive_s3_url: archiveS3Url, entry_name: entryName, field_spec: fieldSpec, password, archive_type: archiveType, nesting_depth: nestingDepth } = req;

      this.logger.info(LogEvent.PROCESSING, { job_id: jobId, entry_name: entryName, nesting_depth: nestingDepth, archive_type: archiveType });

      await markPendingEntryProcessing(jobId, entryName);

      try
      {
        const { s3Url, size } = await this.extractSingleRarEntry(jobId, archiveS3Url, entryName, password);

        const [bucket, key] = this.gcsUtils.parseGcsUrl(s3Url);

        const detectedType: string | null = await this.tryDetectNestedArchiveType(jobId, entryName, bucket, key, nestingDepth);

        if (detectedType)
        {
          await this.handleNestedArchive(jobId, batchId, entryName, s3Url, bucket, key, detectedType, fieldSpec, password, nestingDepth, size);
        }
        else
        {
          await this.publishDiscoveredEntry(jobId, batchId, s3Url, entryName, size, fieldSpec);
        }

        await markPendingEntryCompleted(jobId, entryName);
        this.logger.info(LogEvent.COMPLETED, { job_id: jobId, entry_name: entryName });
        return { success: true };
      }
      catch (err)
      {
        const errMsg: string = err instanceof Error ? err.message : String(err);
        this.logger.error(LogEvent.FAILED, { job_id: jobId, entry_name: entryName, error: errMsg });
        await markPendingEntryFailed(jobId, entryName, errMsg);
        return { success: false, error: errMsg };
      }
    }

    /**
     * Attempts to detect whether the extracted entry is itself an archive that
     * should be recursed into, honoring the configured max nesting depth.
     * Detection failures are logged and treated as "not an archive" rather
     * than failing the whole entry.
     * @private
     */

    private async tryDetectNestedArchiveType(jobId: string, entryName: string, bucket: string, key: string, nestingDepth: number): Promise<string | null>
    {
      if (nestingDepth >= settings.ARCHIVE_MAX_NESTING_DEPTH)
      {
        return null;
      }

      try
      {
        const header: Buffer = await readRange(bucket, key, 0, 511);
        return detectArchiveType(header);
      }
      catch (e)
      {
        this.logger.warn(LogEvent.NESTED_DETECTION_FAILED, { job_id: jobId, entry_name: entryName, error: String(e) });
        return null;
      }
    }

    /**
     * Recurses into a nested archive, dispatching discovered entries, and
     * falls back to treating the extracted file as a regular parseable entry
     * if the nested extraction itself fails.
     * @private
     */

    private async handleNestedArchive(jobId: string, batchId: string, entryName: string, s3Url: string, bucket: string, key: string, detectedType: string, fieldSpec: ArchiveEntryRequest["field_spec"], password: string | undefined, nestingDepth: number, size: number,): Promise<void>
    {
      this.logger.info(LogEvent.NESTED_DETECTED, { job_id: jobId, entry_name: entryName, detected_type: detectedType, depth: nestingDepth });

      try
      {
        const nestedEntries: NestedArchiveEntry[] = await extractArchiveToS3(jobId, s3Url, detectedType, fieldSpec, batchId, password, nestingDepth + 1);

        await gcsClient().bucket(bucket).file(key).delete().catch((err) =>
        {
          this.logger.warn(LogEvent.NESTED_CLEANUP_FAILED, { job_id: jobId, entry_name: entryName, error: String(err) });
        });

        await Promise.all(
            nestedEntries.map((entry) =>
                entry.pending
                    ? createPendingArchiveEntry(jobId, entry.entry_name as string, entry.entry_size as number)
                    : publishEvent(makeJobEvent(EventType.ENTRY_DISCOVERED, jobId, "archive-entry-consumer", entry as Record<string, unknown>))
            ),
        );
      }
      catch (err)
      {
        this.logger.error(LogEvent.NESTED_FAILED, { job_id: jobId, entry_name: entryName, error: String(err) });
        await this.publishDiscoveredEntry(jobId, batchId, s3Url, entryName, size, fieldSpec);
      }
    }

    /**
     * Publishes an ENTRY_DISCOVERED event for a non-nested (or fallback) entry.
     * @private
     */

    private async publishDiscoveredEntry(jobId: string, batchId: string, s3Url: string, entryName: string, size: number, fieldSpec: unknown): Promise<void>
    {
      await publishEvent(makeJobEvent(EventType.ENTRY_DISCOVERED, jobId, "archive-entry-consumer", {
        parent_job_id: jobId, batch_id: batchId, entry_s3_url: s3Url, entry_name: entryName, entry_size: size, field_spec: fieldSpec,
      }));
    }

    /**
     * Extract single RAR entry from archive
     */

    public async extractSingleRarEntry(jobId: string, archiveS3Url: string, entryName: string, password: string | undefined): Promise<{ s3Url: string; size: number }>
    {
      const [bucket, archiveKey] = this.gcsUtils.parseGcsUrl(archiveS3Url);
      const tmpPath: string = path.join(this.rarMountPath, `${crypto.randomUUID()}.rar`);

      await this.downloadArchiveToLocal(jobId, bucket, archiveKey, tmpPath);

      try
      {
        return await this.extractEntryToGcs(jobId, bucket, entryName, tmpPath, password);
      }
      finally
      {
        await this.cleanupTempFile(jobId, tmpPath);
      }
    }

    /**
     * Downloads the archive from GCS to a local scratch path.
     * @private
     */

    private async downloadArchiveToLocal(jobId: string, bucket: string, archiveKey: string, tmpPath: string): Promise<void>
    {
      this.logger.info(LogEvent.DOWNLOAD_START, { job_id: jobId, archive_key: archiveKey, tmp_path: tmpPath });

      const fileStream: Readable = this.gcsUtils.getStorage().bucket(bucket).file(archiveKey).createReadStream();
      const writeStream = createWriteStream(tmpPath);

      fileStream.on("error", (err) =>
      {
        this.logger.error(LogEvent.DOWNLOAD_STREAM_ERROR, { job_id: jobId, error: err.message });
      });

      writeStream.on("error", (err) =>
      {
        this.logger.error(LogEvent.DOWNLOAD_WRITE_ERROR, { job_id: jobId, error: err.message });
      });

      await pipeline(fileStream, writeStream);
      this.logger.info(LogEvent.DOWNLOAD_COMPLETE, { job_id: jobId, tmp_path: tmpPath });
    }

    /**
     * Extracts a single entry from a local archive file and streams it to GCS.
     * @private
     */
    private async extractEntryToGcs(jobId: string, bucket: string, entryName: string, tmpPath: string, password: string | undefined): Promise<{ s3Url: string; size: number }>
    {
      const safeEntryName: string = entryName.replace(/[#\s]+/g, "_");
      const entryKey = `ingested/${jobId}/entries/${safeEntryName}`;
      const entryFile = this.gcsUtils.getStorage().bucket(bucket).file(entryKey);
      const writeStream = entryFile.createWriteStream();

      const extractArgs: string[] = ["p", "-inul", tmpPath, entryName];

      if (password)
      {
        extractArgs.push("-p" + password);
      }

      this.logger.info(LogEvent.EXTRACT_START, { job_id: jobId, entry_name: entryName, extract_args: extractArgs });
      const extractProcess = spawn("unrar", extractArgs);

      extractProcess.stdout.pipe(writeStream);
      extractProcess.stdout.on("error", (err) =>
      {
        this.logger.error(LogEvent.EXTRACT_STDOUT_ERROR, { job_id: jobId, entry_name: entryName, error: err.message });
      });

      let stderrOutput: string = "";
      extractProcess.stderr.on("data", (data) =>
      {
        stderrOutput += data.toString();
      });

      await this.awaitExtractionResult(jobId, entryName, extractProcess, writeStream, () => stderrOutput);

      this.logger.info(LogEvent.EXTRACT_COMPLETE, { job_id: jobId, entry_name: entryName });

      const [meta] = await entryFile.getMetadata();
      const size: number = Number((meta as { size?: string | number }).size ?? 0);

      return { s3Url: `gs://${bucket}/${entryKey}`, size };
    }

    /**
     * Waits for both the unrar process to close successfully AND the GCS write
     * stream to finish flushing before resolving.
     *
     * The original implementation raced these two conditions independently
     * (`writeStream.on("finish", resolve)` vs `extractProcess.on("close", ...)`),
     * which meant a non-zero exit code arriving *after* the write stream's
     * "finish" event could be silently ignored — the promise had already
     * resolved as a success. This version only settles once both signals are
     * in, so a failed extraction is never reported as a success.
     * @private
     */

    private awaitExtractionResult(jobId: string, entryName: string, extractProcess: ReturnType<typeof spawn>, writeStream: NodeJS.WritableStream, getStderr: () => string,): Promise<void>
    {
      return new Promise<void>((resolve, reject) =>
      {
        let settled: boolean = false;
        let processClosed: boolean = false;
        let writeFinished: boolean = false;
        let closeCode: number | null = null;

        const fail = (err: Error): void =>
        {
          if (settled) return;
          settled = true;
          reject(err);
        };

        const trySettle = (): void =>
        {
          if (settled || !processClosed || !writeFinished)
          {
            return;
          }

          if (closeCode !== 0)
          {
            this.logger.error(LogEvent.EXTRACT_FAILED, { job_id: jobId, entry_name: entryName, code: closeCode, stderr: getStderr() });
            fail(new Error(`unrar extraction failed with code ${closeCode}: ${getStderr()}`));
            return;
          }

          settled = true;
          resolve();
        };

        writeStream.once("finish", () =>
        {
          writeFinished = true;
          trySettle();
        });

        writeStream.once("error", (err: Error) =>
        {
          this.logger.error(LogEvent.EXTRACT_WRITE_ERROR, { job_id: jobId, entry_name: entryName, error: err.message });
          fail(err);
        });

        extractProcess.once("error", (err: Error) =>
        {
          this.logger.error(LogEvent.EXTRACT_SPAWN_ERROR, { job_id: jobId, entry_name: entryName, error: err.message });
          fail(err);
        });

        extractProcess.once("close", (code) =>
        {
          processClosed = true;
          closeCode = code;
          trySettle();
        });
      });
    }

    /**
     * Removes the temporary local archive file, best effort.
     * @private
     */

    private async cleanupTempFile(jobId: string, tmpPath: string): Promise<void>
    {
      try
      {
        await fs.unlink(tmpPath);
        this.logger.info(LogEvent.CLEANUP, { job_id: jobId, tmp_path: tmpPath });
      }
      catch (err)
      {
        this.logger.warn(LogEvent.CLEANUP_FAILED, { job_id: jobId, error: String(err) });
      }
    }
}

export default ArchiveEntryConsumerServiceImpl;
