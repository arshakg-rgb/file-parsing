import { randomUUID } from "crypto";
import { createWriteStream } from "fs";
import { pipeline } from "stream/promises";
import path from "path";
import { spawn } from "child_process";
import pino from "pino";
import { settings } from "@shared/Settings.js";
import { objectSize, readRange, gcsClient } from "@shared/GcsUtils.js";
import { sendRaw } from "@shared/QueueService.js";
import { createLogger } from "@utils/logger/Log.js";
import { GcsEntryStore } from "../GcsEntryStore.js";
import { ArchiveTypeDetector } from "../ArchiveTypeDetector.js";
import { TempFileManager } from "../TempFileManager.js";
import { RAR_MAX_ARCHIVE_SIZE, RAR_MAX_INLINE_FILE_SIZE, RAR_MAX_TOTAL_UNCOMPRESSED, RarFileEntry } from "@service/ingest/io/IIngest";
import {Readable} from "node:stream";
import {IngestServiceImpl} from "@service/ingest/IngestServiceImpl";
import {InstantiationError} from "@errors/InstantiationError";
import {DatabaseService} from "@shared/DatabaseManager";

const logger: pino.Logger = createLogger(module);

/**
 * RAR extraction runs full-file CLI-based (unrar) extraction rather than the
 * library approach used for the other formats, to keep memory usage constant
 * regardless of file size — the library approach buffers internally and
 * risks OOM.
 *
 * The class is split into small private methods: downloading the archive,
 * listing its contents, routing oversized entries to async processing, and
 * extracting each remaining entry (with nested-archive detection) directly
 * to GCS.
 */

export class RarExtractor
{
    private static instance: RarExtractor;

    private readonly entryStore: GcsEntryStore;

    private constructor(enforce: () => void)
    {
        if (enforce !== Enforce)
        {
            throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use RarExtractor.getInstance() instead of new.");
        }

        this.entryStore = GcsEntryStore.getInstance();
    }

    /**
     * Gets the singleton instance of RarExtractor.
     *
     * @returns The singleton instance of RarExtractor.
     */

    static getInstance(): RarExtractor
    {
        if (!RarExtractor.instance)
        {
            RarExtractor.instance = new RarExtractor(Enforce);
        }

        return RarExtractor.instance;
    }

    /**
     * Parse `unrar lt -v` (technical listing) or classic table-format output
     * into a list of file entries, skipping directories.
     * @param output - Raw stdout from `unrar lt`
     */

    private parseUnrarListing(output: string): RarFileEntry[] {
        const files: RarFileEntry[] = [];

        if (output.includes("Name:") && output.includes("Size:"))
        {
            const blocks: string[] = output.split(/\r?\n\r?\n/);

            for (const block of blocks
                ) {
                const nameMatch: RegExpMatchArray = block.match(/^\s*Name:\s*(.+)$/m);
                const sizeMatch: RegExpMatchArray = block.match(/^\s*Size:\s*(\d+)$/m);
                const typeMatch: RegExpMatchArray = block.match(/^\s*Type:\s*(.+)$/m);

                if (nameMatch && sizeMatch && (!typeMatch || !/directory/i.test(typeMatch[1])))
                {
                    files.push({ name: nameMatch[1].trim(), size: parseInt(sizeMatch[1], 10) });
                }
            }
            return files;
        }

        for (const line of output.split("\n"))
        {
            const match: RegExpMatchArray = line.match(/^\s+(\.\.A\.\.\.\.)\s+(\d+)\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}\s+(.+)$/);

            if (match)
            {
                const size: number = parseInt(match[2], 10);
                const name: string = match[3].trim();
                if (name && name.length > 0) {
                    files.push({ name, size });
                }
            }
        }
        return files;
    }

    /**
     * Run `unrar lt -v` against a local RAR file and return its parsed listing.
     * @param tmpPath - Path to the local RAR file
     * @param password - Optional archive password
     * @param jobId - Job identifier, for logging
     */

    private async listRarContents(tmpPath: string, password: string | undefined, jobId: string): Promise<RarFileEntry[]>
    {
        const listArgs: string[] = ["lt", "-v", tmpPath];

        if (password)
        {
            listArgs.push("-p" + password);
        }

        logger.info("rar_list_starting", { jobId, args: listArgs });

        const listProcess = spawn("unrar", listArgs);
        let listOutput: string = "";
        let listError: string = "";

        listProcess.stdout.on("data", (data) => {
            listOutput += data.toString();
        });

        listProcess.stderr.on("data", (data) => {
            listError += data.toString();
            logger.error("rar_list_stderr", { jobId, data: data.toString() });
        });

        await new Promise<void>((resolve, reject) =>
        {
            listProcess.on("close", (code) => {
                logger.info("rar_list_complete", { jobId, code, outputLength: listOutput.length, errorLength: listError.length });

                if (code === 0)
                {
                    resolve();
                }

                else reject(new Error(`unrar list failed with code ${code}: ${listError}`));
            });

            listProcess.on("error", (err) => {
                logger.error("rar_list_spawn_error", { jobId, error: err.message });
                reject(err);
            });
        });

        const files: RarFileEntry[] = this.parseUnrarListing(listOutput);
        logger.info("rar_list_parsed", { jobId, fileCount: files.length, files: files.map((f) => ({ name: f.name, size: f.size })) });

        if (files.length === 0 && !/no files to extract|0 files? found/i.test(listOutput) && listOutput.trim().length > 200)
        {
            logger.error("rar_parse_suspicious_empty", { jobId, outputLength: listOutput.length, sampleOutput: listOutput.substring(0, 500) });
            throw new Error(`RAR listing parse produced 0 files but unrar output was non-trivial (${listOutput.length} chars) — parser likely broken, not an empty archive`);
        }

        return files;
    }

    /**
     * Download a RAR archive from GCS to a local temp path (GCS FUSE mount, not
     * RAM-backed /tmp, since the archive can be sizeable).
     * @param bucket - GCS bucket
     * @param key - GCS object key
     * @param jobId - Job identifier, for logging
     */

    private async downloadRarToTemp(bucket: string, key: string, jobId: string): Promise<string>
    {
        const mountPath: string = process.env.RAR_TEMP_MOUNT || "/mnt/scratch";
        const tmpPath: string = path.join(mountPath, `${randomUUID()}.rar`);

        logger.info("rar_download_starting", { jobId, tmpPath, mountPath });

        const fileStream: Readable = gcsClient().bucket(bucket).file(key).createReadStream();
        const writeStream = createWriteStream(tmpPath);

        fileStream.on("error", (err) => logger.error("rar_download_stream_error", { jobId, error: err.message }));
        writeStream.on("error", (err) => logger.error("rar_download_write_error", { jobId, error: err.message }));

        await pipeline(fileStream, writeStream);

        logger.info("rar_download_complete", { jobId, tmpPath });
        return tmpPath;
    }

    /**
     * Route an oversized RAR entry to asynchronous processing rather than
     * extracting it inline. Idempotent: the pending-entry record is created
     * before the queue message is sent, so retried jobs don't double-enqueue.
     * @returns The pending entry event to report for this file
     */

    private async routeRarFileToAsync(jobId: string, batchId: string, s3Url: string, file: RarFileEntry, fieldSpec: string[], password: string | undefined, depth: number): Promise<Record<string, unknown>>
    {
        logger.info("rar_route_to_async", { jobId, name: file.name, size: file.size, threshold: settings.LARGE_FILE_THRESHOLD_BYTES });

        try
        {
            const created: boolean = await DatabaseService.getInstance().createPendingArchiveEntry(jobId, file.name, file.size);

            if (created)
            {
                await sendRaw(settings.ARCHIVE_ENTRY_QUEUE_URL, {
                    job_id: jobId,
                    batchId: batchId,
                    archive_s3_url: s3Url,
                    entry_name: file.name,
                    entry_size: file.size,
                    field_spec: fieldSpec,
                    password: password || undefined,
                    archive_type: "rar",
                    nesting_depth: depth,
                });
            }
            else
            {
                logger.info("rar_pending_entry_exists", { jobId, name: file.name });
            }
        }
        catch (exc)
        {
            logger.error("rar_route_to_async_failed", { jobId, name: file.name, error: exc instanceof Error ? exc.message : String(exc) });
        }

        return { parent_job_id: jobId, batch_id: batchId, entry_s3_url: null, entry_name: file.name, entry_size: file.size, field_spec: fieldSpec, pending: true };
    }

    /**
     * Extract a single RAR entry directly to GCS via CLI, streaming output with
     * OS-level backpressure. If the extracted file is itself an archive and the
     * nesting depth allows it, recurses via IngestService.
     * @returns Entry event(s) for this file — normally one, or several if it was a nested archive
     */

    private async extractRarFileInline(jobId: string, batchId: string, bucket: string, tmpPath: string, file: RarFileEntry, fieldSpec: string[], password: string | undefined, depth: number): Promise<Record<string, unknown>[]>
    {
        logger.info("rar_extracting_file", { jobId, name: file.name, size: file.size });

        const entryKey = `archive/${jobId}/${file.name}`;
        const entryFile = gcsClient().bucket(bucket).file(entryKey);
        const writeStream = entryFile.createWriteStream();

        const extractArgs: string[] = ["p", "-inul", tmpPath, file.name];

        if (password)
        {
            extractArgs.push("-p" + password);
        }

        const extractProcess = spawn("unrar", extractArgs);
        extractProcess.stdout.pipe(writeStream);

        await new Promise<void>((resolve, reject) =>
        {
            writeStream.on("finish", resolve);
            writeStream.on("error", reject);
            extractProcess.on("error", reject);
            extractProcess.on("close", (code) => {
                if (code !== 0)
                {
                    reject(new Error(`unrar extraction failed with code ${code}`));
                }

                else resolve();
            });
        });

        const entryUrl = `gs://${bucket}/${entryKey}`;

        let detectedArchiveType: string | null = null;

        try
        {
            const header: Buffer = await readRange(bucket, entryKey, 0, 511);
            detectedArchiveType = ArchiveTypeDetector.detect(header);
        }
        catch (err)
        {
            logger.error("nested_detection_failed", { jobId, name: file.name, error: err instanceof Error ? err.message : String(err) });
        }


        let result: Record<string, unknown>[];

        if (detectedArchiveType && depth < settings.ARCHIVE_MAX_NESTING_DEPTH)
        {
            logger.info("rar_nested_archive_detected_sync", { jobId, name: file.name, detected_type: detectedArchiveType, depth });
            try
            {
                const nestedEntries: Record<string, unknown>[] = await IngestServiceImpl.getInstance().extractArchiveToS3(
                    jobId,
                    entryUrl,
                    detectedArchiveType,
                    fieldSpec,
                    batchId,
                    password,
                    depth + 1
                );
                await gcsClient().bucket(bucket).file(entryKey).delete();
                result = nestedEntries;
            }
            catch (err)
            {
                logger.error("nested_extraction_failed", { jobId, name: file.name, error: err instanceof Error ? err.message : String(err) });
                result = [this.entryStore.makeEntryEvent(jobId, batchId, entryUrl, file.name, file.size, fieldSpec)];
            }
        }
        else
        {
            result = [this.entryStore.makeEntryEvent(jobId, batchId, entryUrl, file.name, file.size, fieldSpec)];
        }

        logger.info("rar_extracted_file", { jobId, name: file.name, size: file.size });
        return result;
    }

    /**
     * Extract a RAR archive via CLI-based (unrar) processing, enforcing the
     * constant-memory architecture principle: full-file download at a bounded
     * size cap, streaming list/extract via child processes, oversized entries
     * routed to async processing, and nested-archive recursion.
     */

    async extract(jobId: string, s3Url: string, bucket: string, key: string, fieldSpec: string[], batchId: string, password: string | undefined, depth: number): Promise<Record<string, unknown>[]>
    {
        const size: number = await objectSize(bucket, key);
        logger.info("rar_streaming_extract", { jobId, bucket, key, size });

        if (size > RAR_MAX_ARCHIVE_SIZE)
        {
            throw new Error(`RAR file size ${size} bytes exceeds maximum ${RAR_MAX_ARCHIVE_SIZE} bytes. RAR format requires full file access which violates constant memory principle for very large files. Consider using ZIP/7z/tar formats for large archives (they support true streaming).`);
        }

        const tmpPath: string = await this.downloadRarToTemp(bucket, key, jobId);
        const out: Record<string, unknown>[] = [];
        let totalUncompressed: number = 0;

        try
        {
            const files: RarFileEntry[] = await this.listRarContents(tmpPath, password, jobId);

            for (const file of files) {
                if (file.size > settings.LARGE_FILE_THRESHOLD_BYTES)
                {
                    out.push(await this.routeRarFileToAsync(jobId, batchId, s3Url, file, fieldSpec, password, depth));
                    continue;
                }

                if (file.size > RAR_MAX_INLINE_FILE_SIZE) {
                    logger.info("rar_skip_large_file", { jobId, name: file.name, size: file.size });
                    continue;
                }

                if (totalUncompressed + file.size > RAR_MAX_TOTAL_UNCOMPRESSED) {
                    logger.info("rar_skip_total_limit", { jobId, name: file.name });
                    continue;
                }

                try
                {
                    const entries: Record<string, unknown>[] = await this.extractRarFileInline(jobId, batchId, bucket, tmpPath, file, fieldSpec, password, depth);
                    out.push(...entries);
                    totalUncompressed += file.size;
                }
                catch (exc)
                {
                    logger.error("rar_extract_file_failed", { jobId, name: file.name, error: exc instanceof Error ? exc.message : String(exc) });
                }
            }

            logger.info("rar_extraction_complete", { jobId, totalFiles: files.length, totalUncompressed });
        }
        finally
        {
            await TempFileManager.removeFile(tmpPath);
        }

        return out;
    }
}

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void
{
}
