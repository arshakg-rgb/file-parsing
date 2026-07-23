import { randomUUID } from "crypto";
import fs from "fs/promises";
import { createWriteStream } from "fs";
import { pipeline } from "stream/promises";
import os from "os";
import path from "path";
import zlib from "zlib";
import { promisify } from "util";
import NodeStreamZip from "node-stream-zip";
import { extract as extractTar } from "tar";
import Seven from "node-7z";
import { once } from "node:events";
import { settings } from "@shared/Settings.js";
import { parseGcsUrl as parseS3Url, objectSize, readFull, readRange, putObject, listObjects, copyObject, gcsClient } from "@shared/GcsUtils.js";
import { pool, createPendingArchiveEntry } from "@shared/DatabaseManager.js";
import { fetchUrlStream } from "./ssrf_guard.js";
import { sendRaw } from "@shared/QueueService.js";

/**
 * The gunzip
 */
const gunzip = promisify(zlib.gunzip);

/**
 * Class representing a bomb error error.
 */
export class BombError extends Error {
    /**
   * Constructs a new BombError instance.
   * @param message - The message
   */
  constructor(message: string) {
    super(message);
    this.name = "BombError";
  }
}

/**
 * Fetches url to s3
 * @param jobId - The job identifier
 * @param url - The URL to process
 * @returns A promise that resolves to the result
 */
export async function fetchUrlToS3(jobId: string, url: string): Promise<[string, number]> {
  // Handle gs:// URLs directly using GCS utilities
  if (url.startsWith("gs://")) {
    const [bucket, key] = parseS3Url(url);
    const size = await objectSize(bucket, key);
    const s3Key = `ingested/${jobId}/source`;
    
    // Use streaming for large files to avoid OOM
    if (size > settings.SMALL_FILE_SINGLE_GET_THRESHOLD) {
      console.log("gcs_streaming_copy", { jobId, size, threshold: settings.SMALL_FILE_SINGLE_GET_THRESHOLD });
      await streamGcsToGcs(bucket, key, settings.DATA_BUCKET, s3Key);
    } else {
      // Small files: use readFull for efficiency
      const data = await readFull(bucket, key);
      await putObject(settings.DATA_BUCKET, s3Key, data);
    }
    
    const s3Url = `gs://${settings.DATA_BUCKET}/${s3Key}`;
    console.log("gcs_copied_to_gcs", { jobId, s3Url, bytes: size });
    return [s3Url, size];
  }
  
  // Handle HTTP/HTTPS URLs using fetch
  const s3Key = `ingested/${jobId}/source`;
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of fetchUrlStream(url)) {
    total += chunk.length;
    chunks.push(chunk);
  }
  const body = Buffer.concat(chunks);
  await putObject(settings.DATA_BUCKET, s3Key, body);
  const s3Url = `gs://${settings.DATA_BUCKET}/${s3Key}`;
  console.log("url_fetched_to_gcs", { jobId, s3Url, bytes: total });
  return [s3Url, total];
}

// Stream GCS object to another GCS location using GCS copy (server-side, no memory)
async function streamGcsToGcs(srcBucket: string, srcKey: string, dstBucket: string, dstKey: string): Promise<void> {
  await copyObject(srcBucket, srcKey, dstBucket, dstKey);
  console.log("gcs_copy_complete", { srcBucket, srcKey, dstBucket, dstKey });
}

/**
 * Performs the list s3 prefix operation.
 * @param prefixUrl - The prefix url
 * @returns A promise that resolves to the list
 */
export async function listS3Prefix(prefixUrl: string): Promise<[string, number][]> {
  const [bucket, prefix] = parseS3Url(prefixUrl);
  return listObjects(bucket, prefix);
}

/**
 * The m a g i c_ z i p
 */
const MAGIC_ZIP = Buffer.from("PK\x03\x04");
/**
 * The m a g i c_ g z
 */
const MAGIC_GZ = Buffer.from("\x1f\x8b");
/**
 * The m a g i c_7 z
 */
const MAGIC_7Z = Buffer.from("7z\xbc\xaf\x27\x1c");
/**
 * The m a g i c_ r a r
 */
const MAGIC_RAR = Buffer.from("Rar!");

/**
 * Detects archive type
 * @param header - The header
 * @returns The string | null result
 */
export function detectArchiveType(header: Buffer): string | null {
  if (header.slice(0, 4).equals(MAGIC_ZIP)) return "zip";
  if (header.slice(0, 2).equals(MAGIC_GZ)) return "gz";
  if (header.slice(0, 6).equals(MAGIC_7Z)) return "7z";
  if (header.slice(0, 4).equals(MAGIC_RAR)) return "rar";
  if (header.length > 262 && header.slice(257, 262).toString() === "ustar") return "tar";
  return null;
}

/**
 * Extracts archive to s3
 * @param jobId - The job identifier
 * @param s3Url - The s3 url
 * @param archiveType - The archive type
 * @param fieldSpec - The field spec
 * @param batchId - The batch identifier
 * @param password - The password
 * @param _depth - The _depth
 * @returns A promise that resolves to the list
 */
export async function extractArchiveToS3(
  jobId: string,
  s3Url: string,
  archiveType: string,
  fieldSpec: string[],
  batchId: string,
  password?: string,
  _depth = 0
): Promise<Record<string, unknown>[]> {
  if (_depth > settings.ARCHIVE_MAX_NESTING_DEPTH) {
    throw new BombError(`Archive nesting depth ${_depth} exceeds maximum ${settings.ARCHIVE_MAX_NESTING_DEPTH}`);
  }
  const [bucket, key] = parseS3Url(s3Url);

  // For RAR: use CLI-based extraction to avoid library OOM issues
  // Architecture requirement: constant memory usage regardless of file size
  // CLI approach provides real OS-level backpressure and avoids library internal buffering
  if (archiveType === "rar") {
    const size = await objectSize(bucket, key);
    console.log("rar_streaming_extract", { jobId, bucket, key, size });
    
    // Enforce size limit to maintain constant memory principle per architecture
    // 4Gi memory + GCS FUSE overhead + CLI process overhead = ~2.5GB practical limit
    const MAX_RAR_SIZE = 2.5 * 1024 * 1024 * 1024; // 2.5GB limit for RAR with 4Gi memory + GCS FUSE
    if (size > MAX_RAR_SIZE) {
      throw new Error(`RAR file size ${size} bytes exceeds maximum ${MAX_RAR_SIZE} bytes. RAR format requires full file access which violates constant memory principle for very large files. Consider using ZIP/7z/tar formats for large archives (they support true streaming).`);
    }
    
    // Use GCS FUSE mount path instead of RAM-backed /tmp
    const mountPath = process.env.RAR_TEMP_MOUNT || "/mnt/scratch";
    const tmpPath = path.join(mountPath, `${randomUUID()}.rar`);
    console.log("rar_download_starting", { jobId, tmpPath, mountPath });
    const fileStream = gcsClient().bucket(bucket).file(key).createReadStream();
    const writeStream = createWriteStream(tmpPath);
    
    fileStream.on("error", (err) => {
      console.error("rar_download_stream_error", { jobId, error: err.message });
    });
    
    writeStream.on("error", (err) => {
      console.error("rar_download_write_error", { jobId, error: err.message });
    });
    
    await pipeline(fileStream, writeStream);
    console.log("rar_download_complete", { jobId, tmpPath, size });
    
    // Use CLI-based extraction for memory efficiency
    const { spawn } = await import("child_process");
    const out: Record<string, unknown>[] = [];
    let totalUncompressed = 0;
    const MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024;
    const MAX_TOTAL_UNCOMPRESSED = 10 * 1024 * 1024 * 1024;
    
    try {
      // First, list archive contents to get file info
      // Use technical listing mode (lt -v) for stable Key: value format instead of human-readable table
      const listArgs = ["lt", "-v", tmpPath];
      if (password) {
        listArgs.push("-p" + password);
      }
      
      console.log("rar_list_starting", { jobId, args: listArgs });
      const listProcess = spawn("unrar", listArgs);
      let listOutput = "";
      let listError = "";
      
      listProcess.stdout.on("data", (data) => {
        listOutput += data.toString();
      });
      
      listProcess.stderr.on("data", (data) => {
        listError += data.toString();
        console.error("rar_list_stderr", { jobId, data: data.toString() });
      });
      
      await new Promise<void>((resolve, reject) => {
        listProcess.on("close", (code) => {
          console.log("rar_list_complete", { jobId, code, outputLength: listOutput.length, errorLength: listError.length });
          if (code === 0) {
            resolve();
          } else {
            reject(new Error(`unrar list failed with code ${code}: ${listError}`));
          }
        });
        listProcess.on("error", (err) => {
          console.error("rar_list_spawn_error", { jobId, error: err.message });
          reject(err);
        });
      });
      
      // Parse unrar list output (handle both table and technical listing formats)
      function parseUnrarListing(output: string): Array<{ name: string; size: number }> {
        const files: Array<{ name: string; size: number }> = [];
        const lines = output.split("\n");
        
        // Check if output is in technical listing format (Name:/Size: blocks)
        if (output.includes("Name:") && output.includes("Size:")) {
          const blocks = output.split(/\r?\n\r?\n/);
          for (const block of blocks) {
            const nameMatch = block.match(/^\s*Name:\s*(.+)$/m);
            const sizeMatch = block.match(/^\s*Size:\s*(\d+)$/m);
            const typeMatch = block.match(/^\s*Type:\s*(.+)$/m);
            
            if (nameMatch && sizeMatch && (!typeMatch || !/directory/i.test(typeMatch[1]))) {
              files.push({ name: nameMatch[1].trim(), size: parseInt(sizeMatch[1], 10) });
            }
          }
        } else {
          // Parse table format: "  ..A....  12345678  2025-12-28 12:45  filename.ext"
          for (const line of lines) {
            // Match lines that start with attributes and have size + date + time + filename
            const match = line.match(/^\s+(\.\.A\.\.\.\.)\s+(\d+)\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}\s+(.+)$/);
            if (match) {
              const size = parseInt(match[2], 10);
              const name = match[3].trim();
              if (name && name.length > 0) {
                files.push({ name, size });
              }
            }
          }
        }
        
        return files;
      }
      
      const files = parseUnrarListing(listOutput);
      
      console.log("rar_list_parsed", { jobId, fileCount: files.length, files: files.map(f => ({ name: f.name, size: f.size })) });
      
      // Sanity check: distinguish empty archive from parser failure
      if (files.length === 0) {
        if (!/no files to extract|0 files? found/i.test(listOutput) && listOutput.trim().length > 200) {
          console.error("rar_parse_suspicious_empty", { jobId, outputLength: listOutput.length, sampleOutput: listOutput.substring(0, 500) });
          throw new Error(`RAR listing parse produced 0 files but unrar output was non-trivial (${listOutput.length} chars) — parser likely broken, not an empty archive`);
        }
      }
      
      // Extract each file using CLI with streaming to GCS
      for (const file of files) {
        // Route large files to async extraction BEFORE the hard-cap check
        // This allows files > 2GB to be processed asynchronously instead of being skipped
        if (file.size > settings.LARGE_FILE_THRESHOLD_BYTES) {
          console.log("rar_route_to_async", { jobId, name: file.name, size: file.size, threshold: settings.LARGE_FILE_THRESHOLD_BYTES });
          
          try {
            // Insert pending entry synchronously BEFORE sendRaw to provide idempotency
            // This prevents duplicate queue messages when jobs are retried due to Cloud Rollout SIGTERM
            const created = await createPendingArchiveEntry(jobId, file.name, file.size);

            if (created) {
              await sendRaw(settings.ARCHIVE_ENTRY_QUEUE_URL, {
                job_id: jobId,
                batchId: batchId,
                archive_s3_url: s3Url,
                entry_name: file.name,
                entry_size: file.size,
                field_spec: fieldSpec,
                password: password || undefined,
                archive_type: "rar",
                nesting_depth: _depth,
              });
            } else {
              console.log("rar_pending_entry_exists", { jobId, name: file.name });
            }
          } catch (exc) {
            console.error("rar_route_to_async_failed", { jobId, name: file.name, error: exc instanceof Error ? exc.message : String(exc) });
            // Continue processing remaining files instead of aborting the entire batch
          }
          
          // Track as pending entry for job status
          out.push({ parent_job_id: jobId, batch_id: batchId, entry_s3_url: null, entry_name: file.name, entry_size: file.size, field_spec: fieldSpec, pending: true });
          continue;
        }
        
        // Hard cap now only applies to files staying on the inline/synchronous path
        if (file.size > MAX_FILE_SIZE) {
          console.log("rar_skip_large_file", { jobId, name: file.name, size: file.size });
          continue;
        }
        
        if (totalUncompressed + file.size > MAX_TOTAL_UNCOMPRESSED) {
          console.log("rar_skip_total_limit", { jobId, name: file.name });
          continue;
        }
        
        console.log("rar_extracting_file", { jobId, name: file.name, size: file.size });
        
        try {
          const entryKey = `archive/${jobId}/${file.name}`;
          const entryFile = gcsClient().bucket(bucket).file(entryKey);
          const writeStream = entryFile.createWriteStream();
          
          const extractArgs = ["p", "-inul", tmpPath, file.name];
          if (password) {
            extractArgs.push("-p" + password);
          }
          
          const extractProcess = spawn("unrar", extractArgs);
          
          // Pipe extraction output directly to GCS with backpressure
          extractProcess.stdout.pipe(writeStream);
          
          await new Promise<void>((resolve, reject) => {
            writeStream.on("finish", resolve);
            writeStream.on("error", reject);
            extractProcess.on("error", reject);
            extractProcess.on("close", (code) => {
              if (code !== 0) {
                reject(new Error(`unrar extraction failed with code ${code}`));
              } else {
                resolve();
              }
            });
          });
          
          totalUncompressed += file.size;
          const entryUrl = `gs://${bucket}/${entryKey}`;
          
          // Detect if extracted file is itself an archive (nested archive handling)
          let detectedArchiveType: string | null = null;
          try {
            const header = await readRange(bucket, entryKey, 0, 511);
            detectedArchiveType = detectArchiveType(header);
          } catch (err) {
            console.error("nested_detection_failed", { jobId, name: file.name, error: err instanceof Error ? err.message : String(err) });
          }
          
          if (detectedArchiveType && _depth < settings.ARCHIVE_MAX_NESTING_DEPTH) {
            console.log("rar_nested_archive_detected_sync", { jobId, name: file.name, detected_type: detectedArchiveType, depth: _depth });
            try {
              // Extract nested archive recursively
              const nestedEntries = await extractArchiveToS3(
                jobId,
                entryUrl,
                detectedArchiveType,
                fieldSpec,
                batchId,
                password,
                _depth + 1
              );
              // Delete intermediate nested archive file
              await gcsClient().bucket(bucket).file(entryKey).delete();
              // Add nested entries to output
              out.push(...nestedEntries);
            } catch (err) {
              console.error("nested_extraction_failed", { jobId, name: file.name, error: err instanceof Error ? err.message : String(err) });
              // Fall back to treating the file as a normal non-nested entry
              out.push(makeEntryEvent(jobId, batchId, entryUrl, file.name, file.size, fieldSpec));
            }
          } else {
            out.push(makeEntryEvent(jobId, batchId, entryUrl, file.name, file.size, fieldSpec));
          }
          
          console.log("rar_extracted_file", { jobId, name: file.name, size: file.size });
        } catch (exc) {
          console.error("rar_extract_file_failed", { jobId, name: file.name, error: exc instanceof Error ? exc.message : String(exc) });
          // Continue processing remaining files instead of aborting the entire batch
        }
      }
      
      console.log("rar_extraction_complete", { jobId, totalFiles: files.length, totalUncompressed });

    } finally {
      // Cleanup temp file
      await fs.unlink(tmpPath).catch(() => {});
    }

    return out;
  }

  const raw = await readFull(bucket, key);
  const compressedSize = raw.length;

  if (archiveType === "zip") return extractZip(jobId, raw, compressedSize, fieldSpec, batchId, password);
  if (archiveType === "gz") return extractGz(jobId, raw, compressedSize, fieldSpec, batchId);
  if (archiveType === "tar") return extractTarArchive(jobId, raw, compressedSize, fieldSpec, batchId);
  if (archiveType === "7z") return extract7z(jobId, raw, compressedSize, fieldSpec, batchId, password);
  throw new Error(`Unsupported archive type: ${archiveType}`);
}

/**
 * Checks ratio
 * @param compressed - The compressed
 * @param uncompressed - The uncompressed
 */
function checkRatio(compressed: number, uncompressed: number): void {
  if (compressed > 0 && uncompressed / compressed > settings.ARCHIVE_MAX_COMPRESSION_RATIO) {
    throw new BombError(`Compression ratio ${(uncompressed / compressed).toFixed(0)}:1 exceeds cap ${settings.ARCHIVE_MAX_COMPRESSION_RATIO}:1`);
  }
  if (uncompressed > settings.ARCHIVE_MAX_UNCOMPRESSED_BYTES) {
    throw new BombError(`Uncompressed size ${uncompressed} exceeds cap ${settings.ARCHIVE_MAX_UNCOMPRESSED_BYTES}`);
  }
}

/**
 * Stores entry
 * @param jobId - The job identifier
 * @param entryName - The entry name
 * @param data - The data to process
 * @returns A promise that resolves to the result
 */
async function storeEntry(jobId: string, entryName: string, data: Buffer): Promise<[string, number]> {
  const safeName = path.basename(entryName).replace(/[#\s]+/g, "_") || "entry";
  const entryId = randomUUID();
  const s3Key = `ingested/${jobId}/entries/${entryId}/${safeName}`;
  await putObject(settings.DATA_BUCKET, s3Key, data);
  return [`gs://${settings.DATA_BUCKET}/${s3Key}`, data.length];
}

/**
 * Performs the make entry event operation.
 * @param parentJobId - The parent job id
 * @param batchId - The batch identifier
 * @param s3Url - The s3 url
 * @param name - The name value
 * @param size - The size value
 * @param fieldSpec - The field spec
 */
function makeEntryEvent(parentJobId: string, batchId: string, s3Url: string, name: string, size: number, fieldSpec: string[]) {
  return { parent_job_id: parentJobId, batchId: batchId, entry_s3_url: s3Url, entry_name: name, entry_size: size, field_spec: fieldSpec };
}

/**
 * Performs the with temp file operation.
 * @param data - The data to process
 * @param ext - The ext
 * @returns A promise that resolves to the result
 */
async function withTempFile(data: Buffer, ext: string): Promise<string> {
  const tmp = path.join(os.tmpdir(), `${randomUUID()}${ext}`);
  await fs.writeFile(tmp, data);
  return tmp;
}

/**
 * Extracts zip
 * @param jobId - The job identifier
 * @param raw - The raw
 * @param compressedSize - The compressed size
 * @param fieldSpec - The field spec
 * @param batchId - The batch identifier
 * @param password - The password
 * @returns A promise that resolves to the list
 */
async function extractZip(
  jobId: string,
  raw: Buffer,
  compressedSize: number,
  fieldSpec: string[],
  batchId: string,
  password?: string
): Promise<Record<string, unknown>[]> {
  const tmp = await withTempFile(raw, ".zip");
  const zip = new NodeStreamZip.async({ file: tmp, password: password || undefined });
  const entries = await zip.entries();
  if (Object.keys(entries).length > settings.ARCHIVE_MAX_ENTRIES) {
    throw new BombError(`ZIP has ${Object.keys(entries).length} entries > cap ${settings.ARCHIVE_MAX_ENTRIES}`);
  }
  const out: Record<string, unknown>[] = [];
  let totalUncompressed = 0;
  for (const [name, entry] of Object.entries(entries)) {
    if (entry.isDirectory) continue;
    const data = await zip.entryData(name);
    totalUncompressed += data.length;
    checkRatio(compressedSize, totalUncompressed);
    const [url, size] = await storeEntry(jobId, name, Buffer.from(data));
    out.push(makeEntryEvent(jobId, batchId, url, name, size, fieldSpec));
  }
  await zip.close();
  await fs.unlink(tmp).catch(() => {});
  return out;
}

/**
 * Extracts gz
 * @param jobId - The job identifier
 * @param raw - The raw
 * @param compressedSize - The compressed size
 * @param fieldSpec - The field spec
 * @param batchId - The batch identifier
 * @returns A promise that resolves to the list
 */
async function extractGz(
  jobId: string,
  raw: Buffer,
  compressedSize: number,
  fieldSpec: string[],
  batchId: string
): Promise<Record<string, unknown>[]> {
  const data = await gunzip(raw);
  checkRatio(compressedSize, data.length);
  const name = `decompressed_${jobId}.dat`;
  const [url, size] = await storeEntry(jobId, name, data);
  return [makeEntryEvent(jobId, batchId, url, name, size, fieldSpec)];
}

/**
 * Extracts tar archive
 * @param jobId - The job identifier
 * @param raw - The raw
 * @param compressedSize - The compressed size
 * @param fieldSpec - The field spec
 * @param batchId - The batch identifier
 * @returns A promise that resolves to the list
 */
async function extractTarArchive(
  jobId: string,
  raw: Buffer,
  compressedSize: number,
  fieldSpec: string[],
  batchId: string
): Promise<Record<string, unknown>[]> {
  const tmp = await withTempFile(raw, ".tar");
  const extractDir = path.join(os.tmpdir(), randomUUID());
  await fs.mkdir(extractDir, { recursive: true });
  await extractTar({ file: tmp, cwd: extractDir });
  const out: Record<string, unknown>[] = [];
  let totalUncompressed = 0;
  const files = await fs.readdir(extractDir, { recursive: true });
  for (const rel of files) {
    const fpath = path.join(extractDir, rel);
    const stat = await fs.stat(fpath);
    if (stat.isFile()) {
      const data = await fs.readFile(fpath);
      totalUncompressed += data.length;
      checkRatio(compressedSize, totalUncompressed);
      const [url, size] = await storeEntry(jobId, rel, data);
      out.push(makeEntryEvent(jobId, batchId, url, rel, size, fieldSpec));
    }
  }
  await fs.rm(extractDir, { recursive: true, force: true });
  await fs.unlink(tmp).catch(() => {});
  return out;
}

/**
 * Extracts 7z
 * @param jobId - The job identifier
 * @param raw - The raw
 * @param compressedSize - The compressed size
 * @param fieldSpec - The field spec
 * @param batchId - The batch identifier
 * @param password - The password
 * @returns A promise that resolves to the list
 */
async function extract7z(
  jobId: string,
  raw: Buffer,
  compressedSize: number,
  fieldSpec: string[],
  batchId: string,
  password?: string
): Promise<Record<string, unknown>[]> {
  const tmp = await withTempFile(raw, ".7z");
  const extractDir = path.join(os.tmpdir(), randomUUID());
  await fs.mkdir(extractDir, { recursive: true });
  const stream = Seven.extractFull(tmp, extractDir, { password: password || undefined });
  await once(stream, "end");
  const out: Record<string, unknown>[] = [];

  let totalUncompressed = 0;
  const files = await fs.readdir(extractDir, { recursive: true });
  for (const rel of files) {
    const fpath = path.join(extractDir, rel);
    const stat = await fs.stat(fpath);
    if (stat.isFile()) {
      const data = await fs.readFile(fpath);
      totalUncompressed += data.length;
      checkRatio(compressedSize, totalUncompressed);
      const [url, size] = await storeEntry(jobId, rel, data);
      out.push(makeEntryEvent(jobId, batchId, url, rel, size, fieldSpec));
    }
  }
  await fs.rm(extractDir, { recursive: true, force: true });
  await fs.unlink(tmp).catch(() => {});
  return out;
}
