import { randomUUID } from "crypto";
import fs from "fs/promises";
import os from "os";
import path from "path";
import zlib from "zlib";
import { promisify } from "util";
import NodeStreamZip from "node-stream-zip";
import { extract as extractTar } from "tar";
import Seven from "node-7z";
import { once } from "node:events";
import { RARExtractor } from "unrar-async";
import { settings } from "../../shared/config.js";
import { parseGcsUrl as parseS3Url, objectSize, readFull, putObject, listObjects, copyObject } from "../../shared/gcsUtils.js";
import { fetchUrlStream } from "./ssrf_guard.js";

const gunzip = promisify(zlib.gunzip);

export class BombError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BombError";
  }
}

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

export async function listS3Prefix(prefixUrl: string): Promise<[string, number][]> {
  const [bucket, prefix] = parseS3Url(prefixUrl);
  return listObjects(bucket, prefix);
}

const MAGIC_ZIP = Buffer.from("PK\x03\x04");
const MAGIC_GZ = Buffer.from("\x1f\x8b");
const MAGIC_7Z = Buffer.from("7z\xbc\xaf\x27\x1c");
const MAGIC_RAR = Buffer.from("Rar!");

export function detectArchiveType(header: Buffer): string | null {
  if (header.slice(0, 4).equals(MAGIC_ZIP)) return "zip";
  if (header.slice(0, 2).equals(MAGIC_GZ)) return "gz";
  if (header.slice(0, 6).equals(MAGIC_7Z)) return "7z";
  if (header.slice(0, 4).equals(MAGIC_RAR)) return "rar";
  if (header.length > 262 && header.slice(257, 262).toString() === "ustar") return "tar";
  return null;
}

export async function extractArchiveToS3(
  jobId: string,
  s3Url: string,
  archiveType: string,
  fieldSpec: string[],
  batchId: string,
  password?: string,
  _depth = 0
): Promise<Record<string, any>[]> {
  if (_depth > settings.ARCHIVE_MAX_NESTING_DEPTH) {
    throw new BombError(`Archive nesting depth ${_depth} exceeds maximum ${settings.ARCHIVE_MAX_NESTING_DEPTH}`);
  }
  const [bucket, key] = parseS3Url(s3Url);
  const raw = await readFull(bucket, key);
  const compressedSize = raw.length;

  if (archiveType === "zip") return extractZip(jobId, raw, compressedSize, fieldSpec, batchId, password);
  if (archiveType === "gz") return extractGz(jobId, raw, compressedSize, fieldSpec, batchId);
  if (archiveType === "tar") return extractTarArchive(jobId, raw, compressedSize, fieldSpec, batchId);
  if (archiveType === "7z") return extract7z(jobId, raw, compressedSize, fieldSpec, batchId, password);
  if (archiveType === "rar") return extractRar(jobId, raw, compressedSize, fieldSpec, batchId, password);
  throw new Error(`Unsupported archive type: ${archiveType}`);
}

function checkRatio(compressed: number, uncompressed: number): void {
  if (compressed > 0 && uncompressed / compressed > settings.ARCHIVE_MAX_COMPRESSION_RATIO) {
    throw new BombError(`Compression ratio ${(uncompressed / compressed).toFixed(0)}:1 exceeds cap ${settings.ARCHIVE_MAX_COMPRESSION_RATIO}:1`);
  }
  if (uncompressed > settings.ARCHIVE_MAX_UNCOMPRESSED_BYTES) {
    throw new BombError(`Uncompressed size ${uncompressed} exceeds cap ${settings.ARCHIVE_MAX_UNCOMPRESSED_BYTES}`);
  }
}

async function storeEntry(jobId: string, entryName: string, data: Buffer): Promise<[string, number]> {
  const safeName = path.basename(entryName).replace(/[#\s]+/g, "_") || "entry";
  const entryId = randomUUID();
  const s3Key = `ingested/${jobId}/entries/${entryId}/${safeName}`;
  await putObject(settings.DATA_BUCKET, s3Key, data);
  return [`gs://${settings.DATA_BUCKET}/${s3Key}`, data.length];
}

function makeEntryEvent(parentJobId: string, batchId: string, s3Url: string, name: string, size: number, fieldSpec: string[]) {
  return { parent_job_id: parentJobId, batch_id: batchId, entry_s3_url: s3Url, entry_name: name, entry_size: size, field_spec: fieldSpec };
}

async function withTempFile(data: Buffer, ext: string): Promise<string> {
  const tmp = path.join(os.tmpdir(), `${randomUUID()}${ext}`);
  await fs.writeFile(tmp, data);
  return tmp;
}

async function extractZip(
  jobId: string,
  raw: Buffer,
  compressedSize: number,
  fieldSpec: string[],
  batchId: string,
  password?: string
): Promise<Record<string, any>[]> {
  const tmp = await withTempFile(raw, ".zip");
  const zip = new NodeStreamZip.async({ file: tmp, password: password || undefined });
  const entries = await zip.entries();
  if (Object.keys(entries).length > settings.ARCHIVE_MAX_ENTRIES) {
    throw new BombError(`ZIP has ${Object.keys(entries).length} entries > cap ${settings.ARCHIVE_MAX_ENTRIES}`);
  }
  const out: Record<string, any>[] = [];
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

async function extractGz(
  jobId: string,
  raw: Buffer,
  compressedSize: number,
  fieldSpec: string[],
  batchId: string
): Promise<Record<string, any>[]> {
  const data = await gunzip(raw);
  checkRatio(compressedSize, data.length);
  const name = `decompressed_${jobId}.dat`;
  const [url, size] = await storeEntry(jobId, name, data);
  return [makeEntryEvent(jobId, batchId, url, name, size, fieldSpec)];
}

async function extractTarArchive(
  jobId: string,
  raw: Buffer,
  compressedSize: number,
  fieldSpec: string[],
  batchId: string
): Promise<Record<string, any>[]> {
  const tmp = await withTempFile(raw, ".tar");
  const extractDir = path.join(os.tmpdir(), randomUUID());
  await fs.mkdir(extractDir, { recursive: true });
  await extractTar({ file: tmp, cwd: extractDir });
  const out: Record<string, any>[] = [];
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

async function extract7z(
  jobId: string,
  raw: Buffer,
  compressedSize: number,
  fieldSpec: string[],
  batchId: string,
  password?: string
): Promise<Record<string, any>[]> {
  const tmp = await withTempFile(raw, ".7z");
  const extractDir = path.join(os.tmpdir(), randomUUID());
  await fs.mkdir(extractDir, { recursive: true });
  const stream = Seven.extractFull(tmp, extractDir, { password: password || undefined });
  await once(stream, "end");
  const out: Record<string, any>[] = [];

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

async function extractRar(
  jobId: string,
  raw: Buffer,
  compressedSize: number,
  fieldSpec: string[],
  batchId: string,
  password?: string
): Promise<Record<string, any>[]> {
  const tmp = await withTempFile(raw, ".rar");
  const extractor = await RARExtractor.fromFile(tmp, { password: password || undefined });
  const result = await extractor.extract();
  const out: Record<string, any>[] = [];
  let totalUncompressed = 0;
  const MAX_FILE_SIZE = 500 * 1024 * 1024; // 500MB per file limit to avoid memory issues
  for await (const { fileHeader, extraction } of result.files) {
    if (!extraction) continue;
    if (fileHeader.unpSize > MAX_FILE_SIZE) {
      console.log("rar_skip_large_file", { jobId, name: fileHeader.name, size: fileHeader.unpSize });
      continue;
    }
    const chunks: Buffer[] = [];
    for await (const chunk of extraction) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    const data = Buffer.concat(chunks);
    totalUncompressed += data.length;
    checkRatio(compressedSize, totalUncompressed);
    const [url, size] = await storeEntry(jobId, fileHeader.name, data);
    out.push(makeEntryEvent(jobId, batchId, url, fileHeader.name, size, fieldSpec));
  }
  extractor.close();
  await fs.unlink(tmp).catch(() => {}); // temp file cleanup
  return out;
}
