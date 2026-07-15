import crypto from "crypto";
import { Storage } from "@google-cloud/storage";
import { settings } from "./config.js";

let _storage: Storage | undefined;

export function gcsClient(): Storage {
  if (_storage) return _storage;
  _storage = new Storage({
    projectId: settings.GCP_PROJECT_ID,
    ...(settings.GOOGLE_APPLICATION_CREDENTIALS
      ? { keyFilename: settings.GOOGLE_APPLICATION_CREDENTIALS }
      : {}),
  });
  return _storage;
}

const GCS_RETRIES = 3;
const GCS_TIMEOUT_MS = 1200000; // Increased to 1200s (20 minutes) for very large files

function isRetryable(err: any): boolean {
  if (!err) return false;
  const code = err.code;
  if (typeof code === "number") return code === 429 || code >= 500;
  if (typeof code === "string") {
    return ["ECONNRESET", "ETIMEDOUT", "ENOTFOUND", "ECONNREFUSED", "EPIPE"].includes(code);
  }
  return true;
}

async function withRetry<T>(fn: () => Promise<T>, retries = GCS_RETRIES, delay = 200): Promise<T> {
  let lastErr: any;
  for (let i = 0; i <= retries; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (i === retries || !isRetryable(err)) throw err;
      const wait = delay * 2 ** i;
      console.warn("gcs_retry", { attempt: i + 1, wait, error: String(err) });
      await new Promise((r) => setTimeout(r, wait));
    }
  }
  throw lastErr;
}

async function withTimeout<T>(fn: () => Promise<T>, ms: number): Promise<T> {
  return Promise.race([
    fn(),
    new Promise<T>((_, reject) => setTimeout(() => reject(new Error(`GCS timeout after ${ms}ms`)), ms)),
  ]);
}

/** Parse gs:// or legacy s3:// URLs into [bucket, key] */
export function parseGcsUrl(url: string): [string, string] {
  const prefix = url.startsWith("gs://") ? "gs://" : url.startsWith("s3://") ? "s3://" : null;
  if (!prefix) throw new Error(`Expected gs:// URL, got: ${url}`);
  const rest = url.slice(prefix.length);
  const slash = rest.indexOf("/");
  if (slash === -1) return [rest, ""];
  return [rest.slice(0, slash), rest.slice(slash + 1)];
}

export async function objectSize(bucket: string, key: string): Promise<number> {
  return withRetry(
    () => withTimeout(async () => {
      const [meta] = await gcsClient().bucket(bucket).file(key).getMetadata();
      return Number((meta as any).size ?? 0);
    }, GCS_TIMEOUT_MS),
    GCS_RETRIES
  );
}

export async function readRange(bucket: string, key: string, start: number, end: number): Promise<Buffer> {
  return withRetry(
    () => withTimeout(async () => {
      const chunks: Buffer[] = [];
      const stream = gcsClient().bucket(bucket).file(key).createReadStream({ start, end });
      for await (const chunk of stream) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      }
      return Buffer.concat(chunks);
    }, GCS_TIMEOUT_MS),
    GCS_RETRIES
  );
}

export async function readFull(bucket: string, key: string): Promise<Buffer> {
  return withRetry(
    () => withTimeout(async () => {
      const [data] = await gcsClient().bucket(bucket).file(key).download();
      return data;
    }, GCS_TIMEOUT_MS),
    GCS_RETRIES
  );
}

export async function putObject(
  bucket: string,
  key: string,
  body: Buffer,
  contentType = "application/octet-stream"
): Promise<void> {
  await withRetry(
    () => withTimeout(async () => {
      await gcsClient()
        .bucket(bucket)
        .file(key)
        .save(body, { contentType, resumable: body.length > 5 * 1024 * 1024 });
    }, GCS_TIMEOUT_MS),
    GCS_RETRIES
  );
}

export async function putJson(bucket: string, key: string, data: Record<string, unknown>): Promise<void> {
  await putObject(bucket, key, Buffer.from(JSON.stringify(data, null, 2), "utf-8"), "application/json");
}

export async function putParquet(bucket: string, key: string, body: Buffer): Promise<void> {
  await putObject(bucket, key, body, "application/octet-stream");
}

export async function copyObject(
  srcBucket: string,
  srcKey: string,
  dstBucket: string,
  dstKey: string
): Promise<void> {
  await withRetry(
    () => withTimeout(async () => {
      await gcsClient()
        .bucket(srcBucket)
        .file(srcKey)
        .copy(gcsClient().bucket(dstBucket).file(dstKey));
    }, GCS_TIMEOUT_MS),
    GCS_RETRIES
  );
}

export async function listObjects(bucket: string, prefix: string): Promise<[string, number][]> {
  return withRetry(
    () => withTimeout(async () => {
      const [files] = await gcsClient().bucket(bucket).getFiles({ prefix });
      return files.map((f) => [`gs://${bucket}/${f.name}`, Number((f.metadata as any).size ?? 0)]);
    }, GCS_TIMEOUT_MS),
    GCS_RETRIES
  );
}

export async function presignedPutUrl(bucket: string, key: string, expiresIn = 3600): Promise<string> {
  return withRetry(
    () => withTimeout(async () => {
      const [url] = await gcsClient()
        .bucket(bucket)
        .file(key)
        .getSignedUrl({
          action: "write",
          expires: Date.now() + expiresIn * 1000,
        });
      return url;
    }, GCS_TIMEOUT_MS),
    GCS_RETRIES
  );
}

const NL = 0x0a;
const CR = 0x0d;
const QUOTE = 0x22;

interface LineState {
  inQuote: boolean;
}

export async function* streamLines(
  bucket: string,
  key: string,
  chunkSize = settings.FETCH_CHUNK_SIZE,
  encoding = "utf-8"
): AsyncGenerator<[string, number, number]> {
  const total = await objectSize(bucket, key);
  console.log("streamLines_start", { bucket, key, total, threshold: settings.SMALL_FILE_SINGLE_GET_THRESHOLD });

  const state: LineState = { inQuote: false };

  if (total <= settings.SMALL_FILE_SINGLE_GET_THRESHOLD) {
    console.log("streamLines_using_single_get", { total });
    const data = await readFull(bucket, key);
    console.log("streamLines_download_complete", { size: data.length });
    yield* splitBytesToLines(data, 0, encoding, state);
    return;
  }

  let fetchOffset = 0;
  let remainder = Buffer.alloc(0);
  let remainderStart = 0;

  while (fetchOffset < total) {
    const end = Math.min(fetchOffset + chunkSize - 1, total - 1);
    const chunk = await readRange(bucket, key, fetchOffset, end);
    const data = Buffer.concat([remainder, chunk]);
    const dataBase = remainderStart;

    const result = yield* scanLines(data, dataBase, encoding, state);
    remainder = data.slice(result.lineStart);
    remainderStart = dataBase + result.lineStart;
    fetchOffset += chunk.length;
  }

  if (remainder.length > 0) {
    const raw = remainder;
    const lineText = raw.toString(encoding as BufferEncoding).replace(/\r\n$|\n$/, "");
    if (lineText) yield [lineText, remainderStart, raw.length];
  }
}

function* splitBytesToLines(
  data: Buffer,
  baseOffset: number,
  encoding: string,
  state: LineState
): Generator<[string, number, number]> {
  const result = yield* scanLines(data, baseOffset, encoding, state);

  if (result.lineStart < data.length) {
    const raw = data.slice(result.lineStart);
    const text = raw.toString(encoding as BufferEncoding).replace(/\r\n$|\n$/, "");
    if (text) yield [text, baseOffset + result.lineStart, raw.length];
  }
}

function* scanLines(
  data: Buffer,
  dataBase: number,
  encoding: string,
  state: LineState
): Generator<[string, number, number], { lineStart: number; endedAtBoundary: boolean }, void> {
  let pos = 0;
  let lineStart = 0;
  let endedAtBoundary = false;

  while (pos < data.length) {
    const b = data[pos];
    if (b === QUOTE) {
      if (state.inQuote) {
        // Escaped quote: look for "". If the quote is at the chunk boundary we cannot decide yet.
        if (pos + 1 === data.length) {
          endedAtBoundary = true;
          break;
        }
        if (data[pos + 1] === QUOTE) {
          pos += 2;
          continue;
        }
      }
      state.inQuote = !state.inQuote;
      pos++;
      continue;
    }

    if (b === NL && !state.inQuote) {
      const raw = data.slice(lineStart, pos + 1);
      const lineText = raw.toString(encoding as BufferEncoding).replace(/\r\n$|\n$/, "");
      yield [lineText, dataBase + lineStart, raw.length];
      lineStart = pos + 1;
    }

    pos++;
  }

  return { lineStart, endedAtBoundary };
}

export function sha256Hex(data: Buffer): string {
  return crypto.createHash("sha256").update(data).digest("hex");
}

// ── Backward-compat re-exports so existing imports of s3Utils still compile ──
export { parseGcsUrl as parseS3Url };
