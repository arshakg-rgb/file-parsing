import crypto from "crypto";
import { Storage } from "@google-cloud/storage";
import Config from "../../config/system-config/Config.js";
import { decode } from "../normalizers/Normalizer.js";
import { InstantiationError } from "../../errors/InstantiationError.js";

function Enforce(): void {}

const GCS_RETRIES = 3;
const GCS_TIMEOUT_MS = 7200000; // Increased to 7200s (2 hours) for very large files

class FirestoreCacheUtils {
  private static instance: FirestoreCacheUtils;
  private storage: Storage;
  private config: Config;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate FirestoreCacheUtils directly. Use getInstance()");
    }
    this.config = Config.getInstance();
    this.storage = new Storage({
      projectId: this.config.settings.GCP_PROJECT_ID,
      ...(this.config.settings.GOOGLE_APPLICATION_CREDENTIALS
        ? { keyFilename: this.config.settings.GOOGLE_APPLICATION_CREDENTIALS }
        : {}),
    });
  }

  public static getInstance(): FirestoreCacheUtils {
    if (!FirestoreCacheUtils.instance) {
      FirestoreCacheUtils.instance = new FirestoreCacheUtils(Enforce);
    }
    return FirestoreCacheUtils.instance;
  }

  public getStorage(): Storage {
    return this.storage;
  }

  public getConfig(): Config {
    return this.config;
  }

  private isRetryable(err: any): boolean {
    if (!err) return false;
    const code = err.code;
    if (typeof code === "number") return code === 429 || code >= 500;
    if (typeof code === "string") {
      return ["ECONNRESET", "ETIMEDOUT", "ENOTFOUND", "ECONNREFUSED", "EPIPE"].includes(code);
    }
    return true;
  }

  private async withRetry<T>(fn: () => Promise<T>, retries = GCS_RETRIES, delay = 200): Promise<T> {
    let lastErr: any;
    for (let i = 0; i <= retries; i++) {
      try {
        return await fn();
      } catch (err) {
        lastErr = err;
        if (i === retries || !this.isRetryable(err)) throw err;
        const wait = delay * 2 ** i;
        console.warn("gcs_retry", { attempt: i + 1, wait, error: String(err) });
        await new Promise((r) => setTimeout(r, wait));
      }
    }
    throw lastErr;
  }

  private async withTimeout<T>(fn: () => Promise<T>, ms: number): Promise<T> {
    return Promise.race([
      fn(),
      new Promise<T>((_, reject) => setTimeout(() => reject(new Error(`GCS timeout after ${ms}ms`)), ms)),
    ]);
  }

  /** Parse gs:// or legacy s3:// URLs into [bucket, key] */
  public parseGcsUrl(url: string): [string, string] {
    const prefix = url.startsWith("gs://") ? "gs://" : url.startsWith("s3://") ? "s3://" : null;
    if (!prefix) throw new Error(`Expected gs:// URL, got: ${url}`);
    const rest = url.slice(prefix.length);
    const slash = rest.indexOf("/");
    if (slash === -1) return [rest, ""];
    return [rest.slice(0, slash), rest.slice(slash + 1)];
  }

  public async objectSize(bucket: string, key: string): Promise<number> {
    return this.withRetry(
      () => this.withTimeout(async () => {
        const [meta] = await this.storage.bucket(bucket).file(key).getMetadata();
        return Number((meta as any).size ?? 0);
      }, GCS_TIMEOUT_MS),
      GCS_RETRIES
    );
  }

  public async readRange(bucket: string, key: string, start: number, end: number): Promise<Buffer> {
    return this.withRetry(
      () => this.withTimeout(async () => {
        const chunks: Buffer[] = [];
        const stream = this.storage.bucket(bucket).file(key).createReadStream({ start, end });
        for await (const chunk of stream) {
          chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        }
        return Buffer.concat(chunks);
      }, GCS_TIMEOUT_MS),
      GCS_RETRIES
    );
  }

  public async readFull(bucket: string, key: string): Promise<Buffer> {
    return this.withRetry(
      () => this.withTimeout(async () => {
        const [data] = await this.storage.bucket(bucket).file(key).download();
        return data;
      }, GCS_TIMEOUT_MS),
      GCS_RETRIES
    );
  }

  public async putObject(
    bucket: string,
    key: string,
    body: Buffer,
    contentType = "application/octet-stream"
  ): Promise<void> {
    await this.withRetry(
      () => this.withTimeout(async () => {
        await this.storage
          .bucket(bucket)
          .file(key)
          .save(body, { contentType, resumable: body.length > 5 * 1024 * 1024 });
      }, GCS_TIMEOUT_MS),
      GCS_RETRIES
    );
  }

  public async putJson(bucket: string, key: string, data: Record<string, unknown>): Promise<void> {
    await this.putObject(bucket, key, Buffer.from(JSON.stringify(data, null, 2), "utf-8"), "application/json");
  }

  public async putParquet(bucket: string, key: string, body: Buffer): Promise<void> {
    await this.putObject(bucket, key, body, "application/octet-stream");
  }

  public async copyObject(
    srcBucket: string,
    srcKey: string,
    dstBucket: string,
    dstKey: string
  ): Promise<void> {
    const srcFile = this.storage.bucket(srcBucket).file(srcKey);
    const [exists] = await srcFile.exists();
    if (!exists) {
      throw new Error(`Source file not found: ${srcBucket}/${srcKey}`);
    }
    const [meta] = await srcFile.getMetadata();
    const size = Number((meta as any).size ?? 0);
  
    if (size > 100 * 1024 * 1024) {
      console.log(`Using streaming copy for large file: ${size} bytes`);
      await this.streamCopy(srcBucket, srcKey, dstBucket, dstKey);
    } else {
      await this.withRetry(
        () => this.withTimeout(async () => {
          await this.storage
            .bucket(srcBucket)
            .file(srcKey)
            .copy(this.storage.bucket(dstBucket).file(dstKey));
        }, GCS_TIMEOUT_MS),
        GCS_RETRIES
      );
    }
  }

  private async streamCopy(
    srcBucket: string,
    srcKey: string,
    dstBucket: string,
    dstKey: string
  ): Promise<void> {
    const srcFile = this.storage.bucket(srcBucket).file(srcKey);
    const dstFile = this.storage.bucket(dstBucket).file(dstKey);
  
    const [exists] = await dstFile.exists();
    if (exists) {
      await dstFile.delete();
    }
  
    const writeStream = dstFile.createWriteStream({
      resumable: false,
    });
  
    const readStream = srcFile.createReadStream();
  
    return new Promise((resolve, reject) => {
      let bytesCopied = 0;
      const startTime = Date.now();
    
      readStream.on('data', (chunk) => {
        bytesCopied += chunk.length;
        const elapsed = (Date.now() - startTime) / 1000;
        const speed = bytesCopied / elapsed / (1024 * 1024);
        if (bytesCopied % (100 * 1024 * 1024) === 0) {
          console.log(`stream_copy_progress: ${bytesCopied / (1024 * 1024)}MB at ${speed.toFixed(2)}MB/s`);
        }
      });
    
      readStream.pipe(writeStream)
        .on('error', (error) => {
          console.error('stream_copy_error:', error);
          reject(error);
        })
        .on('finish', () => {
          const elapsed = (Date.now() - startTime) / 1000;
          console.log(`stream_copy_complete: ${bytesCopied / (1024 * 1024)}MB in ${elapsed.toFixed(2)}s`);
          resolve();
        });
    });
  }

  public async listObjects(bucket: string, prefix: string): Promise<[string, number][]> {
    return this.withRetry(
      () => this.withTimeout(async () => {
        const [files] = await this.storage.bucket(bucket).getFiles({ prefix });
        return files.map((f) => [`gs://${bucket}/${f.name}`, Number((f.metadata as any).size ?? 0)]);
      }, GCS_TIMEOUT_MS),
      GCS_RETRIES
    );
  }

  public async presignedPutUrl(bucket: string, key: string, expiresIn = 3600, contentType = "application/octet-stream"): Promise<string> {
    return this.withRetry(
      () => this.withTimeout(async () => {
        const [url] = await this.storage
          .bucket(bucket)
          .file(key)
          .getSignedUrl({
            action: "write",
            expires: Date.now() + expiresIn * 1000,
            contentType,
          });
        return url;
      }, GCS_TIMEOUT_MS),
      GCS_RETRIES
    );
  }

  public async* streamLines(
    bucket: string,
    key: string,
    chunkSize = this.config.settings.FETCH_CHUNK_SIZE,
    encoding = "utf-8"
  ): AsyncGenerator<[string, number, number]> {
    const total = await this.objectSize(bucket, key);
    console.log("streamLines_start", { bucket, key, total, threshold: this.config.settings.SMALL_FILE_SINGLE_GET_THRESHOLD });

    const state: LineState = { inQuote: false };

    if (total <= this.config.settings.SMALL_FILE_SINGLE_GET_THRESHOLD) {
      console.log("streamLines_using_single_get", { total });
      const data = await this.readFull(bucket, key);
      console.log("streamLines_download_complete", { size: data.length });
      yield* this.splitBytesToLines(data, 0, encoding, state);
      return;
    }

    let fetchOffset = 0;
    let remainder = Buffer.alloc(0);
    let remainderStart = 0;

    while (fetchOffset < total) {
      const end = Math.min(fetchOffset + chunkSize - 1, total - 1);
      const chunk = await this.readRange(bucket, key, fetchOffset, end);
      const data = Buffer.concat([remainder, chunk]);
      const dataBase = remainderStart;

      const result = yield* this.scanLines(data, dataBase, encoding, state);
      remainder = data.slice(result.lineStart);
      remainderStart = dataBase + result.lineStart;
      fetchOffset += chunk.length;
    }

    if (remainder.length > 0) {
      const raw = remainder;
      const lineText = decode(raw, encoding).replace(/\r\n$|\n$/, "");
      if (lineText) yield [lineText, remainderStart, raw.length];
    }
  }

  public splitAllLines(data: Buffer, encoding = "utf-8"): [string, number, number][] {
    return [...this.splitBytesToLines(data, 0, encoding, { inQuote: false })];
  }

  private* splitBytesToLines(
    data: Buffer,
    baseOffset: number,
    encoding: string,
    state: LineState
  ): Generator<[string, number, number]> {
    const result = yield* this.scanLines(data, baseOffset, encoding, state);

    if (result.lineStart < data.length) {
      const raw = data.slice(result.lineStart);
      const text = decode(raw, encoding).replace(/\r\n$|\n$/, "");
      if (text) yield [text, baseOffset + result.lineStart, raw.length];
    }
  }

  private* scanLines(
    data: Buffer,
    dataBase: number,
    encoding: string,
    state: LineState
  ): Generator<[string, number, number], { lineStart: number; endedAtBoundary: boolean }, void> {
    const NL = 0x0a;
    const CR = 0x0d;
    const QUOTE = 0x22;
    
    let pos = 0;
    let lineStart = 0;
    let endedAtBoundary = false;
    let quotedNewlines = 0;

    const makeLine = (endExclusive: number): [string, number, number] => {
      const raw = data.slice(lineStart, endExclusive);
      const tuple: [string, number, number] = [decode(raw, encoding).replace(/\r\n$|\n$/, ""), dataBase + lineStart, raw.length];
      lineStart = endExclusive;
      quotedNewlines = 0;
      return tuple;
    };

    while (pos < data.length) {
      const b = data[pos];
      if (b === QUOTE) {
        if (state.inQuote) {
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

      if (b === NL) {
        if (!state.inQuote) {
          yield makeLine(pos + 1);
        } else {
          quotedNewlines++;
          if (quotedNewlines > this.config.settings.MAX_QUOTED_NEWLINES || pos + 1 - lineStart >= this.config.settings.MAX_LINE_BYTES) {
            state.inQuote = false;
            yield makeLine(pos + 1);
          }
        }
      }

      pos++;

      if (pos - lineStart >= this.config.settings.MAX_LINE_BYTES) {
        state.inQuote = false;
        yield makeLine(pos);
      }
    }

    return { lineStart, endedAtBoundary };
  }

  public sha256Hex(data: Buffer): string {
    return crypto.createHash("sha256").update(data).digest("hex");
  }
}

interface LineState {
  inQuote: boolean;
}


export default FirestoreCacheUtils;

// Backward compatibility wrappers
const cacheUtils = FirestoreCacheUtils.getInstance();

export function gcsClient() {
  return cacheUtils.getStorage();
}

export function parseGcsUrl(url: string): [string, string] {
  return cacheUtils.parseGcsUrl(url);
}

export async function objectSize(bucket: string, key: string): Promise<number> {
  return cacheUtils.objectSize(bucket, key);
}

export async function readRange(bucket: string, key: string, start: number, end: number): Promise<Buffer> {
  return cacheUtils.readRange(bucket, key, start, end);
}

export async function readFull(bucket: string, key: string): Promise<Buffer> {
  return cacheUtils.readFull(bucket, key);
}

export async function putObject(
  bucket: string,
  key: string,
  body: Buffer,
  contentType = "application/octet-stream"
): Promise<void> {
  return cacheUtils.putObject(bucket, key, body, contentType);
}

export async function putJson(bucket: string, key: string, data: Record<string, unknown>): Promise<void> {
  return cacheUtils.putJson(bucket, key, data);
}

export async function putParquet(bucket: string, key: string, body: Buffer): Promise<void> {
  return cacheUtils.putParquet(bucket, key, body);
}

export async function copyObject(
  srcBucket: string,
  srcKey: string,
  dstBucket: string,
  dstKey: string
): Promise<void> {
  return cacheUtils.copyObject(srcBucket, srcKey, dstBucket, dstKey);
}

export async function listObjects(bucket: string, prefix: string): Promise<[string, number][]> {
  return cacheUtils.listObjects(bucket, prefix);
}

export async function presignedPutUrl(bucket: string, key: string, expiresIn = 3600, contentType = "application/octet-stream"): Promise<string> {
  return cacheUtils.presignedPutUrl(bucket, key, expiresIn, contentType);
}

export async function* streamLines(
  bucket: string,
  key: string,
  chunkSize?: number,
  encoding = "utf-8"
): AsyncGenerator<[string, number, number]> {
  yield* cacheUtils.streamLines(bucket, key, chunkSize, encoding);
}

export function splitAllLines(data: Buffer, encoding = "utf-8"): [string, number, number][] {
  return cacheUtils.splitAllLines(data, encoding);
}

export function sha256Hex(data: Buffer): string {
  return cacheUtils.sha256Hex(data);
}

// Backward-compat re-exports
export { parseGcsUrl as parseS3Url };
