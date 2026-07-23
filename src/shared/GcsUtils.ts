import crypto from "crypto";
import { Storage } from "@google-cloud/storage";
import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
import { decode } from "@utils/normalizers/encoding.js";

/**
 * GcsUtils is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class GcsUtils extends ServiceManager {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: GcsUtils;
    /**
   * Logger instance
   * @private
   */
  private logger: Logger;
    /**
   * Storage
   * @private
   */
  private storage: Storage | null = null;
    /**
   * G C S_ R E T R I E S
   * @private
   */
  private readonly GCS_RETRIES = 3;
    /**
   * G C S_ T I M E O U T_ M S
   * @private
   */
  private readonly GCS_TIMEOUT_MS = 7200000;
    /**
   * F E T C H_ C H U N K_ S I Z E
   * @private
   */
  private readonly FETCH_CHUNK_SIZE = 1048576;
    /**
   * S M A L L_ F I L E_ S I N G L E_ G E T_ T H R E S H O L D
   * @private
   */
  private readonly SMALL_FILE_SINGLE_GET_THRESHOLD = 104857600;
    /**
   * M A X_ Q U O T E D_ N E W L I N E S
   * @private
   */
  private readonly MAX_QUOTED_NEWLINES = 100;
    /**
   * M A X_ L I N E_ B Y T E S
   * @private
   */
  private readonly MAX_LINE_BYTES = 10485760;

    /**
   * Constructs a new GcsUtils instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate GcsUtils directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("gcs-utils");
  }

    /**
   * Gets the single instance of the GcsUtils class.
   * @returns The single instance of the class
   */
  public static getInstance(): GcsUtils {
    if (!GcsUtils.instance) {
      GcsUtils.instance = new GcsUtils(Enforce);
    }
    return GcsUtils.instance;
  }

    /**
   * Gets storage
   * @returns The storage result
   */
  public getStorage(): Storage {
    if (!this.storage) {
      const config = this.getConfig();
      this.storage = new Storage({
        projectId: config.settings.GCP_PROJECT_ID,
        ...(config.settings.GOOGLE_APPLICATION_CREDENTIALS
          ? { keyFilename: config.settings.GOOGLE_APPLICATION_CREDENTIALS }
          : {}),
      });
    }
    return this.storage;
  }

    /**
   * Checks whether retryable
   * @param err - The error that occurred
   * @returns True if the condition is met, false otherwise
   */
  private isRetryable(err: unknown): boolean {
    if (!err) return false;
    const code = (err as { code?: string | number }).code;
    if (typeof code === "number") return code === 429 || code >= 500;
    if (typeof code === "string") {
      return ["ECONNRESET", "ETIMEDOUT", "ENOTFOUND", "ECONNREFUSED", "EPIPE"].includes(code);
    }
    return true;
  }

    /**
   * Performs the with retry operation.
   * @param fn - The fn
   * @param retries - The number of retries
   * @param delay - The delay
   * @returns A promise that resolves to the result
   */
  private async withRetry<T>(fn: () => Promise<T>, retries = this.GCS_RETRIES, delay = 200): Promise<T> {
    let lastErr: unknown;
    for (let i = 0; i <= retries; i++) {
      try {
        return await fn();
      } catch (err) {
        lastErr = err;
        if (i === retries || !this.isRetryable(err)) throw err;
        const wait = delay * 2 ** i;
        this.logger.warn("gcs_retry", { attempt: i + 1, wait, error: String(err) });
        await new Promise((r) => setTimeout(r, wait));
      }
    }
    throw lastErr;
  }

    /**
   * Performs the with timeout operation.
   * @param fn - The fn
   * @param ms - The ms
   * @returns A promise that resolves to the result
   */
  private async withTimeout<T>(fn: () => Promise<T>, ms: number): Promise<T> {
    return Promise.race([
      fn(),
      new Promise<T>((_, reject) => setTimeout(() => reject(new Error(`GCS timeout after ${ms}ms`)), ms)),
    ]);
  }

    /**
   * Parses gcs url
   * @param url - The URL to process
   * @returns The [string, string] result
   */
  public parseGcsUrl(url: string): [string, string] {
    const prefix = url.startsWith("gs://") ? "gs://" : url.startsWith("s3://") ? "s3://" : null;
    if (!prefix) throw new Error(`Expected gs:// URL, got: ${url}`);
    const rest = url.slice(prefix.length);
    const slash = rest.indexOf("/");
    if (slash === -1) return [rest, ""];
    return [rest.slice(0, slash), rest.slice(slash + 1)];
  }

    /**
   * Performs the object size operation.
   * @param bucket - The bucket
   * @param key - The key
   * @returns A promise that resolves to the result
   */
  public async objectSize(bucket: string, key: string): Promise<number> {
    return this.withRetry(
      () => this.withTimeout(async () => {
        const [meta] = await this.getStorage().bucket(bucket).file(key).getMetadata();
        return Number((meta as { size?: string | number }).size ?? 0);
      }, this.GCS_TIMEOUT_MS),
      this.GCS_RETRIES
    );
  }

    /**
   * Performs the object exists operation.
   * @param bucket - The bucket
   * @param key - The key
   * @returns A promise that resolves to true if the object exists
   */
  public async objectExists(bucket: string, key: string): Promise<boolean> {
    return this.withRetry(
      () => this.withTimeout(async () => {
        const [exists] = await this.getStorage().bucket(bucket).file(key).exists();
        return exists;
      }, this.GCS_TIMEOUT_MS),
      this.GCS_RETRIES
    );
  }

    /**
   * Reads range
   * @param bucket - The bucket
   * @param key - The key
   * @param start - The start
   * @param end - The end
   * @returns A promise that resolves to the result
   */
  public async readRange(bucket: string, key: string, start: number, end: number): Promise<Buffer> {
    return this.withRetry(
      () => this.withTimeout(async () => {
        const chunks: Buffer[] = [];
        const stream = this.getStorage().bucket(bucket).file(key).createReadStream({ start, end });
        for await (const chunk of stream) {
          chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        }
        return Buffer.concat(chunks);
      }, this.GCS_TIMEOUT_MS),
      this.GCS_RETRIES
    );
  }

    /**
   * Reads full
   * @param bucket - The bucket
   * @param key - The key
   * @returns A promise that resolves to the result
   */
  public async readFull(bucket: string, key: string): Promise<Buffer> {
    return this.withRetry(
      () => this.withTimeout(async () => {
        const [data] = await this.getStorage().bucket(bucket).file(key).download();
        return data;
      }, this.GCS_TIMEOUT_MS),
      this.GCS_RETRIES
    );
  }

    /**
   * Performs the put object operation.
   * @param bucket - The bucket
   * @param key - The key
   * @param body - The body
   * @param contentType - The content type
   */
  public async putObject(
    bucket: string,
    key: string,
    body: Buffer,
    contentType = "application/octet-stream"
  ): Promise<void> {
    await this.withRetry(
      () => this.withTimeout(async () => {
        await this.getStorage()
          .bucket(bucket)
          .file(key)
          .save(body, { contentType, resumable: body.length > 5 * 1024 * 1024 });
      }, this.GCS_TIMEOUT_MS),
      this.GCS_RETRIES
    );
  }

    /**
   * Performs the put json operation.
   * @param bucket - The bucket
   * @param key - The key
   * @param data - The data to process
   */
  public async putJson(bucket: string, key: string, data: Record<string, unknown>): Promise<void> {
    await this.putObject(bucket, key, Buffer.from(JSON.stringify(data, null, 2), "utf-8"), "application/json");
  }

    /**
   * Performs the put parquet operation.
   * @param bucket - The bucket
   * @param key - The key
   * @param body - The body
   */
  public async putParquet(bucket: string, key: string, body: Buffer): Promise<void> {
    await this.putObject(bucket, key, body, "application/octet-stream");
  }

    /**
   * Copies object
   * @param srcBucket - The src bucket
   * @param srcKey - The src key
   * @param dstBucket - The dst bucket
   * @param dstKey - The dst key
   */
  public async copyObject(
    srcBucket: string,
    srcKey: string,
    dstBucket: string,
    dstKey: string
  ): Promise<void> {
    const srcFile = this.getStorage().bucket(srcBucket).file(srcKey);
    const [exists] = await srcFile.exists();
    if (!exists) {
      throw new Error(`Source file not found: ${srcBucket}/${srcKey}`);
    }
    const [meta] = await srcFile.getMetadata();
    const size = Number((meta as { size?: string | number }).size ?? 0);
  
    if (size > 100 * 1024 * 1024) {
      this.logger.info(`Using streaming copy for large file: ${size} bytes`);
      await this.streamCopy(srcBucket, srcKey, dstBucket, dstKey);
    } else {
      await this.withRetry(
        () => this.withTimeout(async () => {
          await this.getStorage()
            .bucket(srcBucket)
            .file(srcKey)
            .copy(this.getStorage().bucket(dstBucket).file(dstKey));
        }, this.GCS_TIMEOUT_MS),
        this.GCS_RETRIES
      );
    }
  }

    /**
   * Performs the stream copy operation.
   * @param srcBucket - The src bucket
   * @param srcKey - The src key
   * @param dstBucket - The dst bucket
   * @param dstKey - The dst key
   */
  private async streamCopy(
    srcBucket: string,
    srcKey: string,
    dstBucket: string,
    dstKey: string
  ): Promise<void> {
    const srcFile = this.getStorage().bucket(srcBucket).file(srcKey);
    const dstFile = this.getStorage().bucket(dstBucket).file(dstKey);
  
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
    
      readStream.on("data", (chunk) => {
        bytesCopied += chunk.length;
        const elapsed = (Date.now() - startTime) / 1000;
        const speed = bytesCopied / elapsed / (1024 * 1024);
        if (bytesCopied % (100 * 1024 * 1024) === 0) {
          this.logger.debug(`stream_copy_progress: ${bytesCopied / (1024 * 1024)}MB at ${speed.toFixed(2)}MB/s`);
        }
      });
    
      readStream.pipe(writeStream)
        .on("error", (error) => {
          this.logger.error("stream_copy_error:", { error: error.message, stack: error.stack });
          reject(error);
        })
        .on("finish", () => {
          const elapsed = (Date.now() - startTime) / 1000;
          this.logger.info(`stream_copy_complete: ${bytesCopied / (1024 * 1024)}MB in ${elapsed.toFixed(2)}s`);
          resolve();
        });
    });
  }

    /**
   * Performs the list objects operation.
   * @param bucket - The bucket
   * @param prefix - The prefix
   * @returns A promise that resolves to the list
   */
  public async listObjects(bucket: string, prefix: string): Promise<[string, number][]> {
    return this.withRetry(
      () => this.withTimeout(async () => {
        const [files] = await this.getStorage().bucket(bucket).getFiles({ prefix });
        return files.map((f) => [`gs://${bucket}/${f.name}`, Number((f.metadata as { size?: string | number }).size ?? 0)]);
      }, this.GCS_TIMEOUT_MS),
      this.GCS_RETRIES
    );
  }

    /**
   * Performs the presigned put url operation.
   * @param bucket - The bucket
   * @param key - The key
   * @param expiresIn - The expires in
   * @param contentType - The content type
   * @returns A promise that resolves to the result
   */
  public async presignedPutUrl(bucket: string, key: string, expiresIn = 3600, contentType = "application/octet-stream"): Promise<string> {
    return this.withRetry(
      () => this.withTimeout(async () => {
        const [url] = await this.getStorage()
          .bucket(bucket)
          .file(key)
          .getSignedUrl({
            action: "write",
            expires: Date.now() + expiresIn * 1000,
            contentType,
          });
        return url;
      }, this.GCS_TIMEOUT_MS),
      this.GCS_RETRIES
    );
  }

    /**
   * Performs the stream lines operation.
   * @param bucket - The bucket
   * @param key - The key
   * @param chunkSize - The chunk size
   * @param encoding - The encoding
   * @returns The async generator<[string, number, number]> result
   */
  public async *streamLines(
    bucket: string,
    key: string,
    chunkSize = this.FETCH_CHUNK_SIZE,
    encoding = "utf-8"
  ): AsyncGenerator<[string, number, number]> {
    const config = this.getConfig();
    const total = await this.objectSize(bucket, key);
    this.logger.debug("streamLines_start", { bucket, key, total, threshold: config.settings.SMALL_FILE_SINGLE_GET_THRESHOLD });

    const state: { inQuote: boolean } = { inQuote: false };

    if (total <= config.settings.SMALL_FILE_SINGLE_GET_THRESHOLD) {
      this.logger.debug("streamLines_using_single_get", { total });
      const data = await this.readFull(bucket, key);
      this.logger.debug("streamLines_download_complete", { size: data.length });
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

    /**
   * Splits all lines
   * @param data - The data to process
   * @param encoding - The encoding
   * @returns The list of results
   */
  public splitAllLines(data: Buffer, encoding = "utf-8"): [string, number, number][] {
    return [...this.splitBytesToLines(data, 0, encoding, { inQuote: false })];
  }

    /**
   * Splits bytes to lines
   * @param data - The data to process
   * @param baseOffset - The base offset
   * @param encoding - The encoding
   * @param state - The state
   * @returns The generator<[string, number, number]> result
   */
  private *splitBytesToLines(
    data: Buffer,
    baseOffset: number,
    encoding: string,
    state: { inQuote: boolean }
  ): Generator<[string, number, number]> {
    const result = yield* this.scanLines(data, baseOffset, encoding, state);

    if (result.lineStart < data.length) {
      const raw = data.slice(result.lineStart);
      const text = decode(raw, encoding).replace(/\r\n$|\n$/, "");
      
      // Detect and split run-on KV records (multiple records joined by space instead of newline)
      // Pattern: Email: X - Name: Y - Followers: N - Created At: ... Email: A - Name: B...
      if (text.includes("Email:") && (text.match(/Email:/g) || []).length > 1) {
        const chunks = text.split(/\s+(?=Email:\s)/);
        let currentOffset = baseOffset + result.lineStart;
        for (const chunk of chunks) {
          if (chunk.trim()) {
            const chunkBytes = Buffer.byteLength(chunk, encoding as BufferEncoding);
            yield [chunk.trim(), currentOffset, chunkBytes];
            currentOffset += chunkBytes + 1; // +1 for the space separator
          }
        }
      } else if (text) {
        yield [text, baseOffset + result.lineStart, raw.length];
      }
    }
  }

    /**
   * Performs the scan lines operation.
   * @param data - The data to process
   * @param dataBase - The data base
   * @param encoding - The encoding
   * @param state - The state
   * @returns The generator<[string, number, number], { line start: number; ended at boundary: boolean }, void> result
   */
  private *scanLines(
    data: Buffer,
    dataBase: number,
    encoding: string,
    state: { inQuote: boolean }
  ): Generator<[string, number, number], { lineStart: number; endedAtBoundary: boolean }, void> {
    const config = this.getConfig();
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
          if (quotedNewlines > config.settings.MAX_QUOTED_NEWLINES || pos + 1 - lineStart >= config.settings.MAX_LINE_BYTES) {
            state.inQuote = false;
            yield makeLine(pos + 1);
          }
        }
      }

      pos++;

      if (pos - lineStart >= config.settings.MAX_LINE_BYTES) {
        state.inQuote = false;
        yield makeLine(pos);
      }
    }

    return { lineStart, endedAtBoundary };
  }

    /**
   * Performs the sha256 hex operation.
   * @param data - The data to process
   * @returns The string result
   */
  public sha256Hex(data: Buffer): string {
    return crypto.createHash("sha256").update(data).digest("hex");
  }
}


export default GcsUtils;

/**
 * The gcs utils
 */
const gcsUtils = GcsUtils.getInstance();

/**
 * Performs the gcs client operation.
 * @returns The storage result
 */
export function gcsClient(): Storage {
  return gcsUtils.getStorage();
}

/**
 * Parses gcs url
 * @param url - The URL to process
 * @returns The [string, string] result
 */
export function parseGcsUrl(url: string): [string, string] {
  return gcsUtils.parseGcsUrl(url);
}

/**
 * Performs the object size operation.
 * @param bucket - The bucket
 * @param key - The key
 * @returns A promise that resolves to the result
 */
export function objectSize(bucket: string, key: string): Promise<number> {
  return gcsUtils.objectSize(bucket, key);
}

/**
 * Performs the object exists operation.
 * @param bucket - The bucket
 * @param key - The key
 * @returns A promise that resolves to true if the object exists
 */
export function objectExists(bucket: string, key: string): Promise<boolean> {
  return gcsUtils.objectExists(bucket, key);
}

/**
 * Reads range
 * @param bucket - The bucket
 * @param key - The key
 * @param start - The start
 * @param end - The end
 * @returns A promise that resolves to the result
 */
export function readRange(bucket: string, key: string, start: number, end: number): Promise<Buffer> {
  return gcsUtils.readRange(bucket, key, start, end);
}

/**
 * Reads full
 * @param bucket - The bucket
 * @param key - The key
 * @returns A promise that resolves to the result
 */
export function readFull(bucket: string, key: string): Promise<Buffer> {
  return gcsUtils.readFull(bucket, key);
}

/**
 * Performs the put object operation.
 * @param bucket - The bucket
 * @param key - The key
 * @param body - The body
 * @param contentType - The content type
 */
export function putObject(
  bucket: string,
  key: string,
  body: Buffer,
  contentType = "application/octet-stream"
): Promise<void> {
  return gcsUtils.putObject(bucket, key, body, contentType);
}

/**
 * Performs the put json operation.
 * @param bucket - The bucket
 * @param key - The key
 * @param data - The data to process
 */
export function putJson(bucket: string, key: string, data: Record<string, unknown>): Promise<void> {
  return gcsUtils.putJson(bucket, key, data);
}

/**
 * Performs the put parquet operation.
 * @param bucket - The bucket
 * @param key - The key
 * @param body - The body
 */
export function putParquet(bucket: string, key: string, body: Buffer): Promise<void> {
  return gcsUtils.putParquet(bucket, key, body);
}

/**
 * Copies object
 * @param srcBucket - The src bucket
 * @param srcKey - The src key
 * @param dstBucket - The dst bucket
 * @param dstKey - The dst key
 */
export function copyObject(
  srcBucket: string,
  srcKey: string,
  dstBucket: string,
  dstKey: string
): Promise<void> {
  return gcsUtils.copyObject(srcBucket, srcKey, dstBucket, dstKey);
}

/**
 * Performs the list objects operation.
 * @param bucket - The bucket
 * @param prefix - The prefix
 * @returns A promise that resolves to the list
 */
export function listObjects(bucket: string, prefix: string): Promise<[string, number][]> {
  return gcsUtils.listObjects(bucket, prefix);
}

/**
 * Performs the presigned put url operation.
 * @param bucket - The bucket
 * @param key - The key
 * @param expiresIn - The expires in
 * @param contentType - The content type
 * @returns A promise that resolves to the result
 */
export function presignedPutUrl(bucket: string, key: string, expiresIn = 3600, contentType = "application/octet-stream"): Promise<string> {
  return gcsUtils.presignedPutUrl(bucket, key, expiresIn, contentType);
}

/**
 * Performs the stream lines operation.
 * @param bucket - The bucket
 * @param key - The key
 * @param chunkSize - The chunk size
 * @param encoding - The encoding
 * @returns The async generator<[string, number, number]> result
 */
export async function* streamLines(
  bucket: string,
  key: string,
  chunkSize = 1048576,
  encoding = "utf-8"
): AsyncGenerator<[string, number, number]> {
  yield* gcsUtils.streamLines(bucket, key, chunkSize, encoding);
}

/**
 * Splits all lines
 * @param data - The data to process
 * @param encoding - The encoding
 * @returns The list of results
 */
export function splitAllLines(data: Buffer, encoding = "utf-8"): [string, number, number][] {
  return gcsUtils.splitAllLines(data, encoding);
}

/**
 * Performs the sha256 hex operation.
 * @param data - The data to process
 * @returns The string result
 */
export function sha256Hex(data: Buffer): string {
  return gcsUtils.sha256Hex(data);
}

export { parseGcsUrl as parseS3Url };
