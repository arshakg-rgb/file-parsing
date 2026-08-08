import pino from "pino";
import fs from "node:fs";
import { pipeline } from "node:stream/promises";
import { Storage } from "@google-cloud/storage";
import ServiceManager from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { createLogger } from "@utils/logger/Log.js";
import EncodingService from "@utils/normalizers/Encoding";
import Config from "@config/system-config/Config";


/**
 * GcsUtils is a singleton class responsible for managing Google Cloud
 * Storage access — reads, writes, copies, signed URLs, and line-oriented
 * streaming — with built-in retry and timeout handling.
 *
 * There is exactly one Storage client for the whole process, so caching a
 * single instance behind getInstance() is correct and safe.
 */

export class GcsUtils extends ServiceManager
{
  /**
   * Singleton instance
   * @private
   */

  protected static instance: GcsUtils;

  /**
   * Logger instance
   * @private
   */

  private logger: pino.Logger;

  /**
   * Storage
   * @private
   */

  private storage: Storage | null = null;

  /**
   * G C S_ R E T R I E S
   * @private
   */

  private readonly GCS_RETRIES: number = 3;

  /**
   * G C S_ T I M E O U T_ M S
   * @private
   */

  private readonly GCS_TIMEOUT_MS: number = 7200000;

  /**
   * FETCH CHUNK SIZE
   * @private
   */

  private readonly FETCH_CHUNK_SIZE: number = 1048576;

  /**
   * Constructs a new GcsUtils instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */

  private constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Cannot instantiate GcsUtils directly. Use getInstance()");
    }

    super(enforce);

    this.logger = createLogger(module);
  }

  /**
   * Gets the single instance of the GcsUtils class.
   * @returns The single instance of the class
   */
  public static getInstance(): GcsUtils
  {
    if (!GcsUtils.instance)
    {

      GcsUtils.instance = new GcsUtils(Enforce);
    }

    return GcsUtils.instance;
  }

  /**
   * Gets storage
   * @returns The storage result
   */
  public getStorage(): Storage
  {
    if (!this.storage)
    {
      const config: Config = this.getConfig();
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
  private isRetryable(err: unknown): boolean
  {
    if (!err)
    {
      return false;
    }

    const code: string | number = (err as { code?: string | number }).code;

    if (typeof code === "number")
    {
      return code === 429 || code >= 500;
    }

    if (typeof code === "string")
    {
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
  private async withRetry<T>(fn: () => Promise<T>, retries = this.GCS_RETRIES, delay = 200): Promise<T>
  {
    let lastErr: unknown;

    for (let i = 0; i <= retries; i++)
    {
      try
      {
        return await fn();
      }
      catch (err)
      {
        lastErr = err;
        if (i === retries || !this.isRetryable(err))
        {
          throw err;
        }

        const wait: number = delay * 2 ** i;
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

  private async withTimeout<T>(fn: () => Promise<T>, ms: number): Promise<T>
  {
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
  public parseGcsUrl(url: string): [string, string]
  {
    const prefix = url.startsWith("gs://") ? "gs://" : url.startsWith("s3://") ? "s3://" : null;

    if (!prefix)
    {
      throw new Error(`Expected gs:// URL, got: ${url}`);
    }

    const rest: string = url.slice(prefix.length);
    const slash: number = rest.indexOf("/");

    if (slash === -1)
    {
      return [rest, ""];
    }

    return [rest.slice(0, slash), rest.slice(slash + 1)];
  }

  /**
   * Performs the object size operation.
   * @param bucket - The bucket
   * @param key - The key
   * @returns A promise that resolves to the result
   */

  public async objectSize(bucket: string, key: string): Promise<number>
  {
    return this.withRetry(
        () => this.withTimeout(async () => {
          const [meta] = await this.getStorage().bucket(bucket).file(key).getMetadata();
          return Number((meta as { size?: string | number }).size ?? 0);
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
  public async readRange(bucket: string, key: string, start: number, end: number): Promise<Buffer>
  {
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

  public async readFull(bucket: string, key: string): Promise<Buffer>
  {
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

  public async putObject(bucket: string, key: string, body: Buffer, contentType = "application/octet-stream"): Promise<void>
  {
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
   * Downloads an object to a local file using a stream.
   * @param bucket - The bucket
   * @param key - The key
   * @param destination - The local file path
   */
  public async downloadToFile(bucket: string, key: string, destination: string): Promise<void>
  {
    await this.withRetry(
        () => this.withTimeout(async () => {
          const readStream = this.getStorage().bucket(bucket).file(key).createReadStream();
          const writeStream = fs.createWriteStream(destination);
          await pipeline(readStream, writeStream);
        }, this.GCS_TIMEOUT_MS),
        this.GCS_RETRIES
    );
  }

  /**
   * Uploads a local file to GCS using a stream.
   * @param bucket - The bucket
   * @param key - The key
   * @param filePath - The local file path
   * @param contentType - The content type
   */
  public async putObjectFromFile(bucket: string, key: string, filePath: string, contentType = "application/octet-stream"): Promise<void>
  {
    await this.withRetry(
        () => this.withTimeout(async () => {
          const readStream = fs.createReadStream(filePath);
          const writeStream = this.getStorage().bucket(bucket).file(key).createWriteStream({ contentType, resumable: false });
          await pipeline(readStream, writeStream);
        }, this.GCS_TIMEOUT_MS),
        this.GCS_RETRIES
    );
  }

  /**
   * Deletes object
   * @param bucket - The bucket
   * @param key - The key
   */
  public async deleteObject(bucket: string, key: string): Promise<void>
  {
    await this.withRetry(
        () => this.withTimeout(async () => {
          await this.getStorage().bucket(bucket).file(key).delete();
        }, this.GCS_TIMEOUT_MS),
        this.GCS_RETRIES
    );
  }

  /**
   * Copies object
   * @param srcBucket - The src bucket
   * @param srcKey - The src key
   * @param dstBucket - The dst bucket
   * @param dstKey - The dst key
   */

  public async copyObject(srcBucket: string, srcKey: string, dstBucket: string, dstKey: string): Promise<void>
  {
    const srcFile = this.getStorage().bucket(srcBucket).file(srcKey);
    const [exists] = await srcFile.exists();

    if (!exists)
    {
      throw new Error(`Source file not found: ${srcBucket}/${srcKey}`);
    }

    const [meta] = await srcFile.getMetadata();
    const size: number = Number((meta as { size?: string | number }).size ?? 0);

    if (size > 100 * 1024 * 1024)
    {
      this.logger.info(`Using streaming copy for large file: ${size} bytes`);
      await this.streamCopy(srcBucket, srcKey, dstBucket, dstKey);
    }
    else
    {
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
  private async streamCopy(srcBucket: string, srcKey: string, dstBucket: string, dstKey: string): Promise<void>
  {
    const srcFile = this.getStorage().bucket(srcBucket).file(srcKey);
    const dstFile = this.getStorage().bucket(dstBucket).file(dstKey);

    const [exists] = await dstFile.exists();

    if (exists)
    {
      await dstFile.delete();
    }

    const writeStream = dstFile.createWriteStream({
      resumable: false,
    });

    const readStream = srcFile.createReadStream();

    return new Promise((resolve, reject) =>
    {
      let bytesCopied: number = 0;
      const startTime: number = Date.now();

      readStream.on("data", (chunk) => {
        bytesCopied += chunk.length;
        const elapsed = (Date.now() - startTime) / 1000;
        const speed = bytesCopied / elapsed / (1024 * 1024);

        if (bytesCopied % (100 * 1024 * 1024) === 0)
        {
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

  public async listObjects(bucket: string, prefix: string): Promise<[string, number][]>
  {
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
  public async presignedPutUrl(bucket: string, key: string, expiresIn = 3600, contentType = "application/octet-stream"): Promise<string>
  {
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
   * Performs the presigned get url operation.
   * @param bucket - The bucket
   * @param key - The key
   * @param expiresIn - The expires in
   * @param filename - The attachment filename for the downloaded file
   * @returns A promise that resolves to the result
   */
  public async presignedGetUrl(bucket: string, key: string, expiresIn = 3600, filename?: string): Promise<string>
  {
    return this.withRetry(
        () => this.withTimeout(async () => {
          const options: { action: "read"; expires: number; responseDisposition?: string } = {
            action: "read",
            expires: Date.now() + expiresIn * 1000,
          };
          if (filename) {
            options.responseDisposition = `attachment; filename="${filename}"`;
          }
          const [url] = await this.getStorage()
              .bucket(bucket)
              .file(key)
              .getSignedUrl(options);
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

  public async *streamLines(bucket: string, key: string, chunkSize = this.FETCH_CHUNK_SIZE, encoding = "utf-8"): AsyncGenerator<[string, number, number]>
  {
    const config: Config = this.getConfig();
    const total: number = await this.objectSize(bucket, key);
    this.logger.debug("streamLines_start", { bucket, key, total, threshold: config.settings.SMALL_FILE_SINGLE_GET_THRESHOLD });

    const state: { inQuote: boolean } = { inQuote: false };

    if (total <= config.settings.SMALL_FILE_SINGLE_GET_THRESHOLD)
    {
      this.logger.debug("streamLines_using_single_get", { total });
      const data = await this.readFull(bucket, key);
      this.logger.debug("streamLines_download_complete", { size: data.length });
      yield* this.splitBytesToLines(data, 0, encoding, state);

      return;
    }

    let remainder: Buffer<ArrayBuffer> = Buffer.alloc(0);
    let remainderStart: number = 0;

    type PendingRead = { offset: number; expectedLength: number; promise: Promise<Buffer> };

    const startRead = (offset: number): PendingRead | null =>
    {
      if (offset >= total)
      {
        return null;
      }

      const end: number = Math.min(offset + chunkSize - 1, total - 1);

      return { offset, expectedLength: end - offset + 1, promise: this.readRange(bucket, key, offset, end) };
    };

    const discardRead = (read: PendingRead | null): void =>
    {
      read?.promise.catch(() => undefined);
    };

    let pending: PendingRead | null = startRead(0);

    while (pending)
    {
      const current: PendingRead = pending;

      // Speculatively begin the next range read while the current chunk is still
      // being parsed, so the network round-trip overlaps with CPU work.
      let next: PendingRead | null = startRead(current.offset + current.expectedLength);

      const chunk: Buffer = await current.promise;

      // A short read invalidates the speculative offset; discard it and re-issue
      // from the true next offset so byte offsets stay exact.
      if (chunk.length !== current.expectedLength)
      {
        discardRead(next);
        next = startRead(current.offset + chunk.length);
      }

      const data: Buffer<ArrayBuffer> = Buffer.concat([remainder, chunk]);
      const dataBase: number = remainderStart;

      try
      {
        const result = yield* this.scanLines(data, dataBase, encoding, state);
        remainder = data.slice(result.lineStart);
        remainderStart = dataBase + result.lineStart;
      }
      catch (error)
      {
        discardRead(next);
        throw error;
      }

      pending = next;
    }

    if (remainder.length > 0)
    {
      const raw: Buffer<ArrayBuffer> = remainder;
      const lineText: string = EncodingService.decode(raw, encoding).replace(/\r\n$|\n$/, "");

      if (lineText)
      {
        yield [lineText, remainderStart, raw.length];
      }
    }
  }

  /**
   * Splits bytes to lines
   * @param data - The data to process
   * @param baseOffset - The base offset
   * @param encoding - The encoding
   * @param state - The state
   * @returns The generator<[string, number, number]> result
   */
  private *splitBytesToLines(data: Buffer, baseOffset: number, encoding: string, state: { inQuote: boolean }): Generator<[string, number, number]>
  {
    const result = yield* this.scanLines(data, baseOffset, encoding, state);

    if (result.lineStart < data.length)
    {
      const raw: Buffer = data.slice(result.lineStart);
      const text: string = EncodingService.decode(raw, encoding).replace(/\r\n$|\n$/, "");

      if (text.includes("Email:") && (text.match(/Email:/g) || []).length > 1)
      {
        const chunks: string[] = text.split(/\s+(?=Email:\s)/);
        let currentOffset: number = baseOffset + result.lineStart;
        for (const chunk of chunks)
        {
          if (chunk.trim())
          {
            const chunkBytes: number = Buffer.byteLength(chunk, encoding as BufferEncoding);
            yield [chunk.trim(), currentOffset, chunkBytes];
            currentOffset += chunkBytes + 1;
          }
        }
      }
      else if (text)
      {
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
  private *scanLines(data: Buffer, dataBase: number, encoding: string, state: { inQuote: boolean }): Generator<[string, number, number], { lineStart: number; endedAtBoundary: boolean }, void>
  {
    const config: Config = this.getConfig();
    const NL = 0x0a;
    const QUOTE = 0x22;
    let pos: number = 0;
    let lineStart: number = 0;
    let endedAtBoundary: boolean = false;
    let quotedNewlines: number = 0;

    const makeLine = (endExclusive: number): [string, number, number] => {
      const raw: Buffer = data.slice(lineStart, endExclusive);
      const tuple: [string, number, number] = [EncodingService.decode(raw, encoding).replace(/\r\n$|\n$/, ""), dataBase + lineStart, raw.length];
      lineStart = endExclusive;
      quotedNewlines = 0;
      return tuple;
    };

    while (pos < data.length)
    {
      const b :number = data[pos];

      if (b === QUOTE)
      {
        if (state.inQuote)
        {
          if (pos + 1 === data.length)
          {
            endedAtBoundary = true;
            break;
          }

          if (data[pos + 1] === QUOTE)
          {
            pos += 2;
            continue;
          }
        }
        state.inQuote = !state.inQuote;
        pos++;
        continue;
      }

      if (b === NL)
      {
        if (!state.inQuote)
        {
          yield makeLine(pos + 1);
        }
        else
        {
          quotedNewlines++;

          if (quotedNewlines > config.settings.MAX_QUOTED_NEWLINES || pos + 1 - lineStart >= config.settings.MAX_LINE_BYTES)
          {
            state.inQuote = false;
            yield makeLine(pos + 1);
          }
        }
      }

      pos++;

      if (pos - lineStart >= config.settings.MAX_LINE_BYTES)
      {
        state.inQuote = false;
        yield makeLine(pos);
      }
    }

    return { lineStart, endedAtBoundary };
  }
}

/**
 * Private capability token. Only GcsUtils.getInstance() has a reference to
 * this function, so it's the only call site that can satisfy the
 * constructor's `enforce` check — `new GcsUtils(...)` from anywhere else
 * fails fast with InstantiationError.
 */
function Enforce(): void {}
