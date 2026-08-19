import os from "os";
import path from "path";
import fs from "fs/promises";
import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";
import { parquetOutputService } from "./ParquetOutputService.js";
import { settings } from "./Settings.js";
import Config from "@config/system-config/Config";
import {OutputRow} from "@shared/io/IOutputBuffer";
import EncodingService from "@utils/normalizers/Encoding.js";

/**
 * OutputBuffer is responsible for buffering rows for a single job and
 * flushing them to Parquet files in GCS.
 *
 * This class is implemented as a keyed singleton: there is at most one
 * live OutputBuffer instance per jobId at any time. Use
 * `OutputBuffer.getInstance(jobId)` to obtain (or lazily create) the
 * instance for a given job, and `OutputBuffer.releaseInstance(jobId)`
 * once the job is fully flushed and the buffer is no longer needed, so
 * the registry doesn't grow unbounded across the process lifetime.
 *
 * All parquet-sanitization / schema-inference logic that used to live
 * as free functions at module scope is now encapsulated as private
 * static methods on this class, since it is implementation detail that
 * belongs to OutputBuffer and nothing else in this module needs it.
 *
 * Every member below declares its access modifier explicitly
 * (public / private / static) rather than relying on TypeScript's
 * implicit-public default, so the intended API surface is unambiguous
 * at a glance.
 *
 * The constructor is private to enforce singleton usage — callers must
 * go through `getInstance` rather than `new OutputBuffer(...)`.
 */
export class OutputBuffer
{
  /**
   * Registry of live instances keyed by jobId.
   * @private
   */
  private static readonly instances: Map<string, OutputBuffer> = new Map();

  /**
   * Rows
   * @private
   */
  private rows: OutputRow[] = [];

  /**
   * Part Id
   * @private
   */
  private readonly partId: string;

  /**
   * Total flushed rows
   * @private
   */
  private totalFlushed: number = 0;

  /**
   * Approximate pending bytes in the current buffer.
   * @private
   */
  private pendingBytes: number = 0;

  /**
   * Running estimate of bytes per row, refreshed periodically by sampling.
   * @private
   */
  private estimatedRowBytes: number = 0;

  /**
   * How often to recompute the per-row byte estimate.
   * @private
   */
  private static readonly PENDING_BYTES_SAMPLE_INTERVAL: number = 100;

  /**
   * Byte threshold for forcing a flush before the row-count threshold.
   * A 64 MB cap keeps merged parts well under the 64 MB merge limit.
   * @private
   */
  private static readonly FLUSH_THRESHOLD_BYTES: number = 64 * 1024 * 1024;

  /**
   * Template id associated with this buffer's parts (default "mixed" because a
   * single part can contain rows for several templates).
   * @private
   */
  private readonly templateId: string;

  /**
   * Pending flushes
   * @private
   */
  private pendingFlushes: Promise<void>[] = [];

  /**
   * Flushed Paths
   * @private
   */
  private flushedPaths: string[] = [];

  /**
   * Flushed part metadata for `output_parts` bookkeeping.
   * @private
   */
  private flushedParts: Array<{ path: string; row_count: number; template_id: string }> = [];

  /**
   * Fixed Parquet schema for all flushed parts. Building this once removes
   * the per-flush row-scan that used to infer types from the data.
   */
  private static readonly PARQUET_SCHEMA: ParquetSchema = new ParquetSchema({
    id: { type: "INT64", optional: true },
    _job_id: { type: "UTF8", optional: true },
    _byte_offset: { type: "INT64", optional: true },
    _byte_length: { type: "INT64", optional: true },
    _record_index: { type: "INT64", optional: true },
    _line_no: { type: "INT64", optional: true },
    _template_id: { type: "UTF8", optional: true },
    _template_version: { type: "INT64", optional: true },
    _checksum: { type: "UTF8", optional: true },
    _parsed_at: { type: "TIMESTAMP_MILLIS", optional: true },
    _part_id: { type: "UTF8", optional: true },
    fields: { type: "UTF8", optional: true },
  });

  /**
   * Constructs a new OutputBuffer instance.
   * Private — use OutputBuffer.getInstance(jobId) instead.
   * @param jobId - The job identifier
   * @param templateId - The template id to record for flushed parts
   */

  private constructor(jobId: string, templateId = "mixed")
  {
    this.partId = jobId;
    this.templateId = templateId;
    this.totalFlushed = 0;
  }

  /**
   * Returns the singleton OutputBuffer for the given jobId, creating it
   * on first access.
   * @param jobId - The job identifier
   * @param templateId - The template id to record for flushed parts
   * @returns The OutputBuffer instance for this job
   */

  public static getInstance(jobId: string, templateId = "mixed"): OutputBuffer
  {
    let instance: OutputBuffer | undefined = OutputBuffer.instances.get(jobId);

    if (!instance)
    {
      instance = new OutputBuffer(jobId, templateId);
      OutputBuffer.instances.set(jobId, instance);
    }

    return instance;
  }

  /**
   * Removes the singleton instance for a jobId from the registry.
   * Call this once a job's buffer has been fully flushed and drained
   * (e.g. after waitForPendingFlush()) to avoid leaking memory.
   * @param jobId - The job identifier
   */
  public static releaseInstance(jobId: string): void
  {
    OutputBuffer.instances.delete(jobId);
  }

  /**
   * Sanitizes a value so it can be safely written by the parquet writer.
   * @param value - The value to sanitize
   * @param isRecord - Whether the value is a nested record whose own
   *                   fields should be recursively sanitized
   * @returns The sanitized value
   * @private
   */
  private static sanitizeParquetValue(value: unknown, isRecord = false): unknown
  {
    if (value === null || value === undefined)
    {
      return value;
    }

    if (typeof value === "bigint")
    {
      return Number(value);
    }

    if (typeof value === "boolean" || typeof value === "number")
    {
      return value;
    }

    if (typeof value === "string")
    {
      return EncodingService.recoverMojibake(value);
    }

    if (value instanceof Date)
    {
      return value;
    }

    const anyValue = value as { toNumber?: () => number };

    if (typeof anyValue.toNumber === "function")
    {
      try
      {
        const n: number = anyValue.toNumber();

        if (Number.isFinite(n))
        {
          return n;
        }
      } catch { /* fall through */ }
    }

    if (Buffer.isBuffer(value))
    {
      return value.toString("utf-8");
    }

    if (value instanceof Uint8Array)
    {
      return Buffer.from(value).toString("utf-8");
    }

    if (Array.isArray(value))
    {
      return JSON.stringify(value);
    }

    if (typeof value === "object" && isRecord)
    {
      const result: Record<string, unknown> = {};

      for (const [k, v] of Object.entries(value))
      {
        result[k] = OutputBuffer.sanitizeParquetValue(v, false);
      }
      return result;
    }

    return JSON.stringify(value);
  }

  /**
   * Adds row and flushes when the threshold is reached.
   * Returns a promise only when backpressure forces the caller to await.
   * @param row - The row
   * @returns A promise if the caller must wait for pending flushes, otherwise undefined
   */

  public addRow(row: OutputRow): Promise<void> | undefined
  {
    this.rows.push(row);

    if (this.rows.length === 1 || this.rows.length % OutputBuffer.PENDING_BYTES_SAMPLE_INTERVAL === 0)
    {
      this.estimatedRowBytes = Buffer.byteLength(JSON.stringify(row), "utf8");
      this.pendingBytes = this.rows.length * this.estimatedRowBytes;
    }
    else
    {
      this.pendingBytes += this.estimatedRowBytes;
    }

    if (!this.shouldFlush())
    {
      return undefined;
    }

    const rowsToFlush: OutputRow[] = this.rows;
    this.rows = [];
    this.pendingBytes = 0;

    const startRow: number = this.totalFlushed;
    this.totalFlushed += rowsToFlush.length;
    const endRow: number = this.totalFlushed;

    const partName: string = `${this.partId}-${startRow}-${endRow}`;
    const flush: Promise<void> = this.flushPart(rowsToFlush, partName, 0).catch((error) => {
      parquetOutputService.getLogger().error("parquet_flush_error", { part_name: partName, error: String(error) });
      throw error;
    });
    this.pendingFlushes.push(flush);

    if (this.pendingFlushes.length >= settings.OUTPUT_BUFFER_MAX_PENDING_FLUSHES)
    {
      const pending: Promise<void>[] = this.pendingFlushes;
      this.pendingFlushes = [];
      return Promise.all(pending).then(() => {});
    }

    return undefined;
  }

  /**
   * Returns whether the buffer should flush.
   * @returns True if the buffer should flush
   * @private
   */

  private shouldFlush(): boolean
  {
    return this.rows.length >= settings.OUTPUT_BUFFER_FLUSH_THRESHOLD_ROWS ||
        this.pendingBytes >= settings.OUTPUT_BUFFER_FLUSH_THRESHOLD_BYTES;
  }

  /**
   * Flushes the remaining rows and waits for all pending flushes.
   * @returns A promise that resolves to the last flushed path, or null
   */

  public async flush(): Promise<string | null>
  {
    if (this.pendingFlushes.length > 0)
    {
      await Promise.all(this.pendingFlushes);
      this.pendingFlushes = [];
    }

    if (this.rows.length === 0)
    {
      return null;
    }

    const rowsToFlush: OutputRow[] = this.rows;
    this.rows = [];
    this.pendingBytes = 0;

    const startRow: number = this.totalFlushed;
    this.totalFlushed += rowsToFlush.length;
    const endRow: number = this.totalFlushed;
    const partName: string = `${this.partId}-${startRow}-${endRow}`;

    await this.flushPart(rowsToFlush, partName, 0);
    return this.flushedPaths[this.flushedPaths.length - 1] ?? null;
  }

  /**
   * Writes a single part to GCS.
   * @param rowsToFlush - The rows to write
   * @param partName - The part name
   * @param _bytesToFlush - The original pending bytes (for diagnostics)
   * @private
   */

  private async flushPart(rowsToFlush: OutputRow[], partName: string, _bytesToFlush: number): Promise<void>
  {
    parquetOutputService.getLogger().info("parquet_flush", {
      part_name: partName,
      row_count: rowsToFlush.length,
    });

    const sanitizedRows: Record<string, unknown>[] = rowsToFlush.map(
        (row) => OutputBuffer.sanitizeParquetValue(row, true) as Record<string, unknown>,
    );

    const baseTime = Date.now();
    const loadRows: Record<string, unknown>[] = sanitizedRows.map((row, i) => {
      const fields: Record<string, unknown> = {};

      for (const [k, v] of Object.entries(row))
      {
        if (!k.startsWith("_"))
        {
          fields[k] = v;
        }
      }

      return {
        id: baseTime + i,
        _job_id: row._job_id,
        _byte_offset: row._byte_offset,
        _byte_length: row._byte_length,
        _record_index: row._record_index,
        _line_no: row._line_no,
        _template_id: row._template_id,
        _template_version: row._template_version,
        _checksum: row._checksum,
        _parsed_at: row._parsed_at,
        _part_id: row._part_id,
        fields: JSON.stringify(fields),
      };
    });

    const tempFile: string = path.join(os.tmpdir(), `${partName}.parquet`);
    const writer: ParquetWriter = await ParquetWriter.openFile(OutputBuffer.PARQUET_SCHEMA, tempFile);

    for (const row of loadRows)
    {
      await writer.appendRow(row);
    }

    await writer.close();

    const config: Config = parquetOutputService.getGcsUtils().getConfig();
    const gcsPath = `gs://${config.settings.DATA_BUCKET}/output/${partName}.parquet`;
    try
    {
      await parquetOutputService.getGcsUtils().putFile(config.settings.DATA_BUCKET, `output/${partName}.parquet`, tempFile, "application/octet-stream");
    }
    finally
    {
      await fs.unlink(tempFile).catch(() => {});
    }

    this.flushedPaths.push(gcsPath);
    this.flushedParts.push({
      path: gcsPath,
      row_count: rowsToFlush.length,
      template_id: this.templateId,
    });
    parquetOutputService.getLogger().info("parquet_flush_complete", { part_name: partName, path: gcsPath, row_count: rowsToFlush.length });
  }

  /**
   * Waits for pending flushes
   */

  public async waitForPendingFlush(): Promise<void>
  {
    if (this.pendingFlushes.length > 0)
    {
      await Promise.all(this.pendingFlushes);
      this.pendingFlushes = [];
    }
  }

  /**
   * Gets flushed paths
   * @returns The flushed paths result
   */

  public getFlushedPaths(): string[]
  {
    return [...this.flushedPaths];
  }

  /**
   * Gets flushed part metadata for `output_parts` bookkeeping.
   * @returns The flushed parts result
   */

  public getFlushedParts(): Array<{ path: string; row_count: number; template_id: string }>
  {
    return [...this.flushedParts];
  }
}
