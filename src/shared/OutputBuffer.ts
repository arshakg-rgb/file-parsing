import os from "os";
import path from "path";
import fs from "fs/promises";
import { ParquetSchema, ParquetWriter, type SchemaDefinition, type ParquetType } from "@dsnp/parquetjs";
import { parquetOutputService } from "./ParquetOutputService.js";
import Config from "@config/system-config/Config";
import {OutputRow} from "@shared/io/IOutputBuffer";

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
   * Flush Promise
   * @private
   */

  private flushPromise: Promise<string | null> | null = null;

  /**
   * Flush Counter
   * @private
   */

  private flushCounter: number = 0;

  /**
   * Flushed Paths
   * @private
   */

  private flushedPaths: string[] = [];

  /**
   * Constructs a new OutputBuffer instance.
   * Private — use OutputBuffer.getInstance(jobId) instead.
   * @param jobId - The job identifier
   */

  private constructor(jobId: string)
  {
    this.partId = jobId;
  }

  /**
   * Returns the singleton OutputBuffer for the given jobId, creating it
   * on first access.
   * @param jobId - The job identifier
   * @returns The OutputBuffer instance for this job
   */

  public static getInstance(jobId: string): OutputBuffer
  {
    let instance: OutputBuffer | undefined = OutputBuffer.instances.get(jobId);

    if (!instance)
    {
      instance = new OutputBuffer(jobId);
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

    if (typeof value === "boolean" || typeof value === "number" || typeof value === "string")
    {
      return value;
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
   * Infers the parquet column type for a given sanitized value.
   * @param v - The value to infer a type for
   * @returns The parquet type
   * @private
   */

  private static typeForValue(v: unknown): ParquetType
  {
    const value: unknown = OutputBuffer.sanitizeParquetValue(v, false);

    if (value === null || value === undefined)
    {
      return "UTF8";
    }

    if (typeof value === "boolean")
    {
      return "BOOLEAN";
    }

    if (typeof value === "number")
    {
      return Number.isInteger(value) && Number.isSafeInteger(value) ? "INT64" : "DOUBLE";
    }

    if (value instanceof Date)
    {
      return "TIMESTAMP_MILLIS";
    }

    return "UTF8";
  }

  /**
   * Builds a parquet schema by inspecting a set of already-sanitized rows.
   * @param rows - The sanitized rows to infer the schema from
   * @returns The inferred parquet schema
   * @private
   */

  private static buildSchema(rows: Record<string, unknown>[]): ParquetSchema
  {
    const schemaObj: SchemaDefinition = {};

    for (const row of rows)
    {
      const sanitized = OutputBuffer.sanitizeParquetValue(row, true) as Record<string, unknown>;

      for (const [k, v] of Object.entries(sanitized))
      {
        if (!schemaObj[k])
        {
          schemaObj[k] = { type: OutputBuffer.typeForValue(v), optional: true };
        }
      }
    }
    return new ParquetSchema(schemaObj);
  }

  /**
   * Adds row
   * @param row - The row
   */

  public addRow(row: OutputRow): void
  {
    this.rows.push(row);
  }

  /**
   * Flushes the operation
   * @returns A promise that resolves to the result
   */

  public async flush(): Promise<string | null>
  {
    if (this.rows.length === 0)
    {
      return null;
    }

    const rowsToFlush: OutputRow[] = this.rows;
    this.rows = [];

    const flushPartId: string = this.flushCounter === 0 ? this.partId : `${this.partId}-${this.flushCounter}`;
    this.flushCounter++;

    const flushOperation = async (): Promise<string | null> =>
    {
      parquetOutputService.getLogger().info("parquet_flush", {
        part_id: flushPartId,
        row_count: rowsToFlush.length,
      });

      try
      {
        const sanitizedRows: Record<string, unknown>[] = rowsToFlush.map(
            (row) => OutputBuffer.sanitizeParquetValue(row, true) as Record<string, unknown>,
        );
        const schema: ParquetSchema = OutputBuffer.buildSchema(sanitizedRows);
        const tempFile: string = path.join(os.tmpdir(), `${flushPartId}.parquet`);
        const writer: ParquetWriter = await ParquetWriter.openFile(schema, tempFile);

        for (const row of sanitizedRows)
        {
          await writer.appendRow(row);
        }

        await writer.close();

        const buffer = await fs.readFile(tempFile);
        const config: Config = parquetOutputService.getGcsUtils().getConfig();
        const gcsPath = `gs://${config.settings.DATA_BUCKET}/output/${flushPartId}.parquet`;
        await parquetOutputService.getGcsUtils().putObject(config.settings.DATA_BUCKET, `output/${flushPartId}.parquet`, buffer);
        this.flushedPaths.push(gcsPath);

        await fs.unlink(tempFile).catch(() => {});

        return gcsPath;
      }
      catch (error)
      {
        parquetOutputService.getLogger().error("parquet_flush_error", { part_id: flushPartId, error: String(error) });
        throw error;
      }
    };

    this.flushPromise = flushOperation();

    try
    {
      return await this.flushPromise;
    }
    finally
    {
      this.flushPromise = null;
    }
  }

  /**
   * Waits for for pending flush
   */

  public async waitForPendingFlush(): Promise<void>
  {
    if (this.flushPromise)
    {
      await this.flushPromise;
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
}
