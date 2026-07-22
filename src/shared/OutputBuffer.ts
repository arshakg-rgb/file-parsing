import os from "os";
import path from "path";
import fs from "fs/promises";
import { ParquetSchema, ParquetWriter, type SchemaDefinition, type ParquetType } from "@dsnp/parquetjs";
import { parquetOutputService } from "./ParquetOutputService.js";

export interface OutputRow {
  [key: string]: unknown;
}

/**
 * Sanitizes parquet value
 * @param value - The value to use
 * @param isRecord - The is record
 * @returns The unknown result
 */
function sanitizeParquetValue(value: unknown, isRecord = false): unknown {
  if (value === null || value === undefined) return value;
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "boolean" || typeof value === "number" || typeof value === "string") return value;
  if (value instanceof Date) return value;

  const anyValue = value as { toNumber?: () => number };
  if (typeof anyValue.toNumber === "function") {
    try {
      const n = anyValue.toNumber();
      if (Number.isFinite(n)) return n;
    } catch { /* fall through */ }
  }

  if (Buffer.isBuffer(value)) return value.toString("utf-8");
  if (value instanceof Uint8Array) return Buffer.from(value).toString("utf-8");
  if (Array.isArray(value)) return JSON.stringify(value);

  if (typeof value === "object" && isRecord) {
    const result: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) {
      result[k] = sanitizeParquetValue(v, false);
    }
    return result;
  }

  return JSON.stringify(value);
}

/**
 * Performs the type for value operation.
 * @param v - The v
 * @returns The parquet type result
 */
function typeForValue(v: unknown): ParquetType {
  const value = sanitizeParquetValue(v, false);
  if (value === null || value === undefined) return "UTF8";
  if (typeof value === "boolean") return "BOOLEAN";
  if (typeof value === "number") return Number.isInteger(value) && Number.isSafeInteger(value) ? "INT64" : "DOUBLE";
  if (value instanceof Date) return "TIMESTAMP_MILLIS";
  return "UTF8";
}

/**
 * Builds schema
 * @param rows - The rows
 * @returns The parquet schema result
 */
function buildSchema(rows: Record<string, unknown>[]): ParquetSchema {
  const schemaObj: SchemaDefinition = {};
  for (const row of rows) {
    const sanitized = sanitizeParquetValue(row, true) as Record<string, unknown>;
    for (const [k, v] of Object.entries(sanitized)) {
      if (!schemaObj[k]) {
        schemaObj[k] = { type: typeForValue(v), optional: true };
      }
    }
  }
  return new ParquetSchema(schemaObj);
}

/**
 * OutputBuffer is responsible for output buffer operations.
 */
export class OutputBuffer {
    /**
   * Rows
   * @private
   */
  private rows: OutputRow[] = [];
    /**
   * Template Id
   * @private
   */
  private templateId: string;
    /**
   * Part Id
   * @private
   */
  private partId: string;
    /**
   * Job Id
   * @private
   */
  private jobId: string;
    /**
   * Flush Promise
   * @private
   */
  private flushPromise: Promise<string | null> | null = null;
    /**
   * Flush Counter
   * @private
   */
  private flushCounter = 0;
  /**
   * Estimated byte size of rows currently buffered
   * @private
   */
  private rowBytes = 0;

    /**
   * Constructs a new OutputBuffer instance.
   * @param jobId - The job identifier
   * @param templateId - The template id
   */
  constructor(jobId: string, templateId: string) {
    this.jobId = jobId;
    this.templateId = templateId;
    this.partId = `${jobId}-${templateId}-${Date.now()}`;
  }

  private estimateRowBytes(row: OutputRow): number {
    let size = 0;
    for (const v of Object.values(row)) {
      if (v === null || v === undefined) {
        size += 4;
      } else if (typeof v === "number") {
        size += 8;
      } else if (v instanceof Date) {
        size += 24;
      } else if (typeof v === "boolean") {
        size += 4;
      } else {
        size += Buffer.byteLength(String(v), "utf8") + 16;
      }
    }
    return size;
  }

    /**
   * Adds row
   * @param row - The row
   */
  addRow(row: OutputRow): void {
    this.rows.push(row);
    this.rowBytes += this.estimateRowBytes(row);

    const overLine = this.rows.length >= parquetOutputService.getFlushLineThreshold();
    const overBytes = this.rowBytes >= parquetOutputService.getFlushByteThreshold();
    if ((overLine || overBytes) && !this.flushPromise) {
      this.flushPromise = this.flush().finally(() => {
        this.flushPromise = null;
      });
    }
  }

    /**
   * Flushes the operation
   * @returns A promise that resolves to the result
   */
  async flush(): Promise<string | null> {
    if (this.rows.length === 0) {
      return null;
    }

    const rowsToFlush = this.rows;
    this.rows = [];
    this.rowBytes = 0;

    const flushPartId = `${this.partId}-${this.flushCounter++}`;

    parquetOutputService.getLogger().info("parquet_flush", {
      part_id: flushPartId,
      row_count: rowsToFlush.length,
      template_id: this.templateId,
    });

    const tempFile = path.join(os.tmpdir(), `${flushPartId}.parquet`);
    try {
      const sanitizedRows = rowsToFlush.map((row) => sanitizeParquetValue(row, true) as Record<string, unknown>);
      const schema = buildSchema(sanitizedRows);
      const writer = await ParquetWriter.openFile(schema, tempFile);

      for (const row of sanitizedRows) {
        await writer.appendRow(row);
      }

      await writer.close();

      const config = parquetOutputService.getGcsUtils().getConfig();
      const gcsPath = `gs://${config.settings.DATA_BUCKET}/output/${flushPartId}.parquet`;
      await parquetOutputService.getGcsUtils().putObjectFromFile(
        config.settings.DATA_BUCKET,
        `output/${flushPartId}.parquet`,
        tempFile,
        "application/octet-stream"
      );

      return gcsPath;
    } catch (error) {
      parquetOutputService.getLogger().error("parquet_flush_error", { part_id: flushPartId, error: String(error) });
      throw error;
    } finally {
      await fs.unlink(tempFile).catch(() => {});
    }
  }

    /**
   * Waits for for pending flush
   */
  async waitForPendingFlush(): Promise<void> {
    if (this.flushPromise) {
      await this.flushPromise;
    }
  }

    /**
   * Gets part id
   * @returns The string result
   */
  getPartId(): string {
    return this.partId;
  }

    /**
   * Gets row count
   * @returns The numeric result
   */
  getRowCount(): number {
    return this.rows.length;
  }
}
