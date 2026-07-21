import { OutputPartCreationAttributes } from "@config/db/models/OutputPart.js";
import os from "os";
import path from "path";
import fs from "fs/promises";
import { createReadStream } from "fs";
import { pipeline } from "node:stream/promises";
import { randomUUID, createHash } from "crypto";
import { ParquetSchema, ParquetWriter, type SchemaDefinition, type ParquetType } from "@dsnp/parquetjs";
import { settings } from "@shared/Settings.js";
import { OutputPart } from "@shared/models/job.js";
import { repositories } from "@shared/DatabaseManager.js";
import { gcsClient, putObject } from "@shared/GcsUtils.js";

/**
 * Estimates row bytes
 * @param row - The row
 * @returns The numeric result
 */
function estimateRowBytes(row: Record<string, unknown>): number {
  let bytes = 0;
  for (const v of Object.values(row)) {
    bytes += (v === null ? 4 : String(v).length);
  }
  return bytes + Object.keys(row).length * 16;
}

/**
 * ParquetWriterPool is responsible for parquet writer pool operations.
 */
export class ParquetWriterPool {
    /**
   * Job Id
   * @private
   */
  private jobId: string;
    /**
   * Bucket
   * @private
   */
  private bucket: string;
    /**
   * Output Prefix
   * @private
   */
  private outputPrefix: string;
    /**
   * Watermark
   * @private
   */
  private watermark: number;
    /**
   * Buffers
   * @private
   */
  private buffers: { [templateId: string]: Record<string, unknown>[] };
    /**
   * Buffer Bytes
   * @private
   */
  private bufferBytes: number;
    /**
   * Parts
   * @private
   */
  private parts: OutputPart[];
    /**
   * Total Rows
   * @private
   */
  private totalRows: number;
    /**
   * Flush Promise
   * @private
   */
  private flushPromise: Promise<OutputPart[]> | null;

    /**
   * Constructs a new ParquetWriterPool instance.
   * @param jobId - The job identifier
   * @param bucket - The bucket
   * @param outputPrefix - The output prefix
   * @param watermarkBytes - The watermark bytes
   */
  constructor(jobId: string, bucket: string, outputPrefix: string, watermarkBytes = settings.RAM_FLUSH_WATERMARK) {
    this.jobId = jobId;
    this.bucket = bucket;
    this.outputPrefix = outputPrefix.replace(/\/$/, "");
    this.watermark = watermarkBytes;
    this.buffers = {};
    this.bufferBytes = 0;
    this.parts = [];
    this.totalRows = 0;
    this.flushPromise = null;
  }

    /**
   * Writes the operation
   * @param row - The row
   * @param templateId - The template id
   * @param templateVersion - The template version
   * @param byteOffset - The byte offset
   * @param byteLength - The byte length
   * @param lineNo - The line no
   * @param rawLine - The raw line
   */
  write(
    row: Record<string, unknown>,
    templateId: string,
    templateVersion: number,
    byteOffset: number,
    byteLength: number,
    lineNo: number,
    rawLine: string
  ): void {
    if (typeof rawLine !== "string") {
      console.error("parquet_write_invalid_rawline", { jobId: this.jobId, rawLineType: typeof rawLine, byteOffset, lineNo });
      return;
    }
    const checksum = createHash("sha256").update(rawLine).digest("hex");
    const parsedAt = new Date();
    const fullRow = {
      ...row,
      _job_id: this.jobId,
      _byte_offset: byteOffset,
      _byte_length: byteLength,
      _record_index: this.totalRows,
      _line_no: lineNo,
      _template_id: templateId,
      _template_version: templateVersion,
      _checksum: checksum,
      _parsed_at: parsedAt,
    };

    if (!this.buffers[templateId]) this.buffers[templateId] = [];
    this.buffers[templateId].push(fullRow);
    this.bufferBytes += estimateRowBytes(fullRow);
    this.totalRows += 1;
  }

    /**
   * Flushes the operation
   * @returns A promise that resolves to the list
   */
  async flush(): Promise<OutputPart[]> {
    if (this.flushPromise) {
      await this.flushPromise;
    }
    this.flushPromise = this.doFlush();
    try {
      return await this.flushPromise;
    } finally {
      this.flushPromise = null;
    }
  }

    /**
   * Performs the do flush operation.
   * @returns A promise that resolves to the list
   */
  private async doFlush(): Promise<OutputPart[]> {
    const toFlush = this.buffers;
    this.buffers = {};
    this.bufferBytes = 0;
    const flushed: OutputPart[] = [];
    const failed: { [templateId: string]: Record<string, unknown>[] } = {};

    for (const [templateId, rows] of Object.entries(toFlush)) {
      if (!rows.length) continue;
      try {
        const part = await this.writePart(templateId, rows);
        this.parts.push(part);
        flushed.push(part);
      } catch (err) {
        console.error("parquet_writePart_failed", { jobId: this.jobId, templateId, error: String(err) });
        failed[templateId] = rows;
      }
    }

    // Re-queue failed rows so they are retried on the next flush.
    for (const [templateId, rows] of Object.entries(failed)) {
      const existing = this.buffers[templateId] || [];
      this.buffers[templateId] = [...rows, ...existing];
      for (const r of rows) this.bufferBytes += estimateRowBytes(r);
    }

    if (Object.keys(failed).length > 0) {
      throw new Error(`Parquet flush failed for ${Object.keys(failed).length} templates`);
    }
    return flushed;
  }

    /**
   * Writes part
   * @param templateId - The template id
   * @param rows - The rows
   * @returns A promise that resolves to the result
   */
  private async writePart(templateId: string, rows: Record<string, unknown>[]): Promise<OutputPart> {
    const partId = randomUUID();
    for (const r of rows) r._part_id = partId;

    const schema = buildSchema(rows);
    const tempFile = path.join(os.tmpdir(), `${partId}.parquet`);
    const writer = await ParquetWriter.openFile(schema, tempFile);
    let i = 0;
    for (const row of rows) {
      await writer.appendRow(row);
      i += 1;
      if (i % 5000 === 0) {
        await new Promise((resolve) => setImmediate(resolve));
      }
    }
    await writer.close();

    const s3Key = `${this.outputPrefix}/parts/${templateId}/${partId}.parquet`;
    const uploadStream = gcsClient().bucket(this.bucket).file(s3Key).createWriteStream({ contentType: "application/octet-stream" });
    const readStream = createReadStream(tempFile);
    const fileStat = await fs.stat(tempFile);

    try {
      await pipeline(readStream, uploadStream);
    } finally {
      await fs.unlink(tempFile).catch(() => {});
    }

    const part: OutputPart = {
      part_id: partId,
      job_id: this.jobId,
      template_id: templateId,
      s3_path: `s3://${this.bucket}/${s3Key}`,
      row_count: rows.length,
      byte_size: fileStat.size,
      created_at: new Date().toISOString(),
    };

    try {
      await repositories.outputParts.create(part as OutputPartCreationAttributes);
    } catch (e) {
      console.error("output_parts_insert_failed", { jobId: this.jobId, partId, error: String(e) });
    }

    return part;
  }

    /**
   * Gets the buffered rows.
   * @returns The numeric result
   */
  get bufferedRows(): number {
    return Object.values(this.buffers).reduce((a, b) => a + b.length, 0);
  }

    /**
   * Gets the buffered bytes.
   * @returns The numeric result
   */
  get bufferedBytes(): number {
    return this.bufferBytes;
  }

    /**
   * Gets the all part paths.
   * @returns The list of results
   */
  get allPartPaths(): string[] {
    return this.parts.map((p) => p.s3_path);
  }
}

/**
 * RubbishLogWriter is responsible for rubbish log writer operations.
 */
export class RubbishLogWriter {
    /**
   * Job Id
   * @private
   */
  private jobId: string;
    /**
   * Bucket
   * @private
   */
  private bucket: string;
    /**
   * S3 Key
   * @private
   */
  private s3Key: string;
    /**
   * Buffer
   * @private
   */
  private buffer: string[];
    /**
   * Count
   * @private
   */
  private count: number;

    /**
   * Constructs a new RubbishLogWriter instance.
   * @param jobId - The job identifier
   * @param bucket - The bucket
   * @param outputPrefix - The output prefix
   */
  constructor(jobId: string, bucket: string, outputPrefix: string) {
    this.jobId = jobId;
    this.bucket = bucket;
    this.s3Key = `${outputPrefix.replace(/\/$/, "")}/rubbish_log/${jobId}.ndjson`;
    this.buffer = [];
    this.count = 0;
  }

    /**
   * Writes the operation
   * @param byteOffset - The byte offset
   * @param lineNo - The line no
   * @param rawLine - The raw line
   * @param templateId - The template id
   */
  write(byteOffset: number, lineNo: number, rawLine: string, templateId: string): void {
    this.buffer.push(
      JSON.stringify({
        job_id: this.jobId,
        byte_offset: byteOffset,
        line_no: lineNo,
        raw: rawLine,
        matched_template_id: templateId,
      })
    );
    this.count += 1;
  }

    /**
   * Flushes the operation
   * @returns A promise that resolves to the result
   */
  async flush(): Promise<string | null> {
    if (!this.buffer.length) return null;
    const body = Buffer.from(this.buffer.join("\n"));
    await putObject(this.bucket, this.s3Key, body, "application/x-ndjson");
    this.buffer = [];
    return `s3://${this.bucket}/${this.s3Key}`;
  }

    /**
   * Gets counter
   * @returns The numeric result
   */
  getCounter(): number {
    return this.count;
  }
}

/**
 * DLQWriter is responsible for d l q writer operations.
 */
export class DLQWriter {
    /**
   * Job Id
   * @private
   */
  private jobId: string;
    /**
   * Count
   * @private
   */
  private count: number;

    /**
   * Constructs a new DLQWriter instance.
   * @param jobId - The job identifier
   */
  constructor(jobId: string) {
    this.jobId = jobId;
    this.count = 0;
  }

    /**
   * Writes the operation
   * @param byteOffset - The byte offset
   * @param byteLength - The byte length
   * @param lineNo - The line no
   * @param rawLine - The raw line
   * @param failureClass - The failure class
   * @param error - The error that occurred
   */
  async write(byteOffset: number, byteLength: number, lineNo: number, rawLine: string, failureClass: string, error: string): Promise<void> {
    const { sendMessage } = await import("@shared/QueueService.js");
    const dlqId = randomUUID();
    const rawBytes = Buffer.from(rawLine.replace(/\0/g, ""), "utf-8").toString("base64");
    const row = await repositories.deadLetters.create(
      {
        dlq_id: dlqId,
        job_id: this.jobId,
        byte_offset: byteOffset,
        byte_length: byteLength,
        line_no: lineNo,
        raw_bytes: rawBytes,
        failure_class: failureClass,
        error,
        attempts: 0,
        status: "pending",
      },
      { conflictOn: "job_id_line_no" }
    );

    if (!row) return;

    await sendMessage(
      settings.DLQ_QUEUE_URL,
      {
        dlq_id: dlqId,
        job_id: this.jobId,
        byte_offset: byteOffset,
        byte_length: byteLength,
        line_no: lineNo,
        raw_bytes: rawBytes,
        failure_class: failureClass,
        error,
        attempts: 0,
        status: "pending",
      },
      0,
      this.jobId
    );
    this.count += 1;
  }

    /**
   * Gets counter
   * @returns The numeric result
   */
  getCounter(): number {
    return this.count;
  }
}

/**
 * Builds schema
 * @param rows - The rows
 * @returns The parquet schema result
 */
export function buildSchema(rows: Record<string, unknown>[]): ParquetSchema {
  const schemaObj: SchemaDefinition = {};
  for (const row of rows) {
    for (const [k, v] of Object.entries(row)) {
      if (!schemaObj[k]) {
        // All fields optional so sparse rows never throw "missing required field"
        schemaObj[k] = { type: typeForValue(v), optional: true };
      }
    }
  }
  return new ParquetSchema(schemaObj);
}

/**
 * Performs the type for value operation.
 * @param v - The v
 * @returns The parquet type result
 */
function typeForValue(v: unknown): ParquetType {
  if (v === null || v === undefined) return "UTF8";
  if (typeof v === "boolean") return "BOOLEAN";
  if (typeof v === "number") return Number.isInteger(v) && Number.isSafeInteger(v) ? "INT64" : "DOUBLE";
  if (v instanceof Date) return "TIMESTAMP_MILLIS";
  return "UTF8";
}
