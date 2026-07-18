import os from "os";
import path from "path";
import fs from "fs/promises";
import { createReadStream } from "fs";
import { pipeline } from "node:stream/promises";
import { randomUUID, createHash } from "crypto";
import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";
import { settings } from "../../shared/config.js";
import { OutputPart } from "../../shared/models/job.js";
import { pool } from "../../shared/db.js";
import { _gcsClient, putObject } from "../../shared/gcsUtils.js";

function estimateRowBytes(row: Record<string, any>): number 
{
  return Object.values(row).reduce((acc, v) => acc + (v === null ? 4 : String(v).length), 0) + Object.keys(row).length * 16;
}

export class ParquetWriterPool 
{
  private jobId: string;
  private bucket: string;
  private outputPrefix: string;
  private watermark: number;
  private buffers: { [templateId: string]: Record<string, any>[] };
  private bufferBytes: number;
  private parts: OutputPart[];
  private totalRows: number;
  private flushPromise: Promise<OutputPart[]> | null;

  constructor(jobId: string, bucket: string, outputPrefix: string, watermarkBytes = settings.RAM_FLUSH_WATERMARK) 
{
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

  write(
    row: Record<string, any>,
    templateId: string,
    templateVersion: number,
    byteOffset: number,
    byteLength: number,
    lineNo: number,
    rawLine: string
  ): void 
{
    if (typeof rawLine !== "string") 
{
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

  async flush(): Promise<OutputPart[]> 
{
    if (this.flushPromise) 
{
      await this.flushPromise;
    }
    this.flushPromise = this.doFlush();
    try 
{
      return await this.flushPromise;
    }
 finally 
{
      this.flushPromise = null;
    }
  }

  private async doFlush(): Promise<OutputPart[]> 
{
    const toFlush = this.buffers;
    this.buffers = {};
    this.bufferBytes = 0;
    const flushed: OutputPart[] = [];
    const failed: { [templateId: string]: Record<string, any>[] } = {};

    for (const [templateId, rows] of Object.entries(toFlush)) 
{
      if (!rows.length) continue;
      try 
{
        const part = await this.writePart(templateId, rows);
        this.parts.push(part);
        flushed.push(part);
      }
 catch (err) 
{
        console.error("parquet_writePart_failed", { jobId: this.jobId, templateId, error: String(err) });
        failed[templateId] = rows;
      }
    }

    for (const [templateId, rows] of Object.entries(failed)) 
{
      const existing = this.buffers[templateId] || [];
      this.buffers[templateId] = [...rows, ...existing];
      for (const r of rows) this.bufferBytes += estimateRowBytes(r);
    }

    if (Object.keys(failed).length > 0) 
{
      throw new Error(`Parquet flush failed for ${Object.keys(failed).length} templates`);
    }
    return flushed;
  }

  private async writePart(templateId: string, rows: Record<string, any>[]): Promise<OutputPart> 
{
    const partId = randomUUID();
    for (const r of rows) r._part_id = partId;

    const schema = buildSchema(rows);
    const tempFile = path.join(os.tmpdir(), `${partId}.parquet`);
    const writer = await ParquetWriter.openFile(schema, tempFile);
    let i = 0;
    for (const row of rows) 
{
      await writer.appendRow(row);
      i += 1;
      if (i % 5000 === 0) 
{
        await new Promise((resolve) => setImmediate(resolve));
      }
    }
    await writer.close();

    const s3Key = `${this.outputPrefix}/parts/${templateId}/${partId}.parquet`;
    const uploadStream = _gcsClient().bucket(this.bucket).file(s3Key).createWriteStream({ contentType: "application/octet-stream" });
    const readStream = createReadStream(tempFile);
    const fileStat = await fs.stat(tempFile);

    try 
{
      await pipeline(readStream, uploadStream);
    }
 finally 
{
      await fs.unlink(tempFile).catch(() => 
{});
    }

    const part: OutputPart = {
      part_id: partId,
      job_id: this.jobId,
      template_id: templateId,
      s3_path: `s3:
      row_count: rows.length,
      byte_size: fileStat.size,
      created_at: new Date().toISOString(),
    };

    try 
{
      await pool.query(
        `INSERT INTO output_parts (part_id, job_id, template_id, s3_path, row_count, byte_size, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (part_id) DO NOTHING`,
        [partId, this.jobId, templateId, part.s3_path, rows.length, fileStat.size, new Date()]
      );
    }
 catch (e) 
{
      console.error("output_parts_insert_failed", { jobId: this.jobId, partId, error: String(e) });
    }

    return part;
  }

  get bufferedRows(): number 
{
    return Object.values(this.buffers).reduce((a, b) => a + b.length, 0);
  }

  get bufferedBytes(): number 
{
    return this.bufferBytes;
  }

  get allPartPaths(): string[] 
{
    return this.parts.map((p) => p.s3_path);
  }
}

export class RubbishLogWriter 
{
  private jobId: string;
  private bucket: string;
  private s3Key: string;
  private buffer: string[];
  private count: number;

  constructor(jobId: string, bucket: string, outputPrefix: string) 
{
    this.jobId = jobId;
    this.bucket = bucket;
    this.s3Key = `${outputPrefix.replace(/\/$/, "")}/rubbish_log/${jobId}.ndjson`;
    this.buffer = [];
    this.count = 0;
  }

  write(byteOffset: number, lineNo: number, rawLine: string, templateId: string): void 
{
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

  async flush(): Promise<string | null> 
{
    if (!this.buffer.length) return null;
    const body = Buffer.from(this.buffer.join("\n"));
    await putObject(this.bucket, this.s3Key, body, "application/x-ndjson");
    this.buffer = [];
    return `s3:
  }

  getCounter(): number 
{
    return this.count;
  }
}

export class DLQWriter 
{
  private jobId: string;
  private count: number;

  constructor(jobId: string) 
{
    this.jobId = jobId;
    this.count = 0;
  }

  async write(byteOffset: number, byteLength: number, lineNo: number, rawLine: string, failureClass: string, error: string): Promise<void> 
{
    const { sendMessage } = await import("../../shared/queueUtils.js");
    const { _FailureClassFailureClass_FailureClass } = await import("../../shared/models/job.js");
    const dlqId = randomUUID();
    const rawBytes = Buffer.from(rawLine.replace(/\0/g, ""), "utf-8").toString("base64");
    await pool.query(
      `INSERT INTO dead_letters
        (dlq_id, job_id, byte_offset, byte_length, line_no, raw_bytes, failure_class, error, attempts, status, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())
       ON CONFLICT (dlq_id) DO NOTHING`,
      [dlqId, this.jobId, byteOffset, byteLength, lineNo, rawBytes, failureClass, error, 0, "pending"]
    );
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

  getCounter(): number 
{
    return this.count;
  }
}

export function buildSchema(rows: Record<string, any>[]): ParquetSchema 
{
  const schemaObj: any = {};
  for (const row of rows) 
{
    for (const [k, v] of Object.entries(row)) 
{
      if (!schemaObj[k]) 
{
        schemaObj[k] = { type: typeForValue(v), optional: true };
      }
    }
  }
  return new ParquetSchema(schemaObj);
}

function typeForValue(v: any): string 
{
  if (v === null || v === undefined) return "UTF8";
  if (typeof v === "boolean") return "BOOLEAN";
  if (typeof v === "number") return Number.isInteger(v) && Number.isSafeInteger(v) ? "INT64" : "DOUBLE";
  if (v instanceof Date) return "TIMESTAMP_MILLIS";
  return "UTF8";
}
