import os from "os";
import path from "path";
import fs from "fs/promises";
import { createReadStream } from "fs";
import { pipeline } from "node:stream/promises";
import { randomUUID, createHash } from "crypto";
import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";
import { settings } from "./config.js";
import { putObject } from "./gcsUtils.js";
import { createLogger } from "./logger.js";

const logger = createLogger("parquet-writer");

export interface OutputRow {
  [key: string]: any;
}

function estimateRowBytes(row: Record<string, any>): number {
  return Object.values(row).reduce((acc, v) => acc + (v === null ? 4 : String(v).length), 0) + Object.keys(row).length * 16;
}

function typeForValue(v: any): string {
  if (v === null || v === undefined) return "UTF8";
  if (typeof v === "boolean") return "BOOLEAN";
  if (typeof v === "number") return Number.isInteger(v) && Number.isSafeInteger(v) ? "INT64" : "DOUBLE";
  if (v instanceof Date) return "TIMESTAMP_MILLIS";
  return "UTF8";
}

function buildSchema(rows: Record<string, any>[]): ParquetSchema {
  const schemaObj: any = {};
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

export class OutputBuffer {
  private rows: OutputRow[] = [];
  private templateId: string;
  private partId: string;
  private jobId: string;
  private readonly FLUSH_LINE_THRESHOLD = 1000; // Flush after exactly 1000 lines
  private flushPromise: Promise<string | null> | null = null;
  private flushCounter = 0; // Counter to ensure unique partId for each flush

  constructor(jobId: string, templateId: string) {
    this.jobId = jobId;
    this.templateId = templateId;
    this.partId = `${jobId}-${templateId}-${Date.now()}`;
  }

  addRow(row: OutputRow): void {
    this.rows.push(row);
    
    // Flush after exactly 1000 lines - do not await to avoid blocking
    // But we need to handle the case where flush is already in progress
    if (this.rows.length >= this.FLUSH_LINE_THRESHOLD && !this.flushPromise) {
      this.flushPromise = this.flush().finally(() => {
        this.flushPromise = null;
      });
    }
  }

  async flush(): Promise<string | null> {
    if (this.rows.length === 0) {
      return null;
    }

    // CRITICAL: Snapshot and clear rows atomically BEFORE any async work
    // This prevents rows added during flush from being silently dropped
    const rowsToFlush = this.rows;
    this.rows = []; // New array immediately - addRow() calls after this point are safe

    // Generate unique partId for this flush to avoid race conditions
    const flushPartId = `${this.partId}-${this.flushCounter++}`;
    
    logger.info("parquet_flush", { 
      part_id: flushPartId, 
      row_count: rowsToFlush.length,
      template_id: this.templateId 
    });

    try {
      const schema = buildSchema(rowsToFlush);
      const tempFile = path.join(os.tmpdir(), `${flushPartId}.parquet`);
      const writer = await ParquetWriter.openFile(schema, tempFile);
      
      for (const row of rowsToFlush) {
        await writer.appendRow(row);
      }
      
      await writer.close();

      const buffer = await fs.readFile(tempFile);
      const gcsPath = `gs://${settings.DATA_BUCKET}/output/${flushPartId}.parquet`;
      await putObject(settings.DATA_BUCKET, `output/${flushPartId}.parquet`, buffer);

      await fs.unlink(tempFile).catch(() => {});

      return gcsPath;
    } catch (error) {
      logger.error("parquet_flush_error", { part_id: flushPartId, error: String(error) });
      throw error;
    }
  }

  async waitForPendingFlush(): Promise<void> {
    if (this.flushPromise) {
      await this.flushPromise;
    }
  }

  getPartId(): string {
    return this.partId;
  }

  getRowCount(): number {
    return this.rows.length;
  }
}

export class OutputManager {
  private buffers = new Map<string, OutputBuffer>(); // templateId -> buffer

  getBuffer(jobId: string, templateId: string): OutputBuffer {
    const key = `${jobId}-${templateId}`;
    if (!this.buffers.has(key)) {
      this.buffers.set(key, new OutputBuffer(jobId, templateId));
    }
    return this.buffers.get(key)!;
  }

  async flushAll(): Promise<string[]> {
    const paths: string[] = [];
    
    // Wait for any pending flushes to complete before clearing buffers
    // This prevents race conditions when flushAll() is called while a threshold flush is in progress
    for (const buffer of this.buffers.values()) {
      await buffer.waitForPendingFlush();
      const path = await buffer.flush();
      if (path) {
        paths.push(path);
      }
    }
    
    this.buffers.clear();
    return paths;
  }

  async flushTemplate(jobId: string, templateId: string): Promise<string | null> {
    const key = `${jobId}-${templateId}`;
    const buffer = this.buffers.get(key);
    if (buffer) {
      const path = await buffer.flush();
      this.buffers.delete(key);
      return path;
    }
    return null;
  }
}
