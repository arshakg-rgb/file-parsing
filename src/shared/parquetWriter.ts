import os from "os";
import path from "path";
import fs from "fs/promises";
import { createReadStream } from "fs";
import { pipeline } from "node:stream/promises";
import { randomUUID, createHash } from "crypto";
import { ParquetSchema, ParquetWriter, type SchemaDefinition, type ParquetType } from "@dsnp/parquetjs";
import Config from "../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import FirestoreCacheUtils from "../utils/cache/FirestoreCacheUtils.js";
import { createLogger, Logger } from "../utils/logger/logger.js";

class ParquetOutputService extends ServiceManager {
  protected static instance: ParquetOutputService;
  private logger: Logger;
  private gcsUtils: FirestoreCacheUtils;
  private FLUSH_LINE_THRESHOLD: number;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate ParquetOutputService directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("parquet-writer");
    this.gcsUtils = FirestoreCacheUtils.getInstance();
    this.FLUSH_LINE_THRESHOLD = 1000;
  }

  public static getInstance(): ParquetOutputService {
    if (!ParquetOutputService.instance) {
      ParquetOutputService.instance = new ParquetOutputService(Enforce);
    }
    return ParquetOutputService.instance;
  }

  public getLogger(): Logger {
    return this.logger;
  }

  public getGcsUtils(): FirestoreCacheUtils {
    return this.gcsUtils;
  }

  public getFlushLineThreshold(): number {
    return this.FLUSH_LINE_THRESHOLD;
  }
}


export interface OutputRow {
  [key: string]: unknown;
}

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

function estimateRowBytes(row: Record<string, unknown>): number {
  let bytes = 0;
  for (const v of Object.values(row)) {
    const s = sanitizeParquetValue(v, false);
    bytes += (s === null || s === undefined ? 4 : String(s).length);
  }
  return bytes + Object.keys(row).length * 16;
}

function typeForValue(v: unknown): ParquetType {
  const value = sanitizeParquetValue(v, false);
  if (value === null || value === undefined) return "UTF8";
  if (typeof value === "boolean") return "BOOLEAN";
  if (typeof value === "number") return Number.isInteger(value) && Number.isSafeInteger(value) ? "INT64" : "DOUBLE";
  if (value instanceof Date) return "TIMESTAMP_MILLIS";
  return "UTF8";
}

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

export class OutputBuffer {
  private rows: OutputRow[] = [];
  private templateId: string;
  private partId: string;
  private jobId: string;
  private service: ParquetOutputService;
  private flushPromise: Promise<string | null> | null = null;
  private flushCounter = 0;

  constructor(jobId: string, templateId: string) {
    this.jobId = jobId;
    this.templateId = templateId;
    this.partId = `${jobId}-${templateId}-${Date.now()}`;
    this.service = ParquetOutputService.getInstance();
  }

  addRow(row: OutputRow): void {
    this.rows.push(row);
    
    if (this.rows.length >= this.service.getFlushLineThreshold() && !this.flushPromise) {
      this.flushPromise = this.flush().finally(() => {
        this.flushPromise = null;
      });
    }
  }

  async flush(): Promise<string | null> {
    if (this.rows.length === 0) {
      return null;
    }

    const rowsToFlush = this.rows;
    this.rows = [];

    const flushPartId = `${this.partId}-${this.flushCounter++}`;
    
    this.service.getLogger().info("parquet_flush", { 
      part_id: flushPartId, 
      row_count: rowsToFlush.length,
      template_id: this.templateId 
    });

    try {
      const sanitizedRows = rowsToFlush.map((row) => sanitizeParquetValue(row, true) as Record<string, unknown>);
      const schema = buildSchema(sanitizedRows);
      const tempFile = path.join(os.tmpdir(), `${flushPartId}.parquet`);
      const writer = await ParquetWriter.openFile(schema, tempFile);
      
      for (const row of sanitizedRows) {
        await writer.appendRow(row);
      }
      
      await writer.close();

      const buffer = await fs.readFile(tempFile);
      const config = Config.getInstance();
      const gcsPath = `gs://${config.settings.DATA_BUCKET}/output/${flushPartId}.parquet`;
      await this.service.getGcsUtils().putObject(config.settings.DATA_BUCKET, `output/${flushPartId}.parquet`, buffer);

      await fs.unlink(tempFile).catch(() => {});

      return gcsPath;
    } catch (error) {
      this.service.getLogger().error("parquet_flush_error", { part_id: flushPartId, error: String(error) });
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
  private buffers = new Map<string, OutputBuffer>();

  getBuffer(jobId: string, templateId: string): OutputBuffer {
    const key = `${jobId}-${templateId}`;
    if (!this.buffers.has(key)) {
      this.buffers.set(key, new OutputBuffer(jobId, templateId));
    }
    return this.buffers.get(key)!;
  }

  async flushAll(): Promise<string[]> {
    const paths: string[] = [];
    
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
