import pino from "pino";
import os from "os";
import path from "path";
import fs from "fs";
import { finished } from "node:stream/promises";
import type { WriteStream } from "node:fs";
import Config from "@config/system-config/Config.js";
import {FirestoreCacheUtils} from "@utils/cache/FirestoreCacheUtils.js";
import { createLogger } from "@utils/logger/Log.js";

/**
 * Performs the csv escape cell operation.
 * @param v - The v
 * @returns The string result
 */
export function csvEscapeCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  if (typeof v === "bigint") return String(v);
  if (v instanceof Date) return String(v);
  if (typeof v === "string") {
    return /[",\r\n]/.test(v) ? "\"" + v.replace(/"/g, "\"\"") + "\"" : v;
  }
  const s = JSON.stringify(v);
  return /[",\r\n]/.test(s) ? "\"" + s.replace(/"/g, "\"\"") + "\"" : s;
}

/**
 * CsvOutputWriter is responsible for csv output writer operations.
 */
export class CsvOutputWriter {
    /**
   * Tmp Path
   * @private
   */
  private readonly tmpPath: string;
    /**
   * Columns
   * @private
   */
  private readonly columns: string[];
    /**
   * Logger instance
   * @private
   */
  private readonly logger: pino.Logger;
    /**
   * Gcs Utils
   * @private
   */
  private readonly gcsUtils: FirestoreCacheUtils;
    /**
   * Config
   * @private
   */
  private readonly config: Config;

    /**
   * Row Count
   * @private
   */
  private rowCount = 0;
    /**
   * Failed
   * @private
   */
  private failed = false;
    /**
   * Pending
   * @private
   */
  private pending: string[] = [];
    /**
   * Pending Bytes
   * @private
   */
  private pendingBytes = 0;
    /**
   * The f l u s h_ t h r e s h o l d_ b y t e s value
   * @private
   */
  private static readonly FLUSH_THRESHOLD_BYTES = 32 * 1024 * 1024;
    /**
   * Output stream to the tmp CSV file.
   * @private
   */
  private readonly stream: WriteStream;

    /**
   * Constructs a new CsvOutputWriter instance.
   * @param jobId - The job identifier
   * @param fieldSpec - The field spec
   */
  constructor(private readonly jobId: string, fieldSpec: string[]) {
    const base = (fieldSpec && fieldSpec.length > 0 ? fieldSpec : ["value"]).filter((c) => c !== "meta");
    this.columns = [...new Set(base), "meta"];
    this.tmpPath = path.join(os.tmpdir(), `${jobId}-output.csv`);
    this.logger = createLogger(module);
    this.gcsUtils = FirestoreCacheUtils.getInstance();
    this.config = Config.getInstance();
    this.stream = fs.createWriteStream(this.tmpPath, { encoding: "utf8" });
    this.stream.write("\ufeff" + this.line([...this.columns]), "utf8");
  }

  private line(vals: unknown[]): string {
    return vals.map((v) => csvEscapeCell(v)).join(",") + "\r\n";
  }

    /**
   * Adds row
   * @param row - The row
   * @param _lineNo - The _line no
   * @returns A promise if a flush is in progress, otherwise undefined
   */
  public addRow(row: Record<string, unknown>, _lineNo?: number): Promise<void> | undefined
  {
    if (this.failed) return undefined;
    try {
      const line = this.line([...this.columns.map((c) => row[c])]);
      this.pending.push(line);
      this.pendingBytes += Buffer.byteLength(line, "utf8");
      this.rowCount++;
      if (this.pendingBytes >= CsvOutputWriter.FLUSH_THRESHOLD_BYTES) {
        return this.flushPending();
      }
      return undefined;
    } catch (err) {
      this.failed = true;
      this.logger.warn("csv_output_add_failed", { job_id: this.jobId, error: String(err) });
      return undefined;
    }
  }

    /**
   * Flushes pending bytes to the output stream.
   */
  public async flushPending(): Promise<void> {
    if (!this.pending.length || this.failed) return;
    const toWrite = this.pending.join("");
    this.pending = [];
    this.pendingBytes = 0;
    try {
      await new Promise<void>((resolve, reject) => {
        this.stream.write(toWrite, "utf8", (err) => {
          if (err) reject(err);
          else resolve();
        });
      });
    } catch (err) {
      this.failed = true;
      this.logger.warn("csv_output_flush_pending_failed", { job_id: this.jobId, error: String(err) });
    }
  }

    /**
   * Flushes the operation
   * @returns A promise that resolves to the result
   */
  async flush(): Promise<string | null> {
    if (this.rowCount === 0 || this.failed) {
      this.stream.destroy();
      await this.cleanup();
      return null;
    }
    try {
      await this.flushPending();
      this.stream.end();
      await finished(this.stream);
      const key = `output/${this.jobId}.csv`;
      await this.gcsUtils.putFile(this.config.settings.DATA_BUCKET, key, this.tmpPath, "text/csv");
      const gsPath = `gs://${this.config.settings.DATA_BUCKET}/${key}`;
      this.logger.info("csv_output_written", { job_id: this.jobId, rows: this.rowCount, path: gsPath });
      return gsPath;
    } catch (err) {
      this.logger.warn("csv_output_flush_failed", { job_id: this.jobId, error: String(err) });
      return null;
    } finally {
      await this.cleanup();
    }
  }

    /**
   * Performs the cleanup operation.
   */
  private async cleanup(): Promise<void> {
    this.pending = [];
    this.pendingBytes = 0;
    await fs.promises.unlink(this.tmpPath).catch(() => {});
  }
}
