import os from "os";
import path from "path";
import fs from "fs";
import Config from "@config/system-config/Config.js";
import FirestoreCacheUtils from "@utils/cache/FirestoreCacheUtils.js";
import { createLogger, Logger } from "@utils/logger/logger.js";

/**
 * Performs the csv escape cell operation.
 * @param v - The v
 * @returns The string result
 */
export function csvEscapeCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "bigint") return String(v);
  let s: string;
  if (Array.isArray(v) || (typeof v === "object" && !(v instanceof Date))) {
    s = JSON.stringify(v);
  } else {
    s = String(v);
  }
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
  private readonly logger: Logger;
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
   * Header Written
   * @private
   */
  private headerWritten = false;
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
  private static readonly FLUSH_THRESHOLD_BYTES = 8 * 1024 * 1024;

    /**
   * Constructs a new CsvOutputWriter instance.
   * @param jobId - The job identifier
   * @param fieldSpec - The field spec
   */
  constructor(private readonly jobId: string, fieldSpec: string[]) {
    this.columns = fieldSpec && fieldSpec.length > 0 ? fieldSpec : ["value"];
    this.tmpPath = path.join(os.tmpdir(), `${jobId}-output.csv`);
    this.logger = createLogger("csv-output");
    this.gcsUtils = FirestoreCacheUtils.getInstance();
    this.config = Config.getInstance();
  }

  // CRLF line endings + a UTF-8 BOM (written once, before the header) so the file opens cleanly
  // in Excel — matching the delivered reference format. Columns are exactly the field_spec,
  // with no internal line_no column.
  private line(vals: unknown[]): string {
    return vals.map((v) => csvEscapeCell(v)).join(",") + "\r\n";
  }

    /**
   * Adds row
   * @param row - The row
   * @param _lineNo - The _line no
   */
  addRow(row: Record<string, unknown>, _lineNo?: number): void {
    if (this.failed) return;
    try {
      if (!this.headerWritten) {
        fs.appendFileSync(this.tmpPath, "\ufeff" + this.line([...this.columns]), "utf8");
        this.headerWritten = true;
      }
      const line = this.line([...this.columns.map((c) => row[c])]);
      this.pending.push(line);
      this.pendingBytes += Buffer.byteLength(line, "utf8");
      this.rowCount++;
      if (this.pendingBytes >= CsvOutputWriter.FLUSH_THRESHOLD_BYTES) {
        this.flushPending();
      }
    } catch (err) {
      this.failed = true;
      this.logger.warn("csv_output_add_failed", { job_id: this.jobId, error: String(err) });
    }
  }

    /**
   * Flushes pending
   */
  private flushPending(): void {
    if (!this.pending.length) return;
    try {
      fs.appendFileSync(this.tmpPath, this.pending.join(""), "utf8");
      this.pending = [];
      this.pendingBytes = 0;
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
      await this.cleanup();
      return null;
    }
    try {
      this.flushPending();
      const key = `output/${this.jobId}.csv`;
      const body = await fs.promises.readFile(this.tmpPath);
      await this.gcsUtils.putObject(this.config.settings.DATA_BUCKET, key, body, "text/csv");
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
