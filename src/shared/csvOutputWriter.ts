import os from "os";
import path from "path";
import fs from "fs";
import { settings } from "./config.js";
import { putObject } from "./gcsUtils.js";
import { createLogger } from "./logger.js";

const logger = createLogger("csv-output");

/** RFC-4180 CSV cell escaping: quote cells containing comma/quote/newline, double inner quotes. */
export function csvEscapeCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = String(v);
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

/**
 * Writes parsed rows (the client's field_spec columns) to a single per-job CSV at
 * gs://DATA_BUCKET/output/<jobId>.csv, so the output is human-readable alongside the
 * Parquet parts (which feed the DB load). Streams to a temp file during parsing to keep
 * memory bounded, then uploads on flush. Best-effort: callers should not fail a job if the
 * CSV write fails — Parquet remains the authoritative output.
 */
export class CsvOutputWriter {
  private readonly tmpPath: string;
  private stream: fs.WriteStream | null = null;
  private rowCount = 0;
  private failed = false;
  private readonly columns: string[];

  constructor(private readonly jobId: string, fieldSpec: string[]) {
    this.columns = fieldSpec && fieldSpec.length > 0 ? fieldSpec : ["value"];
    this.tmpPath = path.join(os.tmpdir(), `${jobId}-output.csv`);
  }

  private line(vals: unknown[]): string {
    return vals.map((v) => csvEscapeCell(v)).join(",") + "\n";
  }

  addRow(row: Record<string, any>, lineNo?: number): void {
    if (this.failed) return;
    try {
      if (!this.stream) {
        this.stream = fs.createWriteStream(this.tmpPath, { encoding: "utf-8" });
        this.stream.on("error", (err) => {
          this.failed = true;
          logger.warn("csv_output_stream_error", { job_id: this.jobId, error: String(err) });
        });
        this.stream.write(this.line([...this.columns, "line_no"]));
      }
      this.stream.write(this.line([...this.columns.map((c) => row[c]), lineNo ?? ""]));
      this.rowCount++;
    } catch (err) {
      this.failed = true;
      logger.warn("csv_output_add_failed", { job_id: this.jobId, error: String(err) });
    }
  }

  /** Uploads the CSV and returns its gs:// path, or null if there was nothing to write. */
  async flush(): Promise<string | null> {
    if (!this.stream || this.rowCount === 0 || this.failed) {
      await this.cleanup();
      return null;
    }
    try {
      await new Promise<void>((resolve, reject) => {
        this.stream!.once("error", reject);
        this.stream!.end(() => resolve());
      });
      const key = `output/${this.jobId}.csv`;
      const body = await fs.promises.readFile(this.tmpPath);
      await putObject(settings.DATA_BUCKET, key, body, "text/csv");
      const gsPath = `gs://${settings.DATA_BUCKET}/${key}`;
      logger.info("csv_output_written", { job_id: this.jobId, rows: this.rowCount, path: gsPath });
      return gsPath;
    } catch (err) {
      logger.warn("csv_output_flush_failed", { job_id: this.jobId, error: String(err) });
      return null;
    } finally {
      await this.cleanup();
    }
  }

  private async cleanup(): Promise<void> {
    if (this.stream && !this.stream.destroyed) this.stream.destroy();
    this.stream = null;
    await fs.promises.unlink(this.tmpPath).catch(() => {});
  }
}
