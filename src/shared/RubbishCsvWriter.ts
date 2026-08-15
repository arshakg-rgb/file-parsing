import fs from "fs";
import os from "os";
import path from "path";
import pino from "pino";
import { finished } from "node:stream/promises";
import type { WriteStream } from "node:fs";
import { GcsUtils } from "./GcsUtils.js";
import { createLogger } from "@utils/logger/Log.js";
import Config from "@config/system-config/Config.js";
import { csvEscapeCell } from "./CsvOutputWriter.js";

/**
 * Writes dropped (rubbish) and uncertain (DLQ) rows to a CSV for manual inspection.
 */
export class RubbishCsvWriter
{
  private readonly tmpPath: string;
  private readonly columns: string[];
  private readonly logger: pino.Logger;
  private readonly stream: WriteStream;

  private rowCount = 0;
  private failed = false;
  private pending: string[] = [];
  private pendingBytes = 0;
  private static readonly FLUSH_THRESHOLD_BYTES = 8 * 1024 * 1024;

  constructor(private readonly jobId: string)
  {
    this.tmpPath = path.join(os.tmpdir(), `${jobId}-rubbish.csv`);
    this.columns = [
      "line_no",
      "byte_offset",
      "byte_length",
      "raw_bytes",
      "source",
      "failure_class",
      "error",
      "matched_template_id",
      "dlq_id",
    ];
    this.logger = createLogger(module);
    this.stream = fs.createWriteStream(this.tmpPath, { encoding: "utf8" });
    this.stream.write("\ufeff" + this.line([...this.columns]), "utf8");
  }

  private line(vals: unknown[]): string
  {
    return vals.map((v) => csvEscapeCell(v)).join(",") + "\r\n";
  }

  public addRow(row: Record<string, unknown>): Promise<void> | undefined
  {
    if (this.failed) return undefined;

    try
    {
      const line = this.line([...this.columns.map((c) => row[c])]);
      this.pending.push(line);
      this.pendingBytes += Buffer.byteLength(line, "utf8");
      this.rowCount++;

      if (this.pendingBytes >= RubbishCsvWriter.FLUSH_THRESHOLD_BYTES)
      {
        return this.flushPending();
      }

      return undefined;
    }
    catch (err)
    {
      this.failed = true;
      this.logger.warn("rubbish_csv_add_failed", { job_id: this.jobId, error: String(err) });
      return undefined;
    }
  }

  public async flushPending(): Promise<void>
  {
    if (!this.pending.length || this.failed) return;

    const toWrite = this.pending.join("");
    this.pending = [];
    this.pendingBytes = 0;

    try
    {
      await new Promise<void>((resolve, reject) =>
      {
        this.stream.write(toWrite, "utf8", (err) =>
        {
          if (err) reject(err);
          else resolve();
        });
      });
    }
    catch (err)
    {
      this.failed = true;
      this.logger.warn("rubbish_csv_flush_pending_failed", { job_id: this.jobId, error: String(err) });
    }
  }

  public async flush(): Promise<string | null>
  {
    if (this.rowCount === 0 || this.failed)
    {
      this.stream.destroy();
      await this.cleanup();
      return null;
    }

    try
    {
      await this.flushPending();
      this.stream.end();
      await finished(this.stream);

      const bucket = Config.getInstance().settings.DATA_BUCKET;
      const key = `output/rubbish_${this.jobId}.csv`;
      await GcsUtils.getInstance().putObjectFromFile(bucket, key, this.tmpPath, "text/csv");

      const gsPath = `gs://${bucket}/${key}`;
      this.logger.info("rubbish_csv_written", { job_id: this.jobId, rows: this.rowCount, path: gsPath });
      return gsPath;
    }
    catch (err)
    {
      this.logger.warn("rubbish_csv_flush_failed", { job_id: this.jobId, error: String(err) });
      return null;
    }
    finally
    {
      await this.cleanup();
    }
  }

  private async cleanup(): Promise<void>
  {
    this.pending = [];
    this.pendingBytes = 0;
    await fs.promises.unlink(this.tmpPath).catch(() => {});
  }
}
