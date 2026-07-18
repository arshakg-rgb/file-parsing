import os from "os";
import path from "path";
import fs from "fs";
import Config from "../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import FirestoreCacheUtils from "../utils/cache/FirestoreCacheUtils.js";
import { createLogger } from "../utils/logger/logger.js";

class CsvOutputService extends ServiceManager {
  protected static instance: CsvOutputService;
  private logger: any;
  private gcsUtils: FirestoreCacheUtils;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate CsvOutputService directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("csv-output");
    this.gcsUtils = FirestoreCacheUtils.getInstance();
  }

  public static getInstance(): CsvOutputService {
    if (!CsvOutputService.instance) {
      CsvOutputService.instance = new CsvOutputService(Enforce);
    }
    return CsvOutputService.instance;
  }

  public static escapeCell(v: unknown): string {
    if (v === null || v === undefined) return "";
    const s = String(v);
    return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  }
}


export default CsvOutputService;

const csvOutputService = CsvOutputService.getInstance();

export function csvEscapeCell(v: unknown): string {
  return CsvOutputService.escapeCell(v);
}

export class CsvOutputWriter {
  private readonly tmpPath: string;
  private stream: fs.WriteStream | null = null;
  private rowCount = 0;
  private failed = false;
  private readonly columns: string[];
  private readonly logger: any;
  private readonly gcsUtils: FirestoreCacheUtils;
  private readonly config: Config;

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

  addRow(row: Record<string, any>, _lineNo?: number): void {
    if (this.failed) return;
    try {
      if (!this.stream) {
        this.stream = fs.createWriteStream(this.tmpPath, { encoding: "utf-8" });
        this.stream.on("error", (err) => {
          this.failed = true;
          this.logger.warn("csv_output_stream_error", { job_id: this.jobId, error: String(err) });
        });
        this.stream.write("﻿" + this.line([...this.columns]));
      }
      this.stream.write(this.line([...this.columns.map((c) => row[c])]));
      this.rowCount++;
    } catch (err) {
      this.failed = true;
      this.logger.warn("csv_output_add_failed", { job_id: this.jobId, error: String(err) });
    }
  }

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

  private async cleanup(): Promise<void> {
    if (this.stream && !this.stream.destroyed) this.stream.destroy();
    this.stream = null;
    await fs.promises.unlink(this.tmpPath).catch(() => {});
  }
}
