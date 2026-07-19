import os from "os";
import path from "path";
import fs from "fs";
import Config from "../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import FirestoreCacheUtils from "../utils/cache/FirestoreCacheUtils.js";
import { createLogger, Logger } from "../utils/logger/logger.js";

class CsvOutputService extends ServiceManager {
  protected static instance: CsvOutputService;
  private logger: Logger;
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
    if (typeof v === "bigint") return String(v);
    let s: string;
    if (Array.isArray(v) || (typeof v === "object" && !(v instanceof Date))) {
      s = JSON.stringify(v);
    } else {
      s = String(v);
    }
    return /[",\r\n]/.test(s) ? "\"" + s.replace(/"/g, "\"\"") + "\"" : s;
  }
}


export default CsvOutputService;

const csvOutputService = CsvOutputService.getInstance();

export function csvEscapeCell(v: unknown): string {
  return CsvOutputService.escapeCell(v);
}

export class CsvOutputWriter {
  private readonly tmpPath: string;
  private readonly columns: string[];
  private readonly logger: Logger;
  private readonly gcsUtils: FirestoreCacheUtils;
  private readonly config: Config;

  private rowCount = 0;
  private failed = false;
  private headerWritten = false;
  private pending: string[] = [];
  private pendingBytes = 0;
  private static readonly FLUSH_THRESHOLD_BYTES = 8 * 1024 * 1024;

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

  private async cleanup(): Promise<void> {
    this.pending = [];
    this.pendingBytes = 0;
    await fs.promises.unlink(this.tmpPath).catch(() => {});
  }
}
