import Config from "../../config/system-config/Config.js";
import ServiceManager from "../../config/ServiceManager.js";
import { InstantiationError } from "../../errors/InstantiationError.js";
import FirestoreCacheUtils from "../../utils/cache/FirestoreCacheUtils.js";
import MySqlManager from "../../config/db/MySqlManager.js";
import { EventType, JobEvent, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus, LoadMessage } from "../../shared/models/job.js";
import { receiveMessages, deleteMessage, publishEvent } from "../../shared/queueUtils.js";
import { ParquetReader } from "@dsnp/parquetjs";
import { createLogger } from "../../shared/logger.js";
import { metrics } from "../../shared/metrics.js";
import { startHealthCheckServer } from "../../shared/health.js";

class LoadService extends ServiceManager {
  protected static instance: LoadService;
  private logger: any;
  private gcsUtils: FirestoreCacheUtils;
  private dbManager: MySqlManager;
  private SYSTEM_COLS = [
    "_job_id", "_byte_offset", "_byte_length", "_record_index",
    "_line_no", "_template_id", "_template_version", "_checksum",
    "_parsed_at", "_part_id",
  ] as const;
  private PARAMS_PER_ROW: number;
  private UPSERT_BATCH: number;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate LoadService directly. Use getInstance()");
    }
    super(enforce);
    
    this.PARAMS_PER_ROW = this.SYSTEM_COLS.length + 1;
    this.UPSERT_BATCH = Math.floor(60000 / this.PARAMS_PER_ROW);
    
    this.logger = createLogger("load");
    this.gcsUtils = FirestoreCacheUtils.getInstance();
    this.dbManager = MySqlManager.getInstance();
    
    if (process.env.HEALTH_CHECK_PORT) {
      startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
    }
  }

  public static getInstance(): LoadService {
    if (!ServiceManager.instance) {
      ServiceManager.instance = new LoadService(Enforce);
    }
    return ServiceManager.instance as LoadService;
  }

  public getLogger(): any {
    return this.logger;
  }

  public getGcsUtils(): FirestoreCacheUtils {
    return this.gcsUtils;
  }

  public getDbManager(): MySqlManager {
    return this.dbManager;
  }

  private emit(jobId: string, eventType: EventType, data: Record<string, any>) {
    publishEvent(makeJobEvent(eventType, jobId, "load", data));
  }

  public async loadJob(msg: LoadMessage): Promise<void> {
    const jobId = msg.job_id;

    if (msg.recovered_row) {
      this.logger.info("load_recovered_row", { job_id: jobId, byte_offset: msg.byte_offset });
      metrics.increment("load.recovered_row", 1);
      const row = this.buildRecoveredRow(msg);
      await this.upsertRows(jobId, [row]);
      this.emit(jobId, EventType.LOADING_COMPLETED, { total_rows: 1 });
      return;
    }

    this.logger.info("load_start", { job_id: jobId, parts: (msg.output_paths || []).length });
    metrics.increment("load.start", 1, { parts: String((msg.output_paths || []).length) });

    let totalRows = 0;
    try {
      for (const s3Path of msg.output_paths || []) {
        const rows = await this.readParquet(s3Path);
        if (!rows.length) continue;
        await this.upsertRows(jobId, rows);
        totalRows += rows.length;
      }

      this.logger.info("load_complete", { job_id: jobId, total_rows: totalRows });
      metrics.set("load.rows_loaded", totalRows);
      this.emit(jobId, EventType.LOADING_COMPLETED, { total_rows: totalRows });
    } catch (exc) {
      this.logger.error("load_failed", { job_id: jobId }, exc instanceof Error ? exc : new Error(String(exc)));
      metrics.increment("load.error", 1);
      this.emit(jobId, EventType.ERROR_OCCURRED, { error: String(exc) });
    }
  }

  private buildRecoveredRow(msg: LoadMessage): Record<string, any> {
    const now = new Date().toISOString();
    return {
      ...msg.recovered_row,
      _job_id: msg.job_id,
      _byte_offset: msg.byte_offset ?? 0,
      _byte_length: msg.byte_length ?? 0,
      _record_index: 0,
      _line_no: msg.line_no ?? 0,
      _template_id: msg.template_id ?? "unknown",
      _template_version: msg.template_version ?? 1,
      _checksum: "",
      _parsed_at: now,
      _part_id: "recovered",
    };
  }

  private async readParquet(s3Path: string): Promise<Record<string, any>[]> {
    const [bucket, key] = this.gcsUtils.parseGcsUrl(s3Path);
    const raw = await this.gcsUtils.readFull(bucket, key);
    const reader = await ParquetReader.openBuffer(raw);
    const cursor = reader.getCursor();
    const rows: Record<string, any>[] = [];
    let row: any;
    while ((row = await cursor.next())) {
      rows.push(row);
    }
    await reader.close();
    return rows;
  }

  private async upsertRows(_jobId: string, rows: Record<string, any>[]): Promise<void> {
    if (!rows.length) return;

    for (let i = 0; i < rows.length; i += this.UPSERT_BATCH) {
      const batch = rows.slice(i, i + this.UPSERT_BATCH);
      const placeholders: string[] = [];
      const values: any[] = [];
      let idx = 1;

      for (const row of batch) {
        const rowPh: string[] = [];
        for (const col of this.SYSTEM_COLS) {
          rowPh.push(`$${idx++}`);
          let v = row[col] ?? null;
          if (col === "_parsed_at" && typeof v === "number") v = new Date(v);
          values.push(v);
        }
        const fields: Record<string, any> = {};
        for (const [k, v] of Object.entries(row)) {
          if (!k.startsWith("_")) fields[k] = v;
        }
        rowPh.push(`$${idx++}`);
        values.push(JSON.stringify(fields));
        placeholders.push(`(${rowPh.join(", ")})`);
      }

      const colList = [...this.SYSTEM_COLS, "fields"].map((c) => `"${c}"`).join(", ");
      const sql = `INSERT INTO parsed_records (${colList}) VALUES ${placeholders.join(", ")} ON CONFLICT ("_job_id", "_byte_offset") DO NOTHING`;
      await this.dbManager.pool.query(sql, values);
      this.logger.debug("upsert_batch", { rows: batch.length, offset: i });
    }
  }

  public async consumerLoop(): Promise<void> {
    await this.dbManager.initialize();
    this.logger.info("load_consumer_started");
    const config = this.getConfig();
    while (true) {
      const messages = await receiveMessages<LoadMessage>(
        config.settings.LOAD_QUEUE_URL,
        (body) => JSON.parse(body) as LoadMessage,
        1
      );
      for (const { payload, receiptHandle } of messages) {
        try {
          await this.loadJob(payload);
          await deleteMessage(config.settings.LOAD_QUEUE_URL, receiptHandle);
        } catch (exc) {
          const errorStr = String(exc);
          if (errorStr.includes("Job") && (errorStr.includes("not found") || errorStr.includes("cannot transition"))) {
            this.logger.error("load_message_failed_ack", { job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" });
            metrics.increment("load.message_error_ack", 1);
            await deleteMessage(config.settings.LOAD_QUEUE_URL, receiptHandle);
          } else {
            this.logger.error("load_message_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
            metrics.increment("load.message_error", 1);
          }
        }
      }
    }
  }
}

function Enforce(): void {}

export default LoadService;

// Backward compatibility wrappers
const loadService = LoadService.getInstance();

export async function loadJob(msg: LoadMessage): Promise<void> {
  return loadService.loadJob(msg);
}

export async function consumerLoop(): Promise<void> {
  return loadService.consumerLoop();
}
