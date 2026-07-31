import pino from "pino";
import Config from "@config/system-config/Config.js";
import ServiceManager from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import {FirestoreCacheUtils} from "@utils/cache/FirestoreCacheUtils.js";
import PostgreSqlManager from "@config/db/PostgreSqlManager.js";
import { EventType, makeJobEvent } from "@shared/models/events.js";
import { LoadMessage } from "@shared/models/job.js";
import {receiveMessages, deleteMessage, publishEvent, QueueMessage} from "@shared/QueueService.js";
import { ParquetReader } from "@dsnp/parquetjs";
import { createLogger } from "@utils/logger/Log.js";
import { LoadService } from "@service/load/LoadService.js";
import { LoadResponse } from "@service/load/io/ILoad.js";
import HealthService from "@utils/response/Health";
import {MetricsUtils} from "@utils/response/Metrics";

/**
 * LoadServiceImpl is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class LoadServiceImpl extends ServiceManager implements LoadService
{
    /**
   * Singleton instance
   * @private
   */

  protected static instance: LoadServiceImpl;

    /**
   * Logger instance
   * @private
   */

  private logger: pino.Logger;

    /**
   * Gcs Utils
   * @private
   */

  private gcsUtils: FirestoreCacheUtils;

    /**
   * Db Manager
   * @private
   */

  private dbManager: PostgreSqlManager;

    /**
   * S Y S T E M_ C O L S
   * @private
   */

  private SYSTEM_COLS = [
    "_job_id", "_byte_offset", "_byte_length", "_record_index",
    "_line_no", "_template_id", "_template_version", "_checksum",
    "_parsed_at", "_part_id",
  ] as const;

    /**
   * params per row
   * @private
   */

  private PARAMS_PER_ROW: number;

    /**
   * upsert batch
   * @private
   */

  private UPSERT_BATCH: number;

    /**
   * Constructs a new LoadServiceImpl instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */

  protected constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE,"Cannot instantiate LoadServiceImpl directly. Use getInstance()");
    }
    super(enforce);

    this.PARAMS_PER_ROW = this.SYSTEM_COLS.length + 1;
    this.UPSERT_BATCH = Math.floor(60000 / this.PARAMS_PER_ROW);

    this.logger = createLogger(module);
    this.gcsUtils = FirestoreCacheUtils.getInstance();
    this.dbManager = PostgreSqlManager.getInstance();

    if (process.env.HEALTH_CHECK_PORT)
    {
      HealthService.startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
    }
  }

    /**
   * Gets the single instance of the LoadServiceImpl class.
   * @returns The single instance of the class
   */

  public static getInstance(): LoadServiceImpl
  {
    if (!LoadServiceImpl.instance)
    {
      LoadServiceImpl.instance = new LoadServiceImpl(Enforce);
    }

    return LoadServiceImpl.instance;
  }

    /**
   * Gets logger
   * @returns The logger result
   */

  public getLogger(): pino.Logger
  {
    return this.logger;
  }

    /**
   * Processes load
   * @returns A promise that resolves to the result
   */

  public async processLoad(): Promise<LoadResponse>
  {
    return { success: true };
  }

    /**
   * Emits the operation
   * @param jobId - The job identifier
   * @param eventType - The event type
   * @param data - The data to process
   */

  private emit(jobId: string, eventType: EventType, data: Record<string, unknown>)
  {
    publishEvent(makeJobEvent(eventType, jobId, "load", data));
  }

    /**
   * Loads job
   * @param msg - The msg
   */

  public async loadJob(msg: LoadMessage): Promise<void>
    {
    const jobId: string = msg.job_id;

    if (msg.recovered_row)
    {
      this.logger.info("load_recovered_row", { job_id: jobId, byte_offset: msg.byte_offset });
      MetricsUtils.increment("load.recovered_row", 1);
      const row: Record<string, unknown> = this.buildRecoveredRow(msg);
      await this.upsertRows(jobId, [row]);
      this.emit(jobId, EventType.LOADING_COMPLETED, { total_rows: 1 });
      return;
    }

    this.logger.info("load_start", { job_id: jobId, parts: (msg.output_paths || []).length });
    MetricsUtils.increment("load.start", 1, { parts: String((msg.output_paths || []).length) });

    let totalRows: number = 0;

    try
    {
      for (const s3Path of msg.output_paths || [])
      {
        const rows: Record<string, unknown>[] = await this.readParquet(s3Path);

        if (!rows.length)
        {
          continue;
        }

        await this.upsertRows(jobId, rows);
        totalRows += rows.length;
      }

      this.logger.info("load_complete", { job_id: jobId, total_rows: totalRows });
      MetricsUtils.set("load.rows_loaded", totalRows);
      this.emit(jobId, EventType.LOADING_COMPLETED, { total_rows: totalRows });
    }
    catch (exc)
    {
      this.logger.error("load_failed", { job_id: jobId }, exc instanceof Error ? exc : new Error(String(exc)));
      MetricsUtils.increment("load.error", 1);
      this.emit(jobId, EventType.ERROR_OCCURRED, { error: String(exc) });
    }
  }

    /**
   * Builds recovered row
   * @param msg - The msg
   * @returns The record<string, unknown> result
   */

  private buildRecoveredRow(msg: LoadMessage): Record<string, unknown>
    {
    const now: string = new Date().toISOString();

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

    /**
   * Reads parquet
   * @param s3Path - The s3 path
   * @returns A promise that resolves to the list
   */

  private async readParquet(s3Path: string): Promise<Record<string, unknown>[]>
  {
    const [bucket, key] = this.gcsUtils.parseGcsUrl(s3Path);
    const raw: Buffer = await this.gcsUtils.readFull(bucket, key);
    const reader: ParquetReader = await ParquetReader.openBuffer(raw);
    const cursor = reader.getCursor();
    const rows: Record<string, unknown>[] = [];
    let row: unknown;

    while ((row = await cursor.next()))
    {
      rows.push(row as Record<string, unknown>);
    }

    await reader.close();

    return rows;
  }

    /**
   * Performs the upsert rows operation.
   * @param _jobId - The _job id
   * @param rows - The rows
   */

  private async upsertRows(_jobId: string, rows: Record<string, unknown>[]): Promise<void>
  {
    if (!rows.length)
    {
      return;
    }

    for (let i = 0; i < rows.length; i += this.UPSERT_BATCH)
    {
      const batch: Record<string, unknown>[] = rows.slice(i, i + this.UPSERT_BATCH);
      const records = batch.map((row) =>
      {
        const fields: Record<string, unknown> = {};

        for (const [k, v] of Object.entries(row))
        {
          if (!k.startsWith("_"))
          {
            fields[k] = v;
          }
        }

        return {
          _job_id: row._job_id as string,
          _byte_offset: row._byte_offset as number,
          _byte_length: row._byte_length as number,
          _record_index: row._record_index as number,
          _line_no: row._line_no as number,
          _template_id: row._template_id as string,
          _template_version: row._template_version as number,
          _checksum: row._checksum as string,
          _parsed_at: typeof row._parsed_at === "number" ? new Date(row._parsed_at) : (row._parsed_at as Date),
          _part_id: row._part_id as string,
          fields,
        };
      });

      await this.dbManager.repositories.parsedRecords.bulkCreate(records);
      this.logger.debug("upsert_batch", { rows: batch.length, offset: i });
    }
  }

    /**
   * Performs the consumer loop operation.
   */

  public async consumerLoop(): Promise<void>
  {
    await this.dbManager.initialize();
    this.logger.info("load_consumer_started");
    const config: Config = this.getConfig();

    while (true)
    {
      const messages: QueueMessage<LoadMessage>[] = await receiveMessages<LoadMessage>(config.settings.LOAD_QUEUE_URL, (body) => JSON.parse(body) as LoadMessage, 1);

      for (const { payload, receiptHandle } of messages)
      {
        try
        {
          await this.loadJob(payload);
          await deleteMessage(config.settings.LOAD_QUEUE_URL, receiptHandle);
        }
        catch (exc)
        {
          const errorStr: string = String(exc);

          if (errorStr.includes("Job") && (errorStr.includes("not found") || errorStr.includes("cannot transition")))
          {
            this.logger.error("load_message_failed_ack", { job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" });
            MetricsUtils.increment("load.message_error_ack", 1);
            await deleteMessage(config.settings.LOAD_QUEUE_URL, receiptHandle);
          }
          else
          {
            this.logger.error("load_message_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
            MetricsUtils.increment("load.message_error", 1);
          }
        }
      }
    }
  }
}

export default LoadServiceImpl;

function Enforce(): void {}
