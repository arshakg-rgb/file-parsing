import pino from "pino";
import { randomUUID } from "node:crypto";
import Config from "@config/system-config/Config.js";
import { settings } from "@shared/Settings.js";
import ServiceManager from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { DatabaseManager } from "@shared/DatabaseManager.js";
import { toDate } from "@config/db/BigQueryManager.js";
import { EventType, makeJobEvent } from "@shared/models/events.js";
import { LoadMessage } from "@shared/models/job.js";
import { createLogger } from "@utils/logger/Log.js";
import { LoadService } from "@service/load/LoadService.js";
import { LoadResponse } from "@service/load/io/ILoad.js";
import HealthService from "@utils/response/Health";
import { MetricsUtils } from "@utils/response/Metrics";
import { QueueService } from "@shared/QueueService";
import { QueueConsumerPool } from "@shared/QueueConsumerPool.js";
import { ParquetEngine } from "@service/job-service/finalize/ParquetEngine.js";
import { StoragePath } from "@service/job-service/finalize/StoragePath.js";
import { GcsUtils } from "@shared/GcsUtils";

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
   * Db Manager
   * @private
   */

  private dbManager: DatabaseManager;

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

  private queueService: QueueService;

    /**
   * Constructs a new LoadServiceImpl instance.
   * @param enforce - A function to enforce the Singleton pattern
     * @param queueService
   * @throws Error if instantiated directly
   */

  protected constructor(enforce: () => void, queueService: QueueService)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE,"Cannot instantiate LoadServiceImpl directly. Use getInstance()");
    }
    super(enforce);

    this.PARAMS_PER_ROW = this.SYSTEM_COLS.length + 1;
    this.UPSERT_BATCH = Math.floor(60000 / this.PARAMS_PER_ROW);

    this.logger = createLogger(module);
    this.dbManager = DatabaseManager.getInstance();
    this.queueService = queueService;

    if (process.env.HEALTH_CHECK_PORT && process.env.QUEUE_PUSH_MODE !== "true")
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
      LoadServiceImpl.instance = new LoadServiceImpl(Enforce, QueueService.getInstance());
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

  private async emit(jobId: string, eventType: EventType, data: Record<string, unknown>): Promise<void>
  {
    await this.queueService.publishEvent(makeJobEvent(eventType, jobId, "load", data));
  }

    /**
   * Loads job
   * @param msg - The msg
   */

  private deriveBatchSize(sizeBytes: number | bigint | null | undefined): number
  {
    const size = Number(sizeBytes ?? 0);
    const sizeMb = size / (1024 * 1024);
    const base = Math.max(100, Math.floor(60000 / this.PARAMS_PER_ROW));

    if (sizeMb <= 10)
    {
      return base;
    }
    if (sizeMb <= 100)
    {
      return Math.max(100, Math.floor(base * 0.75));
    }
    return Math.max(100, Math.floor(base * 0.5));
  }

  public async loadJob(msg: LoadMessage): Promise<void>
  {
    const jobId: string = msg.job_id;

    if (msg.recovered_row)
    {
      this.logger.info({ job_id: jobId, byte_offset: msg.byte_offset }, "load_recovered_row");
      MetricsUtils.increment("load.recovered_row", 1);
      const row: Record<string, unknown> = this.buildRecoveredRow(msg);
      const totalRows = await this.loadRecoveredRowFromGcs(jobId, row);
      await this.emit(jobId, EventType.LOADING_COMPLETED, { total_rows: totalRows });
      return;
    }

    const parseJob = await this.dbManager.repositories.jobs.findById(jobId);
    this.UPSERT_BATCH = this.deriveBatchSize(parseJob?.size ?? undefined);

    this.logger.info({ job_id: jobId, parts: (msg.output_paths || []).length, size: Number(parseJob?.size ?? 0), batch: this.UPSERT_BATCH }, "load_start");
    MetricsUtils.increment("load.start", 1, { parts: String((msg.output_paths || []).length) });

    let totalRows: number = 0;

    try
    {
      const outputPaths = msg.output_paths || [];

      if (outputPaths.length > 0)
      {
        totalRows = await this.dbManager.repositories.parsedRecords.bulkLoadFromGcs(outputPaths);
        this.logger.info({ job_id: jobId, paths: outputPaths.length, rows: totalRows }, "load_complete");
      }
      else
      {
        this.logger.info({ job_id: jobId }, "load_no_output_paths");
      }
      await this.emit(jobId, EventType.LOADING_COMPLETED, { total_rows: totalRows });
    }
    catch (exc)
    {
      this.logger.error({ job_id: jobId, error: exc instanceof Error ? exc.message : String(exc) }, "load_failed");
      MetricsUtils.increment("load.error", 1);
      await this.emit(jobId, EventType.ERROR_OCCURRED, { error: String(exc) });
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
   * Performs the upsert rows operation.
   * @param _jobId - The _job id
   * @param rows - The rows
   */

  private async loadRecoveredRowFromGcs(jobId: string, row: Record<string, unknown>): Promise<number>
  {
    const userFields: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(row))
    {
      if (!k.startsWith("_"))
      {
        userFields[k] = v;
      }
    }

    const record = {
      id: Date.now(),
      _job_id: row._job_id as string,
      _byte_offset: row._byte_offset as number,
      _byte_length: row._byte_length as number,
      _record_index: row._record_index as number,
      _line_no: row._line_no as number,
      _template_id: row._template_id as string,
      _template_version: row._template_version as number,
      _checksum: row._checksum as string,
      _parsed_at: toDate(row._parsed_at),
      _part_id: row._part_id as string,
      fields: JSON.stringify(userFields),
    };

    const bucket: string = settings.DATA_BUCKET;
    const key: string = `outputs/${jobId}/recovered/${randomUUID()}.parquet`;
    const path = new StoragePath("gs", bucket, key);

    await ParquetEngine.writeRows(path, [record]);
    this.logger.info({ job_id: jobId, path: path.toString() }, "load_recovered_row_written");

    try {
      const loaded = await this.dbManager.repositories.parsedRecords.bulkLoadFromGcs([path.toString()]);
      this.logger.info({ job_id: jobId, rows: loaded }, "load_recovered_row_loaded");
      return loaded;
    }
    finally
    {
      await GcsUtils.getInstance().deleteObject(bucket, key).catch((err) =>
      {
        this.logger.warn({ job_id: jobId, path: path.toString(), error: String(err) }, "load_recovered_row_cleanup_failed");
      });
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

    const pool = new QueueConsumerPool<LoadMessage>(this.queueService, this.logger, {
      queueUrl: config.settings.LOAD_QUEUE_URL,
      parser: (body) => JSON.parse(body) as LoadMessage,
      concurrency: 3,
      memorySoftLimit: settings.QUEUE_MEMORY_SOFT_LIMIT_MB * 1024 * 1024,
    });

    const queueUrl = config.settings.LOAD_QUEUE_URL;

    await pool.run(async (payload, receiptHandle) => {
      const heartbeat = setInterval(() => {
        void this.queueService.modifyAckDeadline(queueUrl, receiptHandle, 600)
          .catch((err) => this.logger.warn("load_ack_heartbeat_failed", { job_id: payload.job_id, error: String(err) }));
      }, 60_000);

      try
      {
        await this.queueService.modifyAckDeadline(queueUrl, receiptHandle, 600);
        await this.loadJob(payload);
        await this.queueService.deleteMessage(queueUrl, receiptHandle);
      }
      catch (exc)
      {
        const errorStr: string = String(exc);

        if (errorStr.includes("Job") && (errorStr.includes("not found") || errorStr.includes("cannot transition")))
        {
          this.logger.error({ job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" }, "load_message_failed_ack");
          MetricsUtils.increment("load.message_error_ack", 1);
          await this.queueService.deleteMessage(queueUrl, receiptHandle);
        }
        else
        {
          this.logger.error({ job_id: payload.job_id, error: exc instanceof Error ? exc.message : String(exc) }, "load_message_failed");
          MetricsUtils.increment("load.message_error", 1);
        }
      }
      finally
      {
        clearInterval(heartbeat);
      }
    });
  }
}

export default LoadServiceImpl;

function Enforce(): void {}
