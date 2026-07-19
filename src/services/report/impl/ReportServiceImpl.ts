import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import FirestoreCacheUtils from "@utils/cache/FirestoreCacheUtils.js";
import MySqlManager from "@config/db/MySqlManager.js";
import { EventType, JobEvent, makeJobEvent } from "@shared/models/events.js";
import { JobStatus, ReportMessage, JobCounts, JobTimings } from "@shared/models/job.js";
import type { ParseJobAttributes } from "@config/db/models/ParseJob.js";
import type { OutputPartAttributes } from "@config/db/models/OutputPart.js";
import { receiveMessages, deleteMessage, publishEvent } from "@shared/QueueService.js";
import { QualityGate } from "@shared/QualityGate.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
import { metrics } from "@utils/response/metrics.js";
import { startHealthCheckServer } from "@utils/response/health.js";
import { ReportService } from "@service/report/ReportService.js";
import { IReport, ReportRequest, ReportResponse } from "@service/report/io/IReport.js";

/**
 * ReportServiceImpl is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class ReportServiceImpl extends ServiceManager implements ReportService {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: ReportServiceImpl;
    /**
   * Logger instance
   * @private
   */
  private logger: Logger;
    /**
   * Gcs Utils
   * @private
   */
  private gcsUtils: FirestoreCacheUtils;
    /**
   * Db Manager
   * @private
   */
  private dbManager: MySqlManager;

    /**
   * Constructs a new ReportServiceImpl instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate ReportServiceImpl directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("report");
    this.gcsUtils = FirestoreCacheUtils.getInstance();
    this.dbManager = MySqlManager.getInstance();
    
    if (process.env.HEALTH_CHECK_PORT) {
      startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
    }
  }

    /**
   * Gets the single instance of the ReportServiceImpl class.
   * @returns The single instance of the class
   */
  public static getInstance(): ReportServiceImpl {
    if (!ReportServiceImpl.instance) {
      ReportServiceImpl.instance = new ReportServiceImpl(Enforce);
    }
    return ReportServiceImpl.instance;
  }

    /**
   * Gets logger
   * @returns The logger result
   */
  public getLogger(): Logger {
    return this.logger;
  }

    /**
   * Gets gcs utils
   * @returns The firestore cache utils result
   */
  public getGcsUtils(): FirestoreCacheUtils {
    return this.gcsUtils;
  }

    /**
   * Gets db manager
   * @returns The my sql manager result
   */
  public getDbManager(): MySqlManager {
    return this.dbManager;
  }

    /**
   * Processes report
   * @param req - The HTTP request object
   * @returns A promise that resolves to the result
   */
  public async processReport(req: ReportRequest): Promise<ReportResponse> {
    // Placeholder implementation
    return { success: true };
  }

    /**
   * Emits the operation
   * @param jobId - The job identifier
   * @param eventType - The event type
   * @param data - The data to process
   */
  private emit(jobId: string, eventType: EventType, data: Record<string, unknown>) {
    publishEvent(makeJobEvent(eventType, jobId, "report", data));
  }

    /**
   * Performs the total failed operation.
   * @param counts - The counts
   * @returns The numeric result
   */
  private totalFailed(counts: JobCounts): number {
    return Object.values(counts.failed_by_class || {}).reduce((a, b) => a + b, 0);
  }

    /**
   * Performs the generate report operation.
   * @param msg - The msg
   */
  public async generateReport(msg: ReportMessage): Promise<void> {
    const jobId = msg.job_id;
    this.logger.info("report_start", { job_id: jobId, status: msg.status });
    metrics.increment("report.start", 1, { status: msg.status });

    const jobRow = await this.getJob(jobId);
    if (!jobRow) {
      throw new Error(`Job ${jobId} not found`);
    }
    const parts = await this.getParts(jobId);
    const batchSiblings = jobRow.batch_id ? await this.getBatchJobs(jobRow.batch_id) : [];

    const qualityGate = QualityGate.getInstance();
    const qualityMetrics = await qualityGate.calculateMetrics(jobId);
    const qualityCheck = await qualityGate.passesQualityGate(jobId);

    const report = {
      job_id: jobId,
      batch_id: jobRow.batch_id,
      generated_at: new Date().toISOString(),
      status: msg.status,
      source: {
        type: jobRow.source_type,
        ref: jobRow.source_ref,
        s3_url: jobRow.s3_url,
        size_bytes: jobRow.size,
      },
      field_spec: jobRow.field_spec,
      counts: {
        parsed: msg.counts.parsed,
        dropped_rubbish: msg.counts.dropped_rubbish,
        failed_total: this.totalFailed(msg.counts),
        failed_by_class: msg.counts.failed_by_class,
      },
      quality: {
        total_lines: qualityMetrics.totalLines,
        parsed_lines: qualityMetrics.parsedLines,
        dropped_rubbish_lines: qualityMetrics.droppedRubbishLines,
        failed_lines: qualityMetrics.failedLines,
        failed_line_ratio: qualityMetrics.failedLineRatio,
        passed_quality_gate: qualityCheck.passes,
        quality_gate_reason: qualityCheck.reason,
      },
      output_parts: parts.map((p) => ({ s3_path: p.s3_path, rows: p.row_count, bytes: p.byte_size })),
      output_paths: msg.output_paths,
      csv_output_path: msg.csv_output_path ?? (jobRow.timings as JobTimings)?._csv_output_path ?? null,
      rubbish_log_path: msg.rubbish_log_path,
      dlq_count: msg.dlq_count,
      timings: jobRow.timings,
    };

    const config = this.getConfig();
    const reportKey = `reports/${jobId}/report.json`;
    await this.gcsUtils.putJson(config.settings.DATA_BUCKET, reportKey, report);
    this.logger.info("report_written", { job_id: jobId, s3_key: reportKey, quality_passed: qualityCheck.passes });
    metrics.increment("report.generated", 1);

    if (batchSiblings.length && jobRow.batch_id) {
      const allTerminal = batchSiblings.every((j) =>
        [JobStatus.DONE, JobStatus.PARTIAL, JobStatus.HELD, JobStatus.FAILED].includes(j.status as JobStatus)
      );
      if (allTerminal) {
        await this.writeBatchRollup(jobRow.batch_id, batchSiblings);
      }
    }

    this.emit(jobId, EventType.REPORTING_COMPLETED, { counts: msg.counts });
  }

    /**
   * Gets job
   * @param jobId - The job identifier
   * @returns A promise that resolves to the result
   */
  private async getJob(jobId: string): Promise<ParseJobAttributes | null> {
    return this.dbManager.repositories.jobs.findById(jobId);
  }

    /**
   * Gets parts
   * @param jobId - The job identifier
   * @returns A promise that resolves to the list
   */
  private async getParts(jobId: string): Promise<OutputPartAttributes[]> {
    return this.dbManager.repositories.outputParts.findByJob(jobId);
  }

    /**
   * Gets batch jobs
   * @param batchId - The batch identifier
   * @returns A promise that resolves to the list
   */
  private async getBatchJobs(batchId: string): Promise<ParseJobAttributes[]> {
    return this.dbManager.repositories.jobs.findByBatchId(batchId);
  }

    /**
   * Writes batch rollup
   * @param batchId - The batch identifier
   * @param jobs - The jobs
   */
  private async writeBatchRollup(batchId: string, jobs: ParseJobAttributes[]): Promise<void> {
    const rollup = {
      batch_id: batchId,
      generated_at: new Date().toISOString(),
      total_jobs: jobs.length,
      done: jobs.filter((j) => j.status === JobStatus.DONE).length,
      partial: jobs.filter((j) => j.status === JobStatus.PARTIAL).length,
      held: jobs.filter((j) => j.status === JobStatus.HELD).length,
      failed: jobs.filter((j) => j.status === JobStatus.FAILED).length,
      total_parsed: jobs.reduce((a, j) => a + (j.counts.parsed || 0), 0),
      total_dropped: jobs.reduce((a, j) => a + (j.counts.dropped_rubbish || 0), 0),
      jobs: jobs.map((j) => ({ job_id: j.job_id, status: j.status, source: j.source_ref })),
    };
    const config = this.getConfig();
    await this.gcsUtils.putJson(config.settings.DATA_BUCKET, `reports/batches/${batchId}/rollup.json`, rollup);
    this.logger.info("batch_rollup_written", { batch_id: batchId, total: jobs.length });
    metrics.increment("report.batch_rollup", 1);
  }

    /**
   * Performs the consumer loop operation.
   */
  public async consumerLoop(): Promise<void> {
    await this.dbManager.initialize();
    this.logger.info("report_consumer_started");
    const config = this.getConfig();
    while (true) {
      const messages = await receiveMessages<ReportMessage>(
        config.settings.REPORT_QUEUE_URL,
        (body) => JSON.parse(body) as ReportMessage,
        5
      );
      for (const { payload, receiptHandle } of messages) {
        try {
          await this.generateReport(payload);
          await deleteMessage(config.settings.REPORT_QUEUE_URL, receiptHandle);
        } catch (exc) {
          const errorStr = String(exc);
          if (errorStr.includes("Job") && (errorStr.includes("not found") || errorStr.includes("cannot transition"))) {
            this.logger.error("report_failed_ack", { job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" });
            metrics.increment("report.error_ack", 1);
            await deleteMessage(config.settings.REPORT_QUEUE_URL, receiptHandle);
          } else {
            this.logger.error("report_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
            metrics.increment("report.error", 1);
          }
        }
      }
    }
  }
}

export default ReportServiceImpl;
