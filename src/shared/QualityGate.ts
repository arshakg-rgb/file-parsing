import pino from "pino";
import ServiceManager from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { DatabaseManager } from "@shared/DatabaseManager.js";
import { createLogger } from "@utils/logger/Log.js";
import {JobCounts, totalFailed} from "@shared/models/job.js";
import {QualityMetrics} from "@shared/io/IQualityGate";


/**
 * QualityGate is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
export class QualityGate extends ServiceManager
{
    /**
   * Singleton instance
   * @private
   */

  protected static instance: QualityGate;

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
   * FAILED LINE RATIO THRESHOLD
   * @private
   */

  private readonly FAILED_LINE_RATIO_THRESHOLD: number;

    /**
   * Constructs a new QualityGate instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */

  private constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE,"Cannot instantiate QualityGate directly. Use getInstance()");
    }
    super(enforce);

    this.logger = createLogger(module);
    this.dbManager = DatabaseManager.getInstance();
    this.FAILED_LINE_RATIO_THRESHOLD = 0.1;
  }

    /**
   * Gets the single instance of the QualityGate class.
   * @returns The single instance of the class
   */

  public static getInstance(): QualityGate
  {
    if (!QualityGate.instance)
    {
      QualityGate.instance = new QualityGate(Enforce);
    }

    return QualityGate.instance;
  }

    /**
   * Calculates metrics
   * @param jobId - The job identifier
   * @returns A promise that resolves to the result
   */

  public async calculateMetrics(jobId: string): Promise<QualityMetrics>
  {
    const counts: JobCounts = await this.dbManager.repositories.jobs.getCounts(jobId);

    if (!counts)
    {
      throw new Error(`Job not found: ${jobId}`);
    }

    const failed: number = totalFailed(counts);
    const totalLines: number = counts.parsed + counts.dropped_rubbish + failed;
    const failedLineRatio: number = totalLines > 0 ? failed / totalLines : 0;

    return {
      totalLines,
      parsedLines: counts.parsed || 0,
      droppedRubbishLines: counts.dropped_rubbish || 0,
      failedLines: failed || 0,
      failedLineRatio,
    };
  }

    /**
   * Performs the passes quality gate operation.
   * @param jobId - The job identifier
   * @returns A promise that resolves to the result
   */

  public async passesQualityGate(jobId: string): Promise<{ passes: boolean; reason?: string }>
  {
    const metrics: QualityMetrics = await this.calculateMetrics(jobId);

    this.logger.info("quality_gate_check", {
      job_id: jobId,
      failed_line_ratio: metrics.failedLineRatio,
      threshold: this.FAILED_LINE_RATIO_THRESHOLD
    });

    if (metrics.failedLineRatio > this.FAILED_LINE_RATIO_THRESHOLD)
    {
      return {
        passes: false,
        reason: `Failed line ratio ${metrics.failedLineRatio.toFixed(2)} exceeds threshold ${this.FAILED_LINE_RATIO_THRESHOLD}`
      };
    }

    return { passes: true };
  }
}

function Enforce(): void {}
