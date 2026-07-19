import Config from "../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import MySqlManager from "../config/db/MySqlManager.js";
import { createLogger, Logger } from "../utils/logger/logger.js";
import { totalFailed } from "../shared/models/job.js";

export interface QualityMetrics {
  totalLines: number;
  parsedLines: number;
  droppedRubbishLines: number;
  failedLines: number;
  failedLineRatio: number;
}

export class QualityGate extends ServiceManager {
  protected static instance: QualityGate;
  private logger: Logger;
  private dbManager: MySqlManager;
  private readonly FAILED_LINE_RATIO_THRESHOLD: number;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate QualityGate directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("quality-gate");
    this.dbManager = MySqlManager.getInstance();
    this.FAILED_LINE_RATIO_THRESHOLD = 0.1;
  }

  public static getInstance(): QualityGate {
    if (!QualityGate.instance) {
      QualityGate.instance = new QualityGate(Enforce);
    }
    return QualityGate.instance;
  }

  public async calculateMetrics(jobId: string): Promise<QualityMetrics> {
    const counts = await this.dbManager.repositories.jobs.getCounts(jobId);
    
    if (!counts) {
      throw new Error(`Job not found: ${jobId}`);
    }
    
    const failed = totalFailed(counts);
    const totalLines = counts.parsed + counts.dropped_rubbish + failed;
    const failedLineRatio = totalLines > 0 ? failed / totalLines : 0;

    return {
      totalLines,
      parsedLines: counts.parsed || 0,
      droppedRubbishLines: counts.dropped_rubbish || 0,
      failedLines: failed || 0,
      failedLineRatio,
    };
  }

  public async passesQualityGate(jobId: string): Promise<{ passes: boolean; reason?: string }> {
    const metrics = await this.calculateMetrics(jobId);
    
    this.logger.info("quality_gate_check", { 
      job_id: jobId, 
      failed_line_ratio: metrics.failedLineRatio,
      threshold: this.FAILED_LINE_RATIO_THRESHOLD 
    });

    if (metrics.failedLineRatio > this.FAILED_LINE_RATIO_THRESHOLD) {
      return {
        passes: false,
        reason: `Failed line ratio ${metrics.failedLineRatio.toFixed(2)} exceeds threshold ${this.FAILED_LINE_RATIO_THRESHOLD}`
      };
    }

    return { passes: true };
  }

  public async applyQualityGate(jobId: string): Promise<void> {
    const { passes, reason } = await this.passesQualityGate(jobId);
    
    if (!passes) {
      await this.dbManager.repositories.jobs.hold(jobId, reason);
      this.logger.warn("quality_gate_failed", { job_id: jobId, reason });
    } else {
      this.logger.info("quality_gate_passed", { job_id: jobId });
    }
  }

  public async getBatchQualityStats(batchId: string): Promise<{
    totalJobs: number;
    passedJobs: number;
    heldJobs: number;
    failedJobs: number;
  }> {
    return this.dbManager.repositories.jobs.getBatchStats(batchId);
  }
}


export default QualityGate;
