import Config from "../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import MySqlManager from "../config/db/MySqlManager.js";
import { createLogger } from "../utils/logger/logger.js";

export interface QualityMetrics {
  totalLines: number;
  parsedLines: number;
  droppedRubbishLines: number;
  failedLines: number;
  failedLineRatio: number;
}

class QualityGateService extends ServiceManager {
  protected static instance: QualityGateService;
  private logger: any;
  private dbManager: MySqlManager;
  private readonly FAILED_LINE_RATIO_THRESHOLD: number;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate QualityGateService directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("quality-gate");
    this.dbManager = MySqlManager.getInstance();
    this.FAILED_LINE_RATIO_THRESHOLD = 0.1;
  }

  public static getInstance(): QualityGateService {
    if (!QualityGateService.instance) {
      QualityGateService.instance = new QualityGateService(Enforce);
    }
    return QualityGateService.instance;
  }

  public async calculateMetrics(jobId: string): Promise<QualityMetrics> {
    const counts = await this.dbManager.repositories.jobs.getCounts(jobId);
    
    if (!counts) {
      throw new Error(`Job not found: ${jobId}`);
    }
    
    const totalLines = counts.parsed + counts.dropped_rubbish + counts.failed;
    const failedLineRatio = totalLines > 0 ? counts.failed / totalLines : 0;

    return {
      totalLines,
      parsedLines: counts.parsed || 0,
      droppedRubbishLines: counts.dropped_rubbish || 0,
      failedLines: counts.failed || 0,
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


export default QualityGateService;

const qualityGateService = QualityGateService.getInstance();

export class QualityGate {
  private readonly FAILED_LINE_RATIO_THRESHOLD = 0.1;

  async calculateMetrics(jobId: string): Promise<QualityMetrics> {
    return qualityGateService.calculateMetrics(jobId);
  }

  async passesQualityGate(jobId: string): Promise<{ passes: boolean; reason?: string }> {
    return qualityGateService.passesQualityGate(jobId);
  }

  async applyQualityGate(jobId: string): Promise<void> {
    return qualityGateService.applyQualityGate(jobId);
  }

  async getBatchQualityStats(batchId: string): Promise<{
    totalJobs: number;
    passedJobs: number;
    heldJobs: number;
    failedJobs: number;
  }> {
    return qualityGateService.getBatchQualityStats(batchId);
  }
}
