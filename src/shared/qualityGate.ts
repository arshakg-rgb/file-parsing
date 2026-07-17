import Config from "../config/system-config/Config.js";
import ServiceManager from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import MySqlManager from "../config/db/MySqlManager.js";
import { createLogger } from "./logger.js";

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
    if (!ServiceManager.instance) {
      ServiceManager.instance = new QualityGateService(Enforce);
    }
    return ServiceManager.instance as QualityGateService;
  }

  public async calculateMetrics(jobId: string): Promise<QualityMetrics> {
    const jobResult = await this.dbManager.pool.query(
      "SELECT counts FROM parse_jobs WHERE job_id = $1",
      [jobId]
    );
    
    if (jobResult.rows.length === 0) {
      throw new Error(`Job not found: ${jobId}`);
    }

    const counts = jobResult.rows[0].counts as Record<string, number>;
    
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
      await this.dbManager.pool.query(
        "UPDATE parse_jobs SET status = 'held', error = $1 WHERE job_id = $2",
        [reason, jobId]
      );
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
    const result = await this.dbManager.pool.query(
      `SELECT 
        COUNT(*) as total_jobs,
        COUNT(*) FILTER (WHERE status = 'done') as passed_jobs,
        COUNT(*) FILTER (WHERE status = 'held') as held_jobs,
        COUNT(*) FILTER (WHERE status = 'failed') as failed_jobs
       FROM parse_jobs 
       WHERE batch_id = $1`,
      [batchId]
    );

    return {
      totalJobs: parseInt(result.rows[0].total_jobs),
      passedJobs: parseInt(result.rows[0].passed_jobs),
      heldJobs: parseInt(result.rows[0].held_jobs),
      failedJobs: parseInt(result.rows[0].failed_jobs),
    };
  }
}

function Enforce(): void {}

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
