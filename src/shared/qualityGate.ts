import { pool } from "./db.js";
import { settings } from "./config.js";
import { createLogger } from "./logger.js";

const logger = createLogger("quality-gate");

export interface QualityMetrics {
  totalLines: number;
  parsedLines: number;
  droppedRubbishLines: number;
  failedLines: number;
  failedLineRatio: number;
}

export class QualityGate {
  private readonly FAILED_LINE_RATIO_THRESHOLD = settings.FAILED_LINE_RATIO_THRESHOLD;

  /**
   * Calculate quality metrics for a job
   */
  async calculateMetrics(jobId: string): Promise<QualityMetrics> {
    // Get job details
    const jobResult = await pool.query(
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

  /**
   * Check if job passes quality gate
   */
  async passesQualityGate(jobId: string): Promise<{ passes: boolean; reason?: string }> {
    const metrics = await this.calculateMetrics(jobId);
    
    logger.info("quality_gate_check", { 
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

  /**
   * Apply quality gate and update job status
   */
  async applyQualityGate(jobId: string): Promise<void> {
    const { passes, reason } = await this.passesQualityGate(jobId);
    
    if (!passes) {
      await pool.query(
        "UPDATE parse_jobs SET status = 'held', error = $1 WHERE job_id = $2",
        [reason, jobId]
      );
      logger.warn("quality_gate_failed", { job_id: jobId, reason });
    } else {
      logger.info("quality_gate_passed", { job_id: jobId });
    }
  }

  /**
   * Get quality gate statistics for a batch
   */
  async getBatchQualityStats(batchId: string): Promise<{
    totalJobs: number;
    passedJobs: number;
    heldJobs: number;
    failedJobs: number;
  }> {
    const result = await pool.query(
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
