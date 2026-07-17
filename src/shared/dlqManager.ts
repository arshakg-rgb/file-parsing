import { readRange } from "./gcsUtils.js";
import { pool } from "./db.js";
import { createLogger } from "./logger.js";
import crypto from "crypto";

const logger = createLogger("dlq-manager");

export interface DeadLetterEntry {
  dlq_id: string;
  job_id: string;
  byte_offset: number;
  byte_length: number;
  line_no: number;
  raw_bytes: string;
  failure_class: string;
  error: string;
  attempts: number;
  status: "pending" | "retry" | "review" | "resolved";
}

export enum FailureClass {
  TRANSFORM_ERROR = "transform_error",
  TYPE_MISMATCH = "type_mismatch",
  ENCODING_ERROR = "encoding_error",
  UNCERTAIN = "uncertain",
  TEMPLATE_MISMATCH = "template_mismatch",
  PARSE_ERROR = "parse_error",
  EXTRACTION_ERROR = "extraction_error",
}

export class DLQManager {
  /**
   * Add entry to dead letter queue
   */
  async addEntry(
    jobId: string,
    byteOffset: number,
    byteLength: number,
    lineNo: number,
    rawBytes: string,
    failureClass: FailureClass,
    error: string
  ): Promise<string | null> {
    const dlqId = crypto.randomUUID();
    
    // Use ON CONFLICT DO NOTHING to prevent duplicate entries on restart
    // Unique constraint on (job_id, line_no) ensures idempotency
    const result = await pool.query(
      `INSERT INTO dead_letters (dlq_id, job_id, byte_offset, byte_length, line_no, raw_bytes, failure_class, error, attempts, status, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())
       ON CONFLICT (job_id, line_no) DO NOTHING
       RETURNING dlq_id`,
      [dlqId, jobId, byteOffset, byteLength, lineNo, rawBytes, failureClass, error, 0, "pending"]
    );
    
    // If no row was returned, it means a duplicate already exists
    if (result.rows.length === 0) {
      logger.info("dlq_entry_duplicate_skipped", { job_id: jobId, line_no: lineNo, byte_offset: byteOffset });
      return null; // Indicate this was a duplicate
    }
    
    logger.info("dlq_entry_added", { dlq_id: dlqId, job_id: jobId, failure_class: failureClass });
    return dlqId;
  }

  /**
   * Fetch failed line by byte range for retry
   */
  async fetchFailedLine(dlqEntry: DeadLetterEntry, s3Url: string): Promise<string> {
    try {
      const [bucket, key] = this.parseGcsUrl(s3Url);
      const buffer = await readRange(bucket, key, dlqEntry.byte_offset, dlqEntry.byte_offset + dlqEntry.byte_length - 1);
      return buffer.toString('utf-8');
    } catch (error) {
      logger.error("dlq_fetch_error", { dlq_id: dlqEntry.dlq_id, error: String(error) });
      throw error;
    }
  }

  /**
   * Retry dead letter entry
   */
  async retryEntry(dlqId: string, s3Url: string): Promise<boolean> {
    const result = await pool.query<DeadLetterEntry>(
      "SELECT * FROM dead_letters WHERE dlq_id = $1",
      [dlqId]
    );
    
    const entry = result.rows[0];
    if (!entry) {
      logger.warn("dlq_entry_not_found", { dlq_id: dlqId });
      return false;
    }

    // Check retry limits
    if (entry.attempts >= 2) {
      await this.markForReview(dlqId);
      logger.info("dlq_max_attempts_reached", { dlq_id: dlqId });
      return false;
    }

    // Increment attempts
    await pool.query(
      "UPDATE dead_letters SET attempts = attempts + 1, status = 'retry', updated_at = NOW() WHERE dlq_id = $1",
      [dlqId]
    );

    // Fetch the failed line
    const line = await this.fetchFailedLine(entry, s3Url);
    
    logger.info("dlq_retry_attempt", { dlq_id: dlqId, attempt: entry.attempts + 1 });
    
    // Return the line for reprocessing
    // In production, this would publish to retry queue
    return true;
  }

  /**
   * Mark entry for human review
   */
  async markForReview(dlqId: string): Promise<void> {
    await pool.query(
      "UPDATE dead_letters SET status = 'review', updated_at = NOW() WHERE dlq_id = $1",
      [dlqId]
    );
    logger.info("dlq_marked_review", { dlq_id: dlqId });
  }

  /**
   * Mark entry as resolved
   */
  async markResolved(dlqId: string): Promise<void> {
    await pool.query(
      "UPDATE dead_letters SET status = 'resolved', updated_at = NOW() WHERE dlq_id = $1",
      [dlqId]
    );
    logger.info("dlq_resolved", { dlq_id: dlqId });
  }

  /**
   * Get pending entries for a job
   */
  async getPendingEntries(jobId: string): Promise<DeadLetterEntry[]> {
    const result = await pool.query<DeadLetterEntry>(
      "SELECT * FROM dead_letters WHERE job_id = $1 AND status = 'pending' ORDER BY byte_offset",
      [jobId]
    );
    return result.rows;
  }

  /**
   * Get retry entries for a job
   */
  async getRetryEntries(jobId: string): Promise<DeadLetterEntry[]> {
    const result = await pool.query<DeadLetterEntry>(
      "SELECT * FROM dead_letters WHERE job_id = $1 AND status = 'retry' ORDER BY byte_offset",
      [jobId]
    );
    return result.rows;
  }

  /**
   * Get review entries for a job
   */
  async getReviewEntries(jobId: string): Promise<DeadLetterEntry[]> {
    const result = await pool.query<DeadLetterEntry>(
      "SELECT * FROM dead_letters WHERE job_id = $1 AND status = 'review' ORDER BY byte_offset",
      [jobId]
    );
    return result.rows;
  }

  /**
   * Batch retry for a job
   */
  async batchRetryJob(jobId: string, s3Url: string): Promise<{ success: number; failed: number }> {
    const entries = await this.getPendingEntries(jobId);
    let success = 0;
    let failed = 0;

    for (const entry of entries) {
      try {
        await this.retryEntry(entry.dlq_id, s3Url);
        success++;
      } catch (error) {
        logger.error("dlq_batch_retry_error", { dlq_id: entry.dlq_id, error: String(error) });
        failed++;
      }
    }

    logger.info("dlq_batch_retry_complete", { job_id: jobId, success, failed });
    return { success, failed };
  }

  private parseGcsUrl(url: string): [string, string] {
    const prefix = url.startsWith("gs://") ? "gs://" : url.startsWith("s3://") ? "s3://" : null;
    if (!prefix) throw new Error(`Expected gs:// URL, got: ${url}`);
    const rest = url.slice(prefix.length);
    const slash = rest.indexOf("/");
    if (slash === -1) return [rest, ""];
    return [rest.slice(0, slash), rest.slice(slash + 1)];
  }
}
