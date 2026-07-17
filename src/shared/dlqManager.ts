import Config from "../config/system-config/Config.js";
import ServiceManager from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import MySqlManager from "../config/db/MySqlManager.js";
import FirestoreCacheUtils from "../utils/cache/FirestoreCacheUtils.js";
import { createLogger } from "./logger.js";
import crypto from "crypto";

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

class DLQManagerService extends ServiceManager {
  protected static instance: DLQManagerService;
  private logger: any;
  private dbManager: MySqlManager;
  private gcsUtils: FirestoreCacheUtils;
  private readonly MAX_RETRY_ATTEMPTS = 2;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate DLQManagerService directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("dlq-manager");
    this.dbManager = MySqlManager.getInstance();
    this.gcsUtils = FirestoreCacheUtils.getInstance();
  }

  public static getInstance(): DLQManagerService {
    if (!ServiceManager.instance) {
      ServiceManager.instance = new DLQManagerService(Enforce);
    }
    return ServiceManager.instance as DLQManagerService;
  }

  public async addEntry(
    jobId: string,
    byteOffset: number,
    byteLength: number,
    lineNo: number,
    rawBytes: string,
    failureClass: FailureClass,
    error: string
  ): Promise<string | null> {
    const dlqId = crypto.randomUUID();
    
    const result = await this.dbManager.pool.query(
      `INSERT INTO dead_letters (dlq_id, job_id, byte_offset, byte_length, line_no, raw_bytes, failure_class, error, attempts, status, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())
       ON CONFLICT (job_id, line_no) DO NOTHING
       RETURNING dlq_id`,
      [dlqId, jobId, byteOffset, byteLength, lineNo, rawBytes, failureClass, error, 0, "pending"]
    );
    
    if (result.rows.length === 0) {
      this.logger.info("dlq_entry_duplicate_skipped", { job_id: jobId, line_no: lineNo, byte_offset: byteOffset });
      return null;
    }
    
    this.logger.info("dlq_entry_added", { dlq_id: dlqId, job_id: jobId, failure_class: failureClass });
    return dlqId;
  }

  public async fetchFailedLine(dlqEntry: DeadLetterEntry, s3Url: string): Promise<string> {
    try {
      const [bucket, key] = this.gcsUtils.parseGcsUrl(s3Url);
      const buffer = await this.gcsUtils.readRange(bucket, key, dlqEntry.byte_offset, dlqEntry.byte_offset + dlqEntry.byte_length - 1);
      return buffer.toString('utf-8');
    } catch (error) {
      this.logger.error("dlq_fetch_error", { dlq_id: dlqEntry.dlq_id, error: String(error) });
      throw error;
    }
  }

  public async retryEntry(dlqId: string, s3Url: string): Promise<boolean> {
    const result = await this.dbManager.pool.query<DeadLetterEntry>(
      "SELECT * FROM dead_letters WHERE dlq_id = $1",
      [dlqId]
    );
    
    const entry = result.rows[0];
    if (!entry) {
      this.logger.warn("dlq_entry_not_found", { dlq_id: dlqId });
      return false;
    }

    if (entry.attempts >= this.MAX_RETRY_ATTEMPTS) {
      await this.markForReview(dlqId);
      this.logger.info("dlq_max_attempts_reached", { dlq_id: dlqId });
      return false;
    }

    await this.dbManager.pool.query(
      "UPDATE dead_letters SET attempts = attempts + 1, status = 'retry', updated_at = NOW() WHERE dlq_id = $1",
      [dlqId]
    );

    const line = await this.fetchFailedLine(entry, s3Url);
    
    this.logger.info("dlq_retry_attempt", { dlq_id: dlqId, attempt: entry.attempts + 1 });
    
    return true;
  }

  public async markForReview(dlqId: string): Promise<void> {
    await this.dbManager.pool.query(
      "UPDATE dead_letters SET status = 'review', updated_at = NOW() WHERE dlq_id = $1",
      [dlqId]
    );
    this.logger.info("dlq_marked_review", { dlq_id: dlqId });
  }

  public async markResolved(dlqId: string): Promise<void> {
    await this.dbManager.pool.query(
      "UPDATE dead_letters SET status = 'resolved', updated_at = NOW() WHERE dlq_id = $1",
      [dlqId]
    );
    this.logger.info("dlq_resolved", { dlq_id: dlqId });
  }

  public async getPendingEntries(jobId: string): Promise<DeadLetterEntry[]> {
    const result = await this.dbManager.pool.query<DeadLetterEntry>(
      "SELECT * FROM dead_letters WHERE job_id = $1 AND status = 'pending' ORDER BY byte_offset",
      [jobId]
    );
    return result.rows;
  }

  public async getRetryEntries(jobId: string): Promise<DeadLetterEntry[]> {
    const result = await this.dbManager.pool.query<DeadLetterEntry>(
      "SELECT * FROM dead_letters WHERE job_id = $1 AND status = 'retry' ORDER BY byte_offset",
      [jobId]
    );
    return result.rows;
  }

  public async getReviewEntries(jobId: string): Promise<DeadLetterEntry[]> {
    const result = await this.dbManager.pool.query<DeadLetterEntry>(
      "SELECT * FROM dead_letters WHERE job_id = $1 AND status = 'review' ORDER BY byte_offset",
      [jobId]
    );
    return result.rows;
  }

  public async batchRetryJob(jobId: string, s3Url: string): Promise<{ success: number; failed: number }> {
    const entries = await this.getPendingEntries(jobId);
    let success = 0;
    let failed = 0;

    for (const entry of entries) {
      try {
        await this.retryEntry(entry.dlq_id, s3Url);
        success++;
      } catch (error) {
        this.logger.error("dlq_batch_retry_error", { dlq_id: entry.dlq_id, error: String(error) });
        failed++;
      }
    }

    this.logger.info("dlq_batch_retry_complete", { job_id: jobId, success, failed });
    return { success, failed };
  }
}

function Enforce(): void {}

export default DLQManagerService;

const dlqManagerService = DLQManagerService.getInstance();

export class DLQManager {
  async addEntry(
    jobId: string,
    byteOffset: number,
    byteLength: number,
    lineNo: number,
    rawBytes: string,
    failureClass: FailureClass,
    error: string
  ): Promise<string | null> {
    return dlqManagerService.addEntry(jobId, byteOffset, byteLength, lineNo, rawBytes, failureClass, error);
  }

  async fetchFailedLine(dlqEntry: DeadLetterEntry, s3Url: string): Promise<string> {
    return dlqManagerService.fetchFailedLine(dlqEntry, s3Url);
  }

  async retryEntry(dlqId: string, s3Url: string): Promise<boolean> {
    return dlqManagerService.retryEntry(dlqId, s3Url);
  }

  async markForReview(dlqId: string): Promise<void> {
    return dlqManagerService.markForReview(dlqId);
  }

  async markResolved(dlqId: string): Promise<void> {
    return dlqManagerService.markResolved(dlqId);
  }

  async getPendingEntries(jobId: string): Promise<DeadLetterEntry[]> {
    return dlqManagerService.getPendingEntries(jobId);
  }

  async getRetryEntries(jobId: string): Promise<DeadLetterEntry[]> {
    return dlqManagerService.getRetryEntries(jobId);
  }

  async getReviewEntries(jobId: string): Promise<DeadLetterEntry[]> {
    return dlqManagerService.getReviewEntries(jobId);
  }

  async batchRetryJob(jobId: string, s3Url: string): Promise<{ success: number; failed: number }> {
    return dlqManagerService.batchRetryJob(jobId, s3Url);
  }
}
