import Config from "../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import MySqlManager from "../config/db/MySqlManager.js";
import FirestoreCacheUtils from "../utils/cache/FirestoreCacheUtils.js";
import { createLogger, Logger } from "../utils/logger/logger.js";
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
  private logger: Logger;
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
    if (!DLQManagerService.instance) {
      DLQManagerService.instance = new DLQManagerService(Enforce);
    }
    return DLQManagerService.instance;
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
    
    const row = await this.dbManager.repositories.deadLetters.create(
      {
        dlq_id: dlqId,
        job_id: jobId,
        byte_offset: byteOffset,
        byte_length: byteLength,
        line_no: lineNo,
        raw_bytes: rawBytes,
        failure_class: failureClass,
        error,
        attempts: 0,
        status: "pending",
      },
      { conflictOn: "job_id_line_no" }
    );
    
    if (!row) {
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
      return buffer.toString("utf-8");
    } catch (error) {
      this.logger.error("dlq_fetch_error", { dlq_id: dlqEntry.dlq_id, error: String(error) });
      throw error;
    }
  }

  public async retryEntry(dlqId: string, s3Url: string): Promise<boolean> {
    const entry = await this.dbManager.repositories.deadLetters.findById(dlqId);
    if (!entry) {
      this.logger.warn("dlq_entry_not_found", { dlq_id: dlqId });
      return false;
    }

    if ((entry.attempts || 0) >= this.MAX_RETRY_ATTEMPTS) {
      await this.markForReview(dlqId);
      this.logger.info("dlq_max_attempts_reached", { dlq_id: dlqId });
      return false;
    }

    await this.dbManager.repositories.deadLetters.incrementAttempts(dlqId, "retry");

    const line = await this.fetchFailedLine(entry as unknown as DeadLetterEntry, s3Url);
    
    this.logger.info("dlq_retry_attempt", { dlq_id: dlqId, attempt: entry.attempts + 1 });
    
    return true;
  }

  public async markForReview(dlqId: string): Promise<void> {
    await this.dbManager.repositories.deadLetters.updateStatus(dlqId, "review");
    this.logger.info("dlq_marked_review", { dlq_id: dlqId });
  }

  public async markResolved(dlqId: string): Promise<void> {
    await this.dbManager.repositories.deadLetters.updateStatus(dlqId, "resolved");
    this.logger.info("dlq_resolved", { dlq_id: dlqId });
  }

  public async getPendingEntries(jobId: string): Promise<DeadLetterEntry[]> {
    return this.dbManager.repositories.deadLetters.findByJobAndStatus(jobId, "pending") as Promise<DeadLetterEntry[]>;
  }

  public async getRetryEntries(jobId: string): Promise<DeadLetterEntry[]> {
    return this.dbManager.repositories.deadLetters.findByJobAndStatus(jobId, "retry") as Promise<DeadLetterEntry[]>;
  }

  public async getReviewEntries(jobId: string): Promise<DeadLetterEntry[]> {
    return this.dbManager.repositories.deadLetters.findByJobAndStatus(jobId, "review") as Promise<DeadLetterEntry[]>;
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
