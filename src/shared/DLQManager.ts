import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import MySqlManager from "@config/db/MySqlManager.js";
import FirestoreCacheUtils from "@utils/cache/FirestoreCacheUtils.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
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

/**
 * DLQManager is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
export class DLQManager extends ServiceManager {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: DLQManager;
    /**
   * Logger instance
   * @private
   */
  private logger: Logger;
    /**
   * Db Manager
   * @private
   */
  private dbManager: MySqlManager;
    /**
   * Gcs Utils
   * @private
   */
  private gcsUtils: FirestoreCacheUtils;
    /**
   * M A X_ R E T R Y_ A T T E M P T S
   * @private
   */
  private readonly MAX_RETRY_ATTEMPTS = 2;

    /**
   * Constructs a new DLQManager instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate DLQManager directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("dlq-manager");
    this.dbManager = MySqlManager.getInstance();
    this.gcsUtils = FirestoreCacheUtils.getInstance();
  }

    /**
   * Gets the single instance of the DLQManager class.
   * @returns The single instance of the class
   */
  public static getInstance(): DLQManager {
    if (!DLQManager.instance) {
      DLQManager.instance = new DLQManager(Enforce);
    }
    return DLQManager.instance;
  }

    /**
   * Adds entry
   * @param jobId - The job identifier
   * @param byteOffset - The byte offset
   * @param byteLength - The byte length
   * @param lineNo - The line no
   * @param rawBytes - The raw bytes
   * @param failureClass - The failure class
   * @param error - The error that occurred
   * @returns A promise that resolves to the result
   */
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

    /**
   * Fetches failed line
   * @param dlqEntry - The dlq entry
   * @param s3Url - The s3 url
   * @returns A promise that resolves to the result
   */
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

    /**
   * Retries entry
   * @param dlqId - The dlq id
   * @param s3Url - The s3 url
   * @returns True if the operation succeeds, false otherwise
   */
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

    /**
   * Marks for review
   * @param dlqId - The dlq id
   */
  public async markForReview(dlqId: string): Promise<void> {
    await this.dbManager.repositories.deadLetters.updateStatus(dlqId, "review");
    this.logger.info("dlq_marked_review", { dlq_id: dlqId });
  }

    /**
   * Marks resolved
   * @param dlqId - The dlq id
   */
  public async markResolved(dlqId: string): Promise<void> {
    await this.dbManager.repositories.deadLetters.updateStatus(dlqId, "resolved");
    this.logger.info("dlq_resolved", { dlq_id: dlqId });
  }

    /**
   * Gets pending entries
   * @param jobId - The job identifier
   * @returns A promise that resolves to the list
   */
  public async getPendingEntries(jobId: string): Promise<DeadLetterEntry[]> {
    return this.dbManager.repositories.deadLetters.findByJobAndStatus(jobId, "pending") as Promise<DeadLetterEntry[]>;
  }

    /**
   * Gets retry entries
   * @param jobId - The job identifier
   * @returns A promise that resolves to the list
   */
  public async getRetryEntries(jobId: string): Promise<DeadLetterEntry[]> {
    return this.dbManager.repositories.deadLetters.findByJobAndStatus(jobId, "retry") as Promise<DeadLetterEntry[]>;
  }

    /**
   * Gets review entries
   * @param jobId - The job identifier
   * @returns A promise that resolves to the list
   */
  public async getReviewEntries(jobId: string): Promise<DeadLetterEntry[]> {
    return this.dbManager.repositories.deadLetters.findByJobAndStatus(jobId, "review") as Promise<DeadLetterEntry[]>;
  }

    /**
   * Performs the batch retry job operation.
   * @param jobId - The job identifier
   * @param s3Url - The s3 url
   * @returns A promise that resolves to the result
   */
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


export default DLQManager;
