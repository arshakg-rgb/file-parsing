import crypto from "crypto";
import { BigQueryManager } from "@config/db/BigQueryManager.js";
import { Repositories } from "@config/db/repositories/index.js";
import type { ParseJobAttributes } from "@config/db/models/ParseJob.js";
import type { OutputPartAttributes } from "@config/db/models/OutputPart.js";
import type { DeadLetterAttributes } from "@config/db/models/DeadLetter.js";
import { InstantiationError } from "@errors/InstantiationError";
import { EventType, makeJobEvent } from "@shared/models/events.js";
import { JobStatus } from "@shared/models/job.js";
import { QueueService } from "@shared/QueueService.js";

export type ParseJobRow = ParseJobAttributes;
export type OutputPartRow = OutputPartAttributes;
export type DeadLetterRow = DeadLetterAttributes;

/**
 * DatabaseService is a thin, typed facade over BigQuery repositories.
 *
 * This is the single point of access for all persisted state. It replaces
 * the previous DatabaseManager-backed implementation.
 */

export class DatabaseService
{
  private static instance: DatabaseService;

  public readonly repositories: Repositories;

  private constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use DatabaseManager.getInstance() instead of new.");
    }

    this.repositories = new Repositories();
  }

  /**
   * Gets the singleton instance of the database service.
   */
  public static getInstance(): DatabaseService
  {
    if (!DatabaseService.instance)
    {
      DatabaseService.instance = new DatabaseService(Enforce);
    }

    return DatabaseService.instance;
  }

  /**
   * Waits for the BigQuery client to be ready (dataset verified).
   */
  public async waitForDb(): Promise<void>
  {
    await BigQueryManager.getInstance().initialize();
    await this.repositories.passwordState.ensureTable();
  }

  /**
   * Alias for waitForDb; keeps the previous lifecycle method names working.
   */
  public async initialize(): Promise<void>
  {
    await this.waitForDb();
  }

  /**
   * No-op for BigQuery: tables must be created in the BigQuery console.
   */
  public async createTables(): Promise<void>
  {
    // Tables are managed out-of-band.
  }

  /**
   * Gets a single parse job by id.
   */
  public async getJob(jobId: string): Promise<ParseJobRow | null>
  {
    return this.repositories.jobs.findById(jobId);
  }

  /**
   * Gets all parse jobs belonging to a batch.
   */
  public async getBatchJobs(batchId: string): Promise<ParseJobRow[]>
  {
    return this.repositories.jobs.findByBatchId(batchId);
  }

  /**
   * Creates a pending archive entry row in the "pending" state.
   */
  public async createPendingArchiveEntry(jobId: string, entryName: string, entrySize: number): Promise<string | null>
  {
    const id = crypto.randomUUID();
    await this.repositories.pendingArchiveEntries.create({
      id,
      job_id: jobId,
      entry_name: entryName,
      entry_size: entrySize,
      status: "pending",
    });

    return id;
  }

  /**
   * Marks a pending archive entry as "processing".
   */
  public async markPendingEntryProcessing(entryId: string): Promise<void>
  {
    await this.repositories.pendingArchiveEntries.markStatus(entryId, "processing");
  }

  /**
   * Marks a pending archive entry as "completed" and checks whether the
   * parent job is now fully resolved.
   */
  public async markPendingEntryCompleted(jobId: string, entryId: string): Promise<void>
  {
    await this.repositories.pendingArchiveEntries.markStatus(entryId, "completed");
    await this.attemptParentCompletion(jobId);
  }

  /**
   * Marks a pending archive entry as "failed" and checks whether the
   * parent job is now fully resolved.
   */
  public async markPendingEntryFailed(jobId: string, entryId: string, error: string): Promise<void>
  {
    await this.repositories.pendingArchiveEntries.markStatus(entryId, "failed", error);
    await this.attemptParentCompletion(jobId);
  }

  /**
   * Publishes a parent completion event when all of its pending archive entries
   * have been processed. The StateMachine's atomic transition guard ensures that
   * only one concurrent completion actually writes the final job status.
   *
   * @private
   */
  private async attemptParentCompletion(jobId: string): Promise<void>
  {
    const counts = await this.repositories.pendingArchiveEntries.getCountByJob(jobId);

    if (counts.pending > 0)
    {
      return;
    }

    const newStatus = counts.failed > 0 ? JobStatus.FAILED : JobStatus.COMPLETED;

    await QueueService.getInstance().publishEvent(makeJobEvent(EventType.JOB_STATUS_CHANGED, jobId, "archive-entry-consumer", {
      new_status: newStatus,
    }));
  }

}

export { DatabaseService as DatabaseManager };

function Enforce(): void {}
