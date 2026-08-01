import crypto from "crypto";
import PostgreSqlManager from "@config/db/PostgreSqlManager.js";
import type { ParseJobAttributes } from "@config/db/models/ParseJob.js";
import type { OutputPartAttributes } from "@config/db/models/OutputPart.js";
import type { DeadLetterAttributes } from "@config/db/models/DeadLetter.js";
import {IPendingArchiveEntry} from "@config/db/models";
import {InstantiationError} from "@errors/InstantiationError";
export type ParseJobRow = ParseJobAttributes;
export type OutputPartRow = OutputPartAttributes;
export type DeadLetterRow = DeadLetterAttributes;

/**
 * DatabaseService is a thin, typed facade over PostgreSqlManager's
 * repositories and models — job lookups, pending-archive-entry lifecycle,
 * and schema sync.
 *
 * This IS a true singleton (unlike per-job classes such as CsvOutputWriter):
 * there is exactly one Postgres connection and one repository set for the
 * whole process, so caching a single instance behind getInstance() is
 * correct and safe — there's no per-caller state that concurrent callers
 * could corrupt by sharing it.
 */

export class DatabaseService
{
  private static instance: DatabaseService;

  private readonly dbManager: PostgreSqlManager;
  public readonly repositories: PostgreSqlManager["repositories"];

  /**
   * @param enforce - Capability token; must be the module-private Enforce
   *   function. Callers cannot obtain a reference to it, so this constructor
   *   is effectively only reachable via DatabaseService.getInstance().
   * @param dbManager - The underlying PostgreSqlManager singleton instance.
   */

  private constructor(enforce: () => void, dbManager: PostgreSqlManager)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use DatabaseService.getInstance() instead of new.");
    }

    this.dbManager = dbManager;
    this.repositories = dbManager.repositories;
  }

  /**
   * Gets the singleton instance of DatabaseService.
   *
   * @returns The singleton instance of DatabaseService.
   */

  public static getInstance(): DatabaseService
  {
    if (!DatabaseService.instance)
    {
      DatabaseService.instance = new DatabaseService(Enforce, PostgreSqlManager.getInstance());
    }

    return DatabaseService.instance;
  }

  /**
   * Waits for the underlying database connection/pool to be ready.
   */

  public async waitForDb(): Promise<void>
  {
    await this.dbManager.initialize();
  }

  /**
   * Gets a single parse job by id.
   * @param jobId - The job identifier.
   * @returns The job row, or null if not found.
   */

  public async getJob(jobId: string): Promise<ParseJobRow | null>
  {
    return this.repositories.jobs.findById(jobId);
  }

  /**
   * Gets all parse jobs belonging to a batch.
   * @param batchId - The batch identifier.
   * @returns The matching job rows.
   */

  public async getBatchJobs(batchId: string): Promise<ParseJobRow[]>
  {
    return this.repositories.jobs.findByBatchId(batchId);
  }

  /**
   * Creates a pending archive entry row in the "pending" state.
   * @param jobId - The job identifier.
   * @param entryName - The archive entry name.
   * @param entrySize - The archive entry size, in bytes.
   * @returns True if the row was created successfully.
   */

  public async createPendingArchiveEntry(jobId: string, entryName: string, entrySize: number): Promise<boolean>
  {
    const row: IPendingArchiveEntry = await this.repositories.pendingArchiveEntries.create({
      id: crypto.randomUUID(),
      job_id: jobId,
      entry_name: entryName,
      entry_size: entrySize,
      status: "pending",
    });

    return row !== null;
  }

  /**
   * Marks a pending archive entry as "processing".
   * @param jobId - The job identifier.
   * @param entryName - The archive entry name.
   */

  public async markPendingEntryProcessing(jobId: string, entryName: string): Promise<void> {
    const entry = await this.findPendingEntry(jobId, entryName);
    if (entry) await this.repositories.pendingArchiveEntries.markStatus(entry.id, "processing");
  }

  /**
   * Marks a pending archive entry as "completed".
   * @param jobId - The job identifier.
   * @param entryName - The archive entry name.
   */

  public async markPendingEntryCompleted(jobId: string, entryName: string): Promise<void> {
    const entry = await this.findPendingEntry(jobId, entryName);
    if (entry) await this.repositories.pendingArchiveEntries.markStatus(entry.id, "completed");
  }

  /**
   * Marks a pending archive entry as "failed".
   * @param jobId - The job identifier.
   * @param entryName - The archive entry name.
   * @param error - The error message describing the failure.
   */

  public async markPendingEntryFailed(jobId: string, entryName: string, error: string): Promise<void> {
    const entry = await this.findPendingEntry(jobId, entryName);
    if (entry) await this.repositories.pendingArchiveEntries.markStatus(entry.id, "failed", error);
  }

  /**
   * Syncs the database schema without dropping existing tables.
   */

  public async createTables(): Promise<void> {
    await this.dbManager.sequelize.sync({ force: false });
  }

  /**
   * Looks up a pending archive entry by job id + entry name. Shared by the
   * three markPendingEntry* methods to avoid repeating the same query.
   * @param jobId - The job identifier.
   * @param entryName - The archive entry name.
   */

  private async findPendingEntry(jobId: string, entryName: string)
  {
    return this.dbManager.models.PendingArchiveEntry.findOne({
      where: { job_id: jobId, entry_name: entryName },
    });
  }
}

export { default as DatabaseManager } from "@config/db/PostgreSqlManager.js";

/**
 * Private capability token. Only DatabaseService.getInstance() has a
 * reference to this function, so it's the only call site that can satisfy
 * the constructor's `enforce` check — `new DatabaseService(...)` from
 * anywhere else fails fast with InstantiationError.
 */
function Enforce(): void {}
