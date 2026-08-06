import { BigQueryManager, paramTypes, toDate } from "../BigQueryManager.js";
import { settings } from "@shared/Settings.js";
import type {
  PendingArchiveEntryAttributes,
  PendingArchiveEntryCreationAttributes,
} from "../models/PendingArchiveEntry.js";

const TABLE = "pending_archive_entries";
const FULL_TABLE = `\`${settings.BIGQUERY_PROJECT_ID}.${settings.BIGQUERY_DATASET}.${TABLE}\``;

const NULLABLE_TYPES: Record<string, string> = {
  error: "STRING",
};

/**
 * BigQuery-backed repository for pending_archive_entries.
 */
export class PendingArchiveEntryRepository
{
  constructor() {}

  private fromRow(row: Record<string, unknown>): PendingArchiveEntryAttributes
  {
    return {
      id: row.id as string,
      job_id: row.job_id as string,
      entry_name: row.entry_name as string,
      entry_size: Number(row.entry_size ?? 0),
      status: row.status as string,
      error: (row.error as string | null) ?? null,
      created_at: toDate(row.created_at),
      updated_at: toDate(row.updated_at),
    };
  }

  /**
   * Creates a pending archive entry.
   */
  async create(data: PendingArchiveEntryCreationAttributes): Promise<PendingArchiveEntryAttributes | null>
  {
    const now = new Date();

    const params = {
      id: data.id,
      job_id: data.job_id,
      entry_name: data.entry_name,
      entry_size: data.entry_size,
      status: data.status,
      error: data.error ?? null,
      created_at: now,
      updated_at: now,
    };

    await BigQueryManager.getInstance().execute(
      `INSERT INTO ${FULL_TABLE} (
        id, job_id, entry_name, entry_size, status, error, created_at, updated_at
      ) VALUES (
        @id, @job_id, @entry_name, @entry_size, @status, @error, @created_at, @updated_at
      )`,
      params,
      paramTypes(params, NULLABLE_TYPES)
    );

    return this.findById(data.id);
  }

  /**
   * Finds a pending archive entry by id.
   */
  async findById(id: string): Promise<PendingArchiveEntryAttributes | null>
  {
    const [row] = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE} WHERE id = @id LIMIT 1`,
      { id }
    );

    return row ? this.fromRow(row) : null;
  }

  /**
   * Updates the status and optionally the error of an entry.
   */
  async markStatus(id: string, status: string, error?: string): Promise<void>
  {
    const setParts: string[] = ["status = @status", "updated_at = CURRENT_TIMESTAMP()"];
    const params: Record<string, unknown> = { id, status };

    if (error !== undefined)
    {
      params.error = error;
      setParts.push("error = @error");
    }

    await BigQueryManager.getInstance().execute(
      `UPDATE ${FULL_TABLE} SET ${setParts.join(", ")} WHERE id = @id`,
      params,
      paramTypes(params, NULLABLE_TYPES)
    );
  }

  /**
   * Finds all entries for a job.
   */
  async findByJob(jobId: string): Promise<PendingArchiveEntryAttributes[]>
  {
    const rows = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE} WHERE job_id = @job_id ORDER BY created_at DESC`,
      { job_id: jobId }
    );

    return rows.map((r) => this.fromRow(r));
  }

  /**
   * Finds stale entries for a job.
   */
  async findStaleEntries(
    jobId: string,
    hours = 3,
    statuses = ["pending", "processing"]
  ): Promise<PendingArchiveEntryAttributes[]>
  {
    const rows = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE}
       WHERE job_id = @job_id
         AND status IN UNNEST(@statuses)
         AND updated_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)`,
      { job_id: jobId, statuses, hours }
    );

    return rows.map((r) => this.fromRow(r));
  }

  /**
   * Gets the count of entries grouped by status for a job.
   */
  async getCountByJob(jobId: string): Promise<{ pending: number; completed: number; failed: number }>
  {
    const [row] = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT
        COUNTIF(status = 'pending') AS pending,
        COUNTIF(status = 'completed') AS completed,
        COUNTIF(status = 'failed') AS failed
      FROM ${FULL_TABLE}
      WHERE job_id = @job_id`,
      { job_id: jobId }
    );

    return {
      pending: Number(row?.pending ?? 0),
      completed: Number(row?.completed ?? 0),
      failed: Number(row?.failed ?? 0),
    };
  }

  /**
   * Gets the total size of completed/processing entries for a job.
   */
  async getTotalSize(jobId: string): Promise<number>
  {
    const [row] = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT COALESCE(SUM(entry_size), 0) AS total
      FROM ${FULL_TABLE}
      WHERE job_id = @job_id AND status IN UNNEST(@statuses)`,
      { job_id: jobId, statuses: ["completed", "processing"] }
    );

    return Number(row?.total ?? 0);
  }
}
