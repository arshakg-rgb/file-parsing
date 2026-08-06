import { BigQueryManager, toDate } from "../BigQueryManager.js";
import { settings } from "@shared/Settings.js";
import type {
  DeadLetterAttributes,
  DeadLetterCreationAttributes,
} from "../models/DeadLetter.js";

const TABLE = "dead_letters";
const FULL_TABLE = `\`${settings.BIGQUERY_PROJECT_ID}.${settings.BIGQUERY_DATASET}.${TABLE}\``;

/**
 * BigQuery-backed repository for dead_letters.
 */
export class DeadLetterRepository
{
  constructor() {}

  private fromRow(row: Record<string, unknown>): DeadLetterAttributes
  {
    return {
      dlq_id: row.dlq_id as string,
      job_id: row.job_id as string,
      byte_offset: Number(row.byte_offset ?? 0),
      byte_length: Number(row.byte_length ?? 0),
      line_no: Number(row.line_no ?? 0),
      raw_bytes: row.raw_bytes as string,
      failure_class: row.failure_class as string,
      error: row.error as string,
      attempts: Number(row.attempts ?? 0),
      status: row.status as string,
      created_at: toDate(row.created_at),
      updated_at: toDate(row.updated_at),
    };
  }

  /**
   * Checks for an existing row when conflictOn is set, then inserts.
   */
  async create(data: DeadLetterCreationAttributes, options?: { conflictOn?: "job_id_line_no" | "dlq_id" }): Promise<DeadLetterAttributes | null>
  {
    if (options?.conflictOn === "job_id_line_no")
    {
      const existing = await BigQueryManager.getInstance().queryOne<Record<string, unknown>>(
        TABLE,
        { job_id: data.job_id, line_no: data.line_no }
      );
      if (existing) return null;
    }

    const now = new Date();

    try
    {
      await BigQueryManager.getInstance().insertOne(TABLE, {
        dlq_id: data.dlq_id,
        job_id: data.job_id,
        byte_offset: data.byte_offset,
        byte_length: data.byte_length,
        line_no: data.line_no,
        raw_bytes: data.raw_bytes,
        failure_class: data.failure_class,
        error: data.error,
        attempts: data.attempts ?? 0,
        status: data.status,
        created_at: now,
        updated_at: now,
      });
    }
    catch
    {
      return null;
    }

    return this.findById(data.dlq_id);
  }

  /**
   * Streaming-inserts rows into the dead_letters table.
   */
  async bulkCreate(rows: DeadLetterCreationAttributes[]): Promise<void>
  {
    const bqRows = rows.map((r) => ({
      ...r,
      created_at: new Date(),
      updated_at: new Date(),
    })) as Record<string, unknown>[];

    await BigQueryManager.getInstance().insert(TABLE, bqRows);
  }

  /**
   * Finds a dead letter by id.
   */
  async findById(dlqId: string): Promise<DeadLetterAttributes | null>
  {
    const row = await BigQueryManager.getInstance().queryOne<Record<string, unknown>>(TABLE, { dlq_id: dlqId });
    return row ? this.fromRow(row) : null;
  }

  /**
   * Finds dead letters for a job and status.
   */
  async findByJobAndStatus(jobId: string, status: string): Promise<DeadLetterAttributes[]>
  {
    const rows = await BigQueryManager.getInstance().queryMany<Record<string, unknown>>(
      TABLE,
      { job_id: jobId, status },
      { column: "byte_offset", direction: "ASC" }
    );

    return rows.map((r) => this.fromRow(r));
  }

  /**
   * Finds all dead letters for a job.
   */
  async findByJob(jobId: string): Promise<DeadLetterAttributes[]>
  {
    const rows = await BigQueryManager.getInstance().queryMany<Record<string, unknown>>(
      TABLE,
      { job_id: jobId },
      { column: "byte_offset", direction: "ASC" }
    );

    return rows.map((r) => this.fromRow(r));
  }

  /**
   * Increments attempts and optionally sets a new status.
   */
  async incrementAttempts(dlqId: string, status?: string): Promise<void>
  {
    const setParts: string[] = ["attempts = attempts + 1", "updated_at = CURRENT_TIMESTAMP()"];
    const params: Record<string, unknown> = { dlq_id: dlqId };

    if (status !== undefined)
    {
      params.status = status;
      setParts.push("status = @status");
    }

    await BigQueryManager.getInstance().execute(
      `UPDATE ${FULL_TABLE} SET ${setParts.join(", ")} WHERE dlq_id = @dlq_id`,
      params
    );
  }

  /**
   * Updates the status and optionally the attempts.
   */
  async updateStatus(dlqId: string, status: string, options?: { attempts?: number }): Promise<void>
  {
    const setParts: string[] = ["status = @status", "updated_at = CURRENT_TIMESTAMP()"];
    const params: Record<string, unknown> = { dlq_id: dlqId, status };

    if (options?.attempts !== undefined)
    {
      params.attempts = options.attempts;
      setParts.push("attempts = @attempts");
    }

    await BigQueryManager.getInstance().execute(
      `UPDATE ${FULL_TABLE} SET ${setParts.join(", ")} WHERE dlq_id = @dlq_id`,
      params
    );
  }

  /**
   * Updates the line number.
   */
  async updateLineNo(dlqId: string, lineNo: number): Promise<void>
  {
    await BigQueryManager.getInstance().execute(
      `UPDATE ${FULL_TABLE} SET line_no = @line_no, updated_at = CURRENT_TIMESTAMP() WHERE dlq_id = @dlq_id`,
      { dlq_id: dlqId, line_no: lineNo }
    );
  }

  /**
   * Counts dead letters for a job.
   */
  async countByJob(jobId: string): Promise<number>
  {
    const [row] = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT COUNT(*) AS count FROM ${FULL_TABLE} WHERE job_id = @job_id`,
      { job_id: jobId }
    );

    return Number(row?.count ?? 0);
  }

  /**
   * Summarizes dead letters for a job.
   */
  async getSummaryByJob(jobId: string, lineNumbersLimit = 500): Promise<{
    count: number;
    line_numbers: number[];
    line_numbers_truncated: boolean;
    by_class: Record<string, number>;
  }> {
    const [countRow] = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT COUNT(*) AS count FROM ${FULL_TABLE} WHERE job_id = @job_id`,
      { job_id: jobId }
    );

    const count = Number(countRow?.count ?? 0);

    const lineRows = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT line_no FROM ${FULL_TABLE}
      WHERE job_id = @job_id
      ORDER BY line_no ASC
      LIMIT @limit`,
      { job_id: jobId, limit: lineNumbersLimit }
    );

    const classRows = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT failure_class, COUNT(*) AS count
      FROM ${FULL_TABLE}
      WHERE job_id = @job_id
      GROUP BY failure_class`,
      { job_id: jobId }
    );

    const by_class: Record<string, number> = {};
    for (const row of classRows)
    {
      by_class[row.failure_class as string] = Number(row.count ?? 0);
    }

    return {
      count,
      line_numbers: lineRows.map((r) => Number(r.line_no)),
      line_numbers_truncated: count > lineNumbersLimit,
      by_class,
    };
  }
}
