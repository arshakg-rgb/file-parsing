import { BigQueryManager, toDate } from "../BigQueryManager.js";
import { settings } from "@shared/Settings.js";
import type {
  RubbishLogAttributes,
  RubbishLogCreationAttributes,
} from "../models/RubbishLog.js";

const TABLE = "rubbish_log";
const FULL_TABLE = `\`${settings.BIGQUERY_PROJECT_ID}.${settings.BIGQUERY_DATASET}.${TABLE}\``;

/**
 * BigQuery-backed repository for rubbish_log.
 */
export class RubbishLogRepository
{
  constructor() {}

  private fromRow(row: Record<string, unknown>): RubbishLogAttributes
  {
    return {
      id: Number(row.id ?? 0),
      job_id: row.job_id as string,
      byte_offset: Number(row.byte_offset ?? 0),
      line_no: Number(row.line_no ?? 0),
      raw_bytes: row.raw_bytes as string,
      matched_template_id: row.matched_template_id as string,
      logged_at: toDate(row.logged_at),
    };
  }

  /**
   * Creates a single rubbish log.
   */
  async create(data: RubbishLogCreationAttributes): Promise<RubbishLogAttributes>
  {
    const id = Date.now();

    await BigQueryManager.getInstance().insertOne(TABLE, {
      id,
      job_id: data.job_id,
      byte_offset: data.byte_offset,
      line_no: data.line_no,
      raw_bytes: data.raw_bytes,
      matched_template_id: data.matched_template_id,
      logged_at: new Date(),
    });

    const row = await BigQueryManager.getInstance().queryOne<Record<string, unknown>>(TABLE, { id });
    return this.fromRow(row as Record<string, unknown>);
  }

  /**
   * Streaming-inserts rows into the rubbish_log table.
   */
  async bulkCreate(rows: RubbishLogCreationAttributes[]): Promise<void>
  {
    const bqRows = rows.map((r, i) => ({
      ...r,
      id: (r as { id?: number }).id ?? Date.now() + i,
      logged_at: (r as { logged_at?: Date }).logged_at ?? new Date(),
    })) as Record<string, unknown>[];

    await BigQueryManager.getInstance().insert(TABLE, bqRows);
  }

  /**
   * Finds all rubbish logs for a job.
   */
  async findByJob(jobId: string): Promise<RubbishLogAttributes[]>
  {
    const rows = await BigQueryManager.getInstance().queryMany<Record<string, unknown>>(
      TABLE,
      { job_id: jobId },
      { column: "byte_offset", direction: "ASC" }
    );

    return rows.map((r) => this.fromRow(r));
  }

  /**
   * Counts rubbish logs for a job.
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
   * Summarizes rubbish logs for a job.
   */
  async getSummaryByJob(jobId: string, lineNumbersLimit = 500): Promise<{
    count: number;
    line_numbers: number[];
    line_numbers_truncated: boolean;
    by_template: Record<string, number>;
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

    const templateRows = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT matched_template_id, COUNT(*) AS count
      FROM ${FULL_TABLE}
      WHERE job_id = @job_id
      GROUP BY matched_template_id`,
      { job_id: jobId }
    );

    const by_template: Record<string, number> = {};
    for (const row of templateRows)
    {
      by_template[row.matched_template_id as string] = Number(row.count ?? 0);
    }

    return {
      count,
      line_numbers: lineRows.map((r) => Number(r.line_no)),
      line_numbers_truncated: count > lineNumbersLimit,
      by_template,
    };
  }
}
