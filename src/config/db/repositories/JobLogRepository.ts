import { BigQueryManager, toDate } from "../BigQueryManager.js";
import type {
  JobLogAttributes,
  JobLogCreationAttributes,
} from "../models/JobLog.js";

const TABLE = "job_logs";

/**
 * BigQuery-backed repository for job_logs.
 */
export class JobLogRepository
{
  constructor() {}

  private fromRow(row: Record<string, unknown>): JobLogAttributes
  {
    return {
      id: Number(row.id ?? 0),
      job_id: row.job_id as string,
      event_type: row.event_type as string,
      stage: (row.stage as string | null) ?? null,
      template_id: (row.template_id as string | null) ?? null,
      message: (row.message as string | null) ?? null,
      metadata: (typeof row.metadata === "string" ? JSON.parse(row.metadata) : row.metadata) as Record<string, unknown> ?? {},
      created_at: toDate(row.created_at),
    };
  }

  /**
   * Records a single job log entry.
   */
  async log(data: JobLogCreationAttributes): Promise<JobLogAttributes>
  {
    const id = Date.now();
    const now = new Date();

    await BigQueryManager.getInstance().insertOne(TABLE, {
      id,
      job_id: data.job_id,
      event_type: data.event_type,
      stage: data.stage ?? null,
      template_id: data.template_id ?? null,
      message: data.message ?? null,
      metadata: data.metadata ?? {},
      created_at: now,
    });

    return this.findById(id);
  }

  /**
   * Finds a log entry by id.
   */
  private async findById(id: number): Promise<JobLogAttributes>
  {
    const row = await BigQueryManager.getInstance().queryOne<Record<string, unknown>>(TABLE, { id });
    return this.fromRow(row ?? {});
  }

  /**
   * Finds all logs for a job.
   */
  async findByJob(jobId: string): Promise<JobLogAttributes[]>
  {
    const rows = await BigQueryManager.getInstance().queryMany<Record<string, unknown>>(
      TABLE,
      { job_id: jobId },
      { column: "created_at", direction: "ASC" }
    );

    return rows.map((r) => this.fromRow(r));
  }
}
