import { BigQueryManager } from "../BigQueryManager.js";
import { settings } from "@shared/Settings.js";
import type { DatabaseModels } from "../models/index.js";
import type {
  JobLogAttributes,
  JobLogCreationAttributes,
} from "../models/JobLog.js";

const TABLE = "job_logs";
const FULL_TABLE = `\`${settings.BIGQUERY_PROJECT_ID}.${settings.BIGQUERY_DATASET}.${TABLE}\``;

/**
 * BigQuery-backed repository for job_logs.
 */
export class JobLogRepository
{
  constructor(private models: DatabaseModels) {}

  private fromRow(row: Record<string, unknown>): JobLogAttributes
  {
    return {
      id: Number(row.id ?? 0),
      job_id: row.job_id as string,
      event_type: row.event_type as string,
      stage: (row.stage as string | null) ?? null,
      template_id: (row.template_id as string | null) ?? null,
      message: (row.message as string | null) ?? null,
      metadata: (row.metadata ? JSON.parse(row.metadata as string) : {}) as Record<string, unknown>,
      created_at: new Date(row.created_at as string),
    };
  }

  /**
   * Records a single job log entry.
   */
  async log(data: JobLogCreationAttributes): Promise<JobLogAttributes>
  {
    const id = Date.now();
    const now = new Date();

    await BigQueryManager.getInstance().execute(
      `INSERT INTO ${FULL_TABLE} (
        id, job_id, event_type, stage, template_id, message, metadata, created_at
      ) VALUES (
        @id, @job_id, @event_type, @stage, @template_id, @message, @metadata, @created_at
      )`,
      {
        id,
        job_id: data.job_id,
        event_type: data.event_type,
        stage: data.stage ?? null,
        template_id: data.template_id ?? null,
        message: data.message ?? null,
        metadata: JSON.stringify(data.metadata ?? {}),
        created_at: now,
      }
    );

    return this.findById(id);
  }

  /**
   * Finds a log entry by id.
   */
  private async findById(id: number): Promise<JobLogAttributes>
  {
    const [row] = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE} WHERE id = @id LIMIT 1`,
      { id }
    );

    return this.fromRow(row);
  }

  /**
   * Finds all log entries for a job, oldest first.
   */
  async findByJob(jobId: string): Promise<JobLogAttributes[]>
  {
    const rows = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE} WHERE job_id = @job_id ORDER BY created_at ASC`,
      { job_id: jobId }
    );

    return rows.map((r) => this.fromRow(r));
  }
}
