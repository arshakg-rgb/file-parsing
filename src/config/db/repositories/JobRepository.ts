import { BigQueryManager } from "../BigQueryManager.js";
import { settings } from "@shared/Settings.js";
import type {
  ParseJobAttributes,
  ParseJobCreationAttributes,
} from "../models/ParseJob.js";
import type { JobCounts, JobTimings } from "@shared/models/job.js";

const TABLE = "parse_jobs";
const FULL_TABLE = `\`${settings.BIGQUERY_PROJECT_ID}.${settings.BIGQUERY_DATASET}.${TABLE}\``;

function toJson(value: unknown): string
{
  if (value === undefined || value === null)
  {
    return "{}";
  }

  return JSON.stringify(value);
}

function fromJson<T>(value: unknown): T | undefined
{
  if (value === undefined || value === null || value === "")
  {
    return undefined;
  }

  try
  {
    return JSON.parse(value as string) as T;
  }
  catch
  {
    return undefined;
  }
}

function toDate(value: unknown): Date
{
  if (!value)
  {
    return new Date();
  }

  return new Date(value as string | number | Date);
}

function fromRow(row: Record<string, unknown>): ParseJobAttributes
{
  return {
    job_id: row.job_id as string,
    batch_id: (row.batch_id as string | null) ?? null,
    parent_job_id: (row.parent_job_id as string | null) ?? null,
    source_type: row.source_type as string,
    source_ref: row.source_ref as string,
    s3_url: (row.s3_url as string | null) ?? null,
    size: (row.size as number | null) ?? null,
    field_spec: fromJson<string[]>(row.field_spec) ?? [],
    exec_path: row.exec_path as string,
    status: row.status as string,
    output_paths: fromJson<string[]>(row.output_paths) ?? [],
    counts: fromJson<JobCounts>(row.counts) ?? { parsed: 0, dropped_rubbish: 0, dlq_count: 0, failed_by_class: {} },
    timings: fromJson<JobTimings>(row.timings) ?? {},
    error: (row.error as string | null) ?? null,
    created_at: toDate(row.created_at),
    updated_at: toDate(row.updated_at),
  };
}

function serializeField(key: string, value: unknown): unknown
{
  if (["field_spec", "output_paths", "counts", "timings"].includes(key))
  {
    return toJson(value);
  }

  return value;
}

/**
 * BigQuery-backed repository for parse_jobs.
 *
 * This replaces the previous Sequelize implementation. The table is expected
 * to exist in the configured dataset with the JSON columns stored as STRING.
 */
export class JobRepository
{
  constructor() {}

  private bq(): BigQueryManager
  {
    return BigQueryManager.getInstance();
  }

  /**
   * Finds a job by id.
   */
  async findById(jobId: string, _options?: { attributes?: (keyof ParseJobAttributes)[] }): Promise<ParseJobAttributes | null>
  {
    const rows = await this.bq().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE} WHERE job_id = @job_id LIMIT 1`,
      { job_id: jobId }
    );

    return rows[0] ? fromRow(rows[0]) : null;
  }

  /**
   * Finds all jobs in a batch.
   */
  async findByBatchId(batchId: string): Promise<ParseJobAttributes[]>
  {
    const rows = await this.bq().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE} WHERE batch_id = @batch_id ORDER BY created_at DESC`,
      { batch_id: batchId }
    );

    return rows.map(fromRow);
  }

  /**
   * Finds all jobs, optionally filtered by statuses.
   */
  async findAll(statuses?: string[]): Promise<ParseJobAttributes[]>
  {
    const where = statuses && statuses.length > 0
      ? "WHERE status IN UNNEST(@statuses)"
      : "";

    const rows = await this.bq().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE} ${where} ORDER BY created_at DESC`,
      statuses && statuses.length > 0 ? { statuses } : undefined
    );

    return rows.map(fromRow);
  }

  /**
   * Creates a new job row.
   */
  async create(data: ParseJobCreationAttributes): Promise<ParseJobAttributes>
  {
    const now = new Date();

    const params: Record<string, unknown> = {
      job_id: data.job_id,
      batch_id: data.batch_id ?? null,
      parent_job_id: data.parent_job_id ?? null,
      source_type: data.source_type,
      source_ref: data.source_ref,
      s3_url: data.s3_url ?? null,
      size: data.size ?? null,
      field_spec: toJson(data.field_spec),
      exec_path: data.exec_path,
      status: data.status,
      output_paths: toJson(data.output_paths),
      counts: toJson(data.counts),
      timings: toJson(data.timings),
      error: data.error ?? null,
      created_at: now,
      updated_at: now,
    };

    await this.bq().execute(
      `INSERT INTO ${FULL_TABLE} (
        job_id, batch_id, parent_job_id, source_type, source_ref, s3_url, size,
        field_spec, exec_path, status, output_paths, counts, timings, error,
        created_at, updated_at
      ) VALUES (
        @job_id, @batch_id, @parent_job_id, @source_type, @source_ref, @s3_url, @size,
        @field_spec, @exec_path, @status, @output_paths, @counts, @timings, @error,
        @created_at, @updated_at
      )`,
      params
    );

    const created = await this.findById(data.job_id);
    return created as ParseJobAttributes;
  }

  /**
   * Updates arbitrary fields on a job.
   */
  async updateFields(jobId: string, fields: Partial<ParseJobAttributes>): Promise<void>
  {
    const setParts: string[] = ["updated_at = CURRENT_TIMESTAMP()"];
    const params: Record<string, unknown> = { job_id: jobId };

    for (const [key, value] of Object.entries(fields))
    {
      if (value === undefined || key === "updated_at" || key === "created_at" || key === "job_id")
      {
        continue;
      }

      params[key] = serializeField(key, value);
      setParts.push(`${key} = @${key}`);
    }

    if (setParts.length === 1)
    {
      return;
    }

    await this.bq().execute(
      `UPDATE ${FULL_TABLE} SET ${setParts.join(", ")} WHERE job_id = @job_id`,
      params
    );
  }

  /**
   * Gets the job status.
   */
  async getStatus(jobId: string): Promise<string | undefined>
  {
    const row = await this.findById(jobId, { attributes: ["status"] });
    return row?.status;
  }

  /**
   * Atomically transitions status from an allowed set of source statuses.
   */
  async tryTransitionStatus(
    jobId: string,
    newStatus: string,
    allowedFromStatuses: string[],
    extraFields: Partial<ParseJobAttributes> = {}
  ): Promise<boolean>
  {
    const row = await this.findById(jobId);

    if (!row || !allowedFromStatuses.includes(row.status))
    {
      return false;
    }

    const setParts: string[] = ["status = @new_status", "updated_at = CURRENT_TIMESTAMP()"];
    const params: Record<string, unknown> = {
      job_id: jobId,
      new_status: newStatus,
      allowed_from_statuses: allowedFromStatuses,
    };

    for (const [key, value] of Object.entries(extraFields))
    {
      if (value === undefined || key === "job_id" || key === "status" || key === "updated_at" || key === "created_at")
      {
        continue;
      }

      if (key === "timings")
      {
        const merged = { ...(row.timings || {}), ...(value as JobTimings || {}) };
        params[key] = toJson(merged);
      }
      else
      {
        params[key] = serializeField(key, value);
      }

      setParts.push(`${key} = @${key}`);
    }

    const affected = await this.bq().execute(
      `UPDATE ${FULL_TABLE} SET ${setParts.join(", ")} WHERE job_id = @job_id AND status IN UNNEST(@allowed_from_statuses)`,
      params
    );

    return affected > 0;
  }

  /**
   * Gets the field spec for a job.
   */
  async getFieldSpec(jobId: string): Promise<string[]>
  {
    const row = await this.findById(jobId, { attributes: ["field_spec"] });
    return row?.field_spec || [];
  }

  /**
   * Updates s3 url and size.
   */
  async updateS3Url(jobId: string, s3Url: string, size: number): Promise<void>
  {
    await this.updateFields(jobId, { s3_url: s3Url, size });
  }

  /**
   * Marks a job as failed.
   */
  async markFailed(jobId: string, reason: string): Promise<void>
  {
    const job = await this.findById(jobId);

    if (!job)
    {
      return;
    }

    const timings = { ...(job.timings || {}), failed_at: new Date().toISOString() };
    await this.updateFields(jobId, { status: "failed", error: reason, timings });
  }

  /**
   * Holds a job.
   */
  async hold(jobId: string, reason?: string): Promise<void>
  {
    const existing = await this.findById(jobId, { attributes: ["error"] });
    await this.updateFields(jobId, { status: "held", error: reason || existing?.error });
  }

  /**
   * Finds jobs that are not in a terminal status and have not updated recently.
   */
  async findStuckJobs(thresholdMinutes: number): Promise<ParseJobAttributes[]>
  {
    const rows = await this.bq().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE}
       WHERE status NOT IN UNNEST(@terminal_statuses)
         AND updated_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @threshold_minutes MINUTE)`,
      {
        terminal_statuses: ["done", "failed", "partial", "held"],
        threshold_minutes: thresholdMinutes,
      }
    );

    return rows.map(fromRow);
  }

  /**
   * Finds jobs stuck in the ingesting status.
   */
  async findStuckIngesting(hours = 2): Promise<ParseJobAttributes[]>
  {
    const rows = await this.bq().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE}
       WHERE status = 'ingesting'
         AND updated_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)`,
      { hours }
    );

    return rows.map(fromRow);
  }

  /**
   * Returns stats for a batch.
   */
  async getBatchStats(batchId: string): Promise<{
    totalJobs: number;
    passedJobs: number;
    heldJobs: number;
    failedJobs: number;
  }> {
    const [row] = await this.bq().query<Record<string, unknown>>(
      `SELECT
        COUNT(*) AS total,
        COUNTIF(status = 'done') AS passed,
        COUNTIF(status = 'held') AS held,
        COUNTIF(status = 'failed') AS failed
      FROM ${FULL_TABLE}
      WHERE batch_id = @batch_id`,
      { batch_id: batchId }
    );

    return {
      totalJobs: Number(row?.total ?? 0),
      passedJobs: Number(row?.passed ?? 0),
      heldJobs: Number(row?.held ?? 0),
      failedJobs: Number(row?.failed ?? 0),
    };
  }

  /**
   * Returns the counts object for a job.
   */
  async getCounts(jobId: string): Promise<JobCounts>
  {
    const row = await this.findById(jobId, { attributes: ["counts"] });
    return row?.counts || { parsed: 0, dropped_rubbish: 0, dlq_count: 0, failed_by_class: {} };
  }
}
