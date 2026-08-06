import { BigQueryManager, toDate } from "../BigQueryManager.js";
import { settings } from "@shared/Settings.js";
import type {
  OutputPartAttributes,
  OutputPartCreationAttributes,
} from "../models/OutputPart.js";

const TABLE = "output_parts";
const FULL_TABLE = `\`${settings.BIGQUERY_PROJECT_ID}.${settings.BIGQUERY_DATASET}.${TABLE}\``;

/**
 * BigQuery-backed repository for output_parts.
 */
export class OutputPartRepository
{
  constructor() {}

  private fromRow(row: Record<string, unknown>): OutputPartAttributes
  {
    return {
      part_id: row.part_id as string,
      job_id: row.job_id as string,
      template_id: row.template_id as string,
      s3_path: row.s3_path as string,
      row_count: Number(row.row_count ?? 0),
      byte_size: Number(row.byte_size ?? 0),
      created_at: toDate(row.created_at),
    };
  }

  /**
   * Finds all output parts for a job.
   */
  async findByJob(jobId: string): Promise<OutputPartAttributes[]>
  {
    const rows = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE} WHERE job_id = @job_id ORDER BY created_at DESC`,
      { job_id: jobId }
    );

    return rows.map((r) => this.fromRow(r));
  }

  /**
   * Finds a single output part by id.
   */
  private async findById(partId: string): Promise<OutputPartAttributes | null>
  {
    const [row] = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT * FROM ${FULL_TABLE} WHERE part_id = @part_id LIMIT 1`,
      { part_id: partId }
    );

    return row ? this.fromRow(row) : null;
  }

  /**
   * Creates or finds an output part.
   */
  async create(data: OutputPartCreationAttributes): Promise<OutputPartAttributes | null>
  {
    const existing = await this.findById(data.part_id);
    if (existing)
    {
      return existing;
    }

    const now = new Date();

    await BigQueryManager.getInstance().execute(
      `INSERT INTO ${FULL_TABLE} (
        part_id, job_id, template_id, s3_path, row_count, byte_size, created_at
      ) VALUES (
        @part_id, @job_id, @template_id, @s3_path, @row_count, @byte_size, @created_at
      )`,
      {
        part_id: data.part_id,
        job_id: data.job_id,
        template_id: data.template_id,
        s3_path: data.s3_path,
        row_count: data.row_count,
        byte_size: data.byte_size,
        created_at: now,
      }
    );

    return this.findById(data.part_id);
  }
}
