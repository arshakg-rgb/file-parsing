import { BigQueryManager, toDate } from "../BigQueryManager.js";
import { settings } from "@shared/Settings.js";
import type {
  ParsedRecordAttributes,
  ParsedRecordCreationAttributes,
} from "../models/ParsedRecord.js";

const TABLE = "parsed_records";
const FULL_TABLE = `\`${settings.BIGQUERY_PROJECT_ID}.${settings.BIGQUERY_DATASET}.${TABLE}\``;

/**
 * BigQuery-backed repository for parsed_records.
 */
export class ParsedRecordRepository
{
  constructor() {}

  private fromRow(row: Record<string, unknown>): ParsedRecordAttributes
  {
    return {
      id: Number(row.id ?? 0),
      _job_id: row._job_id as string,
      _byte_offset: Number(row._byte_offset ?? 0),
      _byte_length: Number(row._byte_length ?? 0),
      _record_index: Number(row._record_index ?? 0),
      _line_no: Number(row._line_no ?? 0),
      _template_id: row._template_id as string,
      _template_version: Number(row._template_version ?? 1),
      _checksum: row._checksum as string,
      _parsed_at: toDate(row._parsed_at),
      _part_id: row._part_id as string,
      fields: (typeof row.fields === "string" ? JSON.parse(row.fields) : row.fields) as Record<string, unknown> ?? {},
    };
  }

  /**
   * Creates a single parsed record.
   */
  async create(data: ParsedRecordCreationAttributes): Promise<ParsedRecordAttributes | null>
  {
    const id = Date.now();

    try
    {
      await BigQueryManager.getInstance().insertOne(TABLE, {
        id,
        _job_id: data._job_id,
        _byte_offset: data._byte_offset,
        _byte_length: data._byte_length,
        _record_index: data._record_index,
        _line_no: data._line_no,
        _template_id: data._template_id,
        _template_version: data._template_version,
        _checksum: data._checksum,
        _parsed_at: data._parsed_at,
        _part_id: data._part_id,
        fields: data.fields ?? {},
      });
    }
    catch
    {
      return null;
    }

    const row = await BigQueryManager.getInstance().queryOne<Record<string, unknown>>(TABLE, { id });
    return row ? this.fromRow(row) : null;
  }

  /**
   * Streaming-inserts rows into the parsed_records table.
   */
  async bulkCreate(rows: ParsedRecordCreationAttributes[], _ignoreDuplicates = true): Promise<void>
  {
    const bqRows = rows.map((r, i) => ({
      ...r,
      id: (r as { id?: number }).id ?? Date.now() + i,
      _parsed_at: r._parsed_at,
      fields: JSON.stringify(r.fields ?? {}),
    })) as Record<string, unknown>[];

    await BigQueryManager.getInstance().insert(TABLE, bqRows);
  }

  /**
   * Finds all parsed records for a job.
   */
  async findByJob(jobId: string): Promise<ParsedRecordAttributes[]>
  {
    const rows = await BigQueryManager.getInstance().queryMany<Record<string, unknown>>(
      TABLE,
      { _job_id: jobId },
      { column: "_byte_offset", direction: "ASC" }
    );

    return rows.map((r) => this.fromRow(r));
  }

  /**
   * Checks if a record exists at a given byte offset for a job.
   */
  async exists(jobId: string, byteOffset: number): Promise<boolean>
  {
    const [row] = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT 1 AS one FROM ${FULL_TABLE} WHERE _job_id = @_job_id AND _byte_offset = @_byte_offset LIMIT 1`,
      { _job_id: jobId, _byte_offset: byteOffset }
    );

    return !!row;
  }

  /**
   * Counts parsed records for a job.
   */
  async countByJob(jobId: string): Promise<number>
  {
    const [row] = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT COUNT(*) AS count FROM ${FULL_TABLE} WHERE _job_id = @_job_id`,
      { _job_id: jobId }
    );

    return Number(row?.count ?? 0);
  }

  /**
   * Returns how many rows each record template parsed for a job.
   */
  async getTemplateUsageCounts(jobId: string): Promise<{ template_id: string; count: number }[]>
  {
    const rows = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT _template_id AS template_id, COUNT(*) AS count
      FROM ${FULL_TABLE}
      WHERE _job_id = @_job_id
      GROUP BY _template_id`,
      { _job_id: jobId }
    );

    return rows.map((r) => ({
      template_id: r.template_id as string,
      count: Number(r.count ?? 0),
    }));
  }
}
