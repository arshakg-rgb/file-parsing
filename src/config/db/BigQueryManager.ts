import { randomUUID } from "node:crypto";
import pino from "pino";
import { BigQuery, Dataset, Table, Job } from "@google-cloud/bigquery";
import { settings } from "@shared/Settings.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import ServiceManager from "../ServiceManager.js";

/**
 * BigQueryManager is a singleton responsible for all BigQuery client access.
 * It replaces DatabaseManager as the database layer for the full-BigQuery
 * migration. Tables are expected to exist in the configured dataset; this
 * manager does not run schema migrations.
 */
interface TableSchemaField {
  name: string;
  type: string;
  mode: string;
}

export class BigQueryManager extends ServiceManager
{
  protected static instance: BigQueryManager;

  private readonly client: BigQuery;
  private readonly dataset: Dataset;
  private readonly logger: pino.Logger;
  private schemaCache: Record<string, TableSchemaField[]> = {};

  protected constructor(enforce: () => void)
  {
    super(enforce);

    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Cannot instantiate BigQueryManager directly. Use getInstance()");
    }

    this.logger = pino({ name: "BigQueryManager" });
    this.client = new BigQuery({
      projectId: settings.BIGQUERY_PROJECT_ID,
    });
    this.dataset = this.client.dataset(settings.BIGQUERY_DATASET);
  }

  public static getInstance(): BigQueryManager
  {
    if (!BigQueryManager.instance)
    {
      BigQueryManager.instance = new BigQueryManager(Enforce);
    }

    return BigQueryManager.instance;
  }

  /**
   * Verifies that the configured dataset exists.
   */
  public async initialize(): Promise<void>
  {
    const [exists] = await this.dataset.exists();

    if (!exists)
    {
      throw new Error(`BigQuery dataset '${settings.BIGQUERY_DATASET}' does not exist in project '${settings.BIGQUERY_PROJECT_ID}'`);
    }

    this.logger.info({ dataset: settings.BIGQUERY_DATASET, project: settings.BIGQUERY_PROJECT_ID, location: settings.BIGQUERY_LOCATION }, "BigQueryManager initialized");
  }

  /**
   * Runs a parameterized BigQuery SQL query and returns typed rows.
   *
   * BigQuery cannot infer a parameter's type when its value is null, so any
   * null-valued parameter must have an explicit entry in `types`.
   */
  public async query<T = unknown>(sql: string, params?: unknown[] | Record<string, unknown>, types?: Record<string, string>): Promise<T[]>
  {
    const [rows] = await this.client.query({
      query: sql,
      params,
      types: types && Object.keys(types).length ? types : undefined,
      location: settings.BIGQUERY_LOCATION,
      useLegacySql: false,
    });

    return rows as unknown as T[];
  }

  /**
   * Returns a Table reference for the given name inside the configured dataset.
   */
  public table(name: string): Table
  {
    return this.dataset.table(name);
  }

  /**
   * Streaming-inserts rows into a BigQuery table. Throws on any invalid rows.
   */
  public async insert(tableName: string, rows: Record<string, unknown>[]): Promise<void>
  {
    if (rows.length === 0)
    {
      return;
    }

    const [response] = await this.table(tableName).insert(rows, {
      raw: true,
      skipInvalidRows: false,
    });

    const insertErrors = (response as { insertErrors?: unknown[] }).insertErrors;

    if (insertErrors?.length)
    {
      const errorSummary = `BigQuery insert failed for ${tableName}: ${JSON.stringify(insertErrors)}`;
      this.logger.error({ table: tableName, errors: insertErrors }, errorSummary);
      throw new Error(errorSummary);
    }

    this.logger.debug({ table: tableName, count: rows.length }, "BigQuery insert succeeded");
  }

  /**
   * Runs a DML statement (INSERT, UPDATE, DELETE) and returns the number of
   * affected rows. Uses createQueryJob so the job is executed synchronously.
   */
  public async execute(sql: string, params?: Record<string, unknown>, types?: Record<string, string>): Promise<number>
  {
    const [job] = await this.client.createQueryJob({
      query: sql,
      params,
      types: types && Object.keys(types).length ? types : undefined,
      location: settings.BIGQUERY_LOCATION,
      useLegacySql: false,
    });

    await job.getQueryResults();
    const rawMetadata = await job.getMetadata();
    const metadata = Array.isArray(rawMetadata) ? rawMetadata[0] : rawMetadata;

    return Number((metadata as { statistics?: { query?: { numDmlAffectedRows?: string | number } } }).statistics?.query?.numDmlAffectedRows ?? 0);
  }

  public async queryOne<T = Record<string, unknown>>(tableName: string, where: Record<string, unknown>): Promise<T | null>
  {
    const [row] = await this.query<T>(
      `SELECT * FROM ${this.fullTableName(tableName)} WHERE ${this.whereClause(Object.keys(where))} LIMIT 1`,
      where,
      await this.inferTypes(where, tableName)
    );

    return row ?? null;
  }

  public async queryMany<T = Record<string, unknown>>(
    tableName: string,
    where: Record<string, unknown> = {},
    orderBy?: { column: string; direction?: "ASC" | "DESC" },
    limit?: number
  ): Promise<T[]>
  {
    let sql = `SELECT * FROM ${this.fullTableName(tableName)}`;
    const types = await this.inferTypes(where, tableName);

    if (Object.keys(where).length)
    {
      sql += ` WHERE ${this.whereClause(Object.keys(where))}`;
    }

    if (orderBy)
    {
      sql += ` ORDER BY ${orderBy.column} ${orderBy.direction ?? "ASC"}`;
    }

    if (limit !== undefined)
    {
      sql += " LIMIT @limit";
    }

    return this.query<T>(sql, { ...where, ...(limit !== undefined ? { limit } : {}) }, types);
  }

  public async insertOne(tableName: string, data: Record<string, unknown>): Promise<number>
  {
    const columns = Object.keys(data);
    const placeholders = columns.map((c) => `@${c}`).join(", ");
    const sql = `INSERT INTO ${this.fullTableName(tableName)} (${columns.join(", ")}) VALUES (${placeholders})`;

    return this.execute(sql, data, await this.inferTypes(data, tableName));
  }

  public async updateOne(tableName: string, data: Record<string, unknown>, where: Record<string, unknown>): Promise<number>
  {
    const setParts = Object.keys(data).map((c) => `${c} = @${c}`);
    const whereParts = Object.keys(where).map((c) => `${c} = @where_${c}`);
    const params: Record<string, unknown> = { ...data };

    for (const key of Object.keys(where))
    {
      params[`where_${key}`] = where[key];
    }

    const sql = `UPDATE ${this.fullTableName(tableName)} SET ${setParts.join(", ")} WHERE ${whereParts.join(" AND ")}`;

    return this.execute(sql, params, await this.inferTypes(params, tableName));
  }

  public async deleteOne(tableName: string, where: Record<string, unknown>): Promise<number>
  {
    const whereParts = Object.keys(where).map((c) => `${c} = @where_${c}`);
    const params: Record<string, unknown> = {};

    for (const key of Object.keys(where))
    {
      params[`where_${key}`] = where[key];
    }

    const sql = `DELETE FROM ${this.fullTableName(tableName)} WHERE ${whereParts.join(" AND ")}`;

    return this.execute(sql, params, await this.inferTypes(params, tableName));
  }

  private fullTableName(tableName: string): string
  {
    return `\`${settings.BIGQUERY_PROJECT_ID}.${settings.BIGQUERY_DATASET}.${tableName}\``;
  }

  private whereClause(columns: string[]): string
  {
    return columns.map((c) => `${c} = @${c}`).join(" AND ");
  }

  public async getTableSchema(tableName: string): Promise<TableSchemaField[]>
  {
    const cached = this.schemaCache[tableName];
    if (cached)
    {
      return cached;
    }

    const table = this.dataset.table(tableName);
    const [metadata] = await table.getMetadata();
    const fields = (metadata.schema?.fields ?? []) as TableSchemaField[];

    this.schemaCache[tableName] = fields;
    return fields;
  }

  /**
   * Builds a BigQuery `types` map for params that are null or JSON-typed,
   * inferring types from the live table schema. This removes the need for
   * each repository to maintain hand-written NULLABLE_TYPES maps.
   */
  public async inferTypes(params: Record<string, unknown>, tableName: string): Promise<Record<string, string> | undefined>
  {
    const fields = await this.getTableSchema(tableName);

    const types: Record<string, string> = {};

    for (const key of Object.keys(params))
    {
      const field = fields.find((f) => f.name === key);
      if (!field)
      {
        continue;
      }

      const value = params[key];
      if (value === null || field.type === "JSON")
      {
        types[key] = this.mapBigQueryType(field.type);
      }
    }

    return Object.keys(types).length ? types : undefined;
  }

  private mapBigQueryType(type: string): string
  {
    switch (type.toUpperCase())
    {
      case "STRING":
        return "STRING";
      case "INT64":
      case "INTEGER":
        return "INT64";
      case "FLOAT64":
      case "FLOAT":
        return "FLOAT64";
      case "BOOL":
      case "BOOLEAN":
        return "BOOL";
      case "TIMESTAMP":
        return "TIMESTAMP";
      case "DATETIME":
        return "DATETIME";
      case "DATE":
        return "DATE";
      case "TIME":
        return "TIME";
      case "JSON":
        return "JSON";
      case "BYTES":
        return "BYTES";
      case "NUMERIC":
        return "NUMERIC";
      case "BIGNUMERIC":
        return "BIGNUMERIC";
      case "GEOGRAPHY":
        return "GEOGRAPHY";
      case "STRUCT":
      case "RECORD":
        return "STRUCT";
      case "ARRAY":
        return "ARRAY<STRING>";
      default:
        return "STRING";
    }
  }

  public async gracefulStop(): Promise<void>
  {
    // BigQuery client is stateless; nothing to close.
  }

  /**
   * Bulk-loads a Parquet file from GCS into a temporary staging table, then
   * uses a DML INSERT ... SELECT to cast the STRING `fields` column into the
   * target `JSON` column of `parsed_records`.
   */
  public async bulkLoadFromGcs(gcsUris: string[]): Promise<number>
  {
    if (!gcsUris.length)
    {
      return 0;
    }

    const stagingName = `parsed_records_staging_${randomUUID().replace(/-/g, "_")}`;
    const stagingFull = this.fullTableName(stagingName);
    const targetFull = this.fullTableName("parsed_records");

    this.logger.info({ sources: gcsUris, staging: stagingName }, "bigquery_bulk_load_start");

    const [job] = await this.client.createJob({
      configuration: {
        load: {
          sourceUris: gcsUris,
          destinationTable: {
            projectId: settings.BIGQUERY_PROJECT_ID,
            datasetId: settings.BIGQUERY_DATASET,
            tableId: stagingName,
          },
          sourceFormat: "PARQUET",
          autodetect: true,
          writeDisposition: "WRITE_TRUNCATE",
        },
      },
    });

    const metadata = await this.waitForJob(job);
    const outputRows = Number((metadata as { statistics?: { load?: { outputRows?: string | number } } }).statistics?.load?.outputRows ?? 0);
    this.logger.info({ sources: gcsUris, staging: stagingName, outputRows }, "bigquery_staging_loaded");

    const insertSql = `
      INSERT INTO ${targetFull}
        (id, _job_id, _byte_offset, _byte_length, _record_index, _line_no, _template_id, _template_version, _checksum, _parsed_at, _part_id, fields)
      SELECT
        id,
        _job_id,
        _byte_offset,
        _byte_length,
        _record_index,
        _line_no,
        _template_id,
        _template_version,
        _checksum,
        _parsed_at,
        _part_id,
        PARSE_JSON(fields) AS fields
      FROM ${stagingFull}
    `;

    this.logger.info({ sources: gcsUris, staging: stagingName }, "bigquery_insert_start");
    const inserted = await this.execute(insertSql);
    this.logger.info({ sources: gcsUris, staging: stagingName, inserted }, "bigquery_insert_complete");

    this.logger.info({ sources: gcsUris, staging: stagingName }, "bigquery_drop_staging_start");
    await this.client.query({
      query: `DROP TABLE IF EXISTS ${stagingFull}`,
      useLegacySql: false,
      location: settings.BIGQUERY_LOCATION,
    });

    this.logger.info({ sources: gcsUris, staging: stagingName, outputRows, inserted }, "bigquery_bulk_load_complete");
    return inserted;
  }

  private async waitForJob(job: Job): Promise<unknown>
  {
    while (true)
    {
      const [metadata] = await job.getMetadata();
      const status = (metadata as { status?: { state?: string; errorResult?: unknown } }).status;

      if (status?.state === "DONE")
      {
        if (status.errorResult)
        {
          const { reason, message } = status.errorResult as { reason?: string; message?: string };
          const detail = message ?? JSON.stringify(status.errorResult, (key, value) => typeof value === "bigint" ? value.toString() : value);
          throw new Error(`BigQuery load job failed [${reason ?? "unknown"}]: ${detail}`);
        }
        return metadata;
      }

      await new Promise((r) => setTimeout(r, 500));
    }
  }
}

/**
 * Builds a BigQuery `types` map for any parameter whose value is null.
 * BigQuery requires an explicit type for null-valued query parameters since
 * it cannot infer one. Only keys present in `params` with a null value and a
 * matching entry in `typeMap` are included.
 */
export function paramTypes(params: Record<string, unknown>, typeMap: Record<string, string>): Record<string, string> | undefined
{
  const types: Record<string, string> = {};

  for (const key of Object.keys(params))
  {
    if (params[key] === null && typeMap[key])
    {
      types[key] = typeMap[key];
    }
  }

  return Object.keys(types).length ? types : undefined;
}

/**
 * Converts a BigQuery row timestamp value into a JavaScript Date.
 * BigQuery's client can return DATE/TIMESTAMP/DATETIME fields as
 * BigQueryTimestamp / BigQueryDate / BigQueryDatetime wrapper objects,
 * plain strings, numbers, or already-instantiated Date objects.
 */
export function toDate(value: unknown): Date
{
  if (value instanceof Date && !isNaN(value.getTime()))
  {
    return value;
  }

  if (value === null || value === undefined || value === "")
  {
    return new Date();
  }

  const raw: unknown = (typeof value === "object" && value !== null && "value" in value)
    ? (value as { value: unknown }).value
    : value;

  const date = new Date(raw as string | number | Date);
  if (isNaN(date.getTime()))
  {
    return new Date();
  }

  return date;
}

function Enforce(): void {}
