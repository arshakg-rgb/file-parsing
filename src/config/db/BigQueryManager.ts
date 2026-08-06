import pino from "pino";
import { BigQuery, Dataset, Table } from "@google-cloud/bigquery";
import { settings } from "@shared/Settings.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import ServiceManager from "../ServiceManager.js";

/**
 * BigQueryManager is a singleton responsible for all BigQuery client access.
 * It replaces DatabaseManager as the database layer for the full-BigQuery
 * migration. Tables are expected to exist in the configured dataset; this
 * manager does not run schema migrations.
 */
export class BigQueryManager extends ServiceManager
{
  protected static instance: BigQueryManager;

  private readonly client: BigQuery;
  private readonly dataset: Dataset;
  private readonly logger: pino.Logger;

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
      this.logger.error({ table: tableName, errors: insertErrors }, "BigQuery insert errors");
      throw new Error(`BigQuery insert failed for ${tableName}: ${JSON.stringify(insertErrors)}`);
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

  public async gracefulStop(): Promise<void>
  {
    // BigQuery client is stateless; nothing to close.
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

function Enforce(): void {}
