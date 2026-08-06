import { BigQueryManager } from "../BigQueryManager.js";
import { settings } from "@shared/Settings.js";
import type {
  SchemaMigrationAttributes,
  SchemaMigrationCreationAttributes,
} from "../models/SchemaMigration.js";

const TABLE = "schema_migrations";
const FULL_TABLE = `\`${settings.BIGQUERY_PROJECT_ID}.${settings.BIGQUERY_DATASET}.${TABLE}\``;

/**
 * BigQuery-backed repository for schema_migrations.
 *
 * The ensureTable() method is a no-op because BigQuery tables are created
 * externally; the other methods support the lightweight version tracking that
 * the rest of the app expects.
 */
export class SchemaMigrationRepository
{
  constructor() {}

  /**
   * No-op in BigQuery; tables must be created in the console.
   */
  async ensureTable(): Promise<void>
  {
    // BigQuery tables are managed out-of-band.
  }

  /**
   * Gets the list of applied migration versions.
   */
  async getAppliedVersions(): Promise<number[]>
  {
    const rows = await BigQueryManager.getInstance().query<Record<string, unknown>>(
      `SELECT version FROM ${FULL_TABLE} ORDER BY version ASC`
    );

    return rows.map((r) => Number(r.version));
  }

  /**
   * Adds a new applied migration version.
   */
  async addVersion(version: number, description?: string): Promise<void>
  {
    const params = { version, description: description ?? null };

    await BigQueryManager.getInstance().execute(
      `INSERT INTO ${FULL_TABLE} (version, applied_at, description)
      VALUES (@version, CURRENT_TIMESTAMP(), @description)`,
      params,
      await BigQueryManager.getInstance().inferTypes(params, TABLE)
    );
  }
}
