import { BigQueryManager } from "../BigQueryManager.js";
import { settings } from "@shared/Settings.js";

const TABLE = "password_state";
const FULL_TABLE = `\`${settings.BIGQUERY_PROJECT_ID}.${settings.BIGQUERY_DATASET}.${TABLE}\``;

export interface IPasswordState {
  job_id: string;
  password: string | null;
  attempts: number;
  created_at: Date;
  updated_at: Date;
}

/**
 * Password state is stored as an append-only log of (job_id, password,
 * attempts) rows in BigQuery. Writes use streaming insert instead of DML
 * UPDATE/INSERT so they do not count toward the "table update operations"
 * quota that was causing ingest startup failures.
 */
export class PasswordStateRepository {
  private bq(): BigQueryManager {
    return BigQueryManager.getInstance();
  }

  /**
   * Fetches the current password state for a job (most recent by updated_at).
   */
  async get(jobId: string): Promise<{ password: string | null; attempts: number }> {
    const rows = await this.bq().queryMany<Record<string, unknown>>(
      TABLE,
      { job_id: jobId },
      { column: "updated_at", direction: "DESC" },
      1
    );
    const row = rows[0];
    if (!row) {
      return { password: null, attempts: 0 };
    }
    return {
      password: (row.password as string) || null,
      attempts: Number(row.attempts ?? 0),
    };
  }

  private now(): Date {
    return new Date();
  }

  /**
   * Stores a provided password. Writes a new row; never UPDATEs in place.
   */
  async setPassword(jobId: string, password: string): Promise<void> {
    const existing = await this.get(jobId);
    const now = this.now();
    await this.bq().insert(TABLE, [
      {
        job_id: jobId,
        password,
        attempts: existing.attempts,
        created_at: now,
        updated_at: now,
      },
    ]);
  }

  /**
   * Increments the password attempt counter. Writes a new row; never UPDATEs.
   */
  async incrementAttempts(jobId: string): Promise<number> {
    const existing = await this.get(jobId);
    const attempts = existing.attempts + 1;
    const now = this.now();
    await this.bq().insert(TABLE, [
      {
        job_id: jobId,
        password: existing.password,
        attempts,
        created_at: now,
        updated_at: now,
      },
    ]);
    return attempts;
  }

  /**
   * Removes the password state for a job. Kept for compatibility; DML delete
   * is still a table update, but it is only called on job completion/failure.
   */
  async delete(jobId: string): Promise<void> {
    await this.bq().execute(`DELETE FROM ${FULL_TABLE} WHERE job_id = @job_id`, { job_id: jobId });
  }
}
