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

export class PasswordStateRepository {
  private bq(): BigQueryManager {
    return BigQueryManager.getInstance();
  }

  /**
   * Ensures the password_state table exists (no-op if it already exists).
   */
  async ensureTable(): Promise<void> {
    await this.bq().query(`
      CREATE TABLE IF NOT EXISTS ${FULL_TABLE} (
        job_id STRING NOT NULL,
        password STRING,
        attempts INT64 DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
      )
    `);
  }

  /**
   * Fetches the current password state for a job.
   */
  async get(jobId: string): Promise<{ password: string | null; attempts: number }> {
    const row = await this.bq().queryOne<Record<string, unknown>>(TABLE, { job_id: jobId });
    if (!row) {
      return { password: null, attempts: 0 };
    }
    return {
      password: (row.password as string) || null,
      attempts: Number(row.attempts ?? 0),
    };
  }

  /**
   * Stores or updates the provided password for a job.
   */
  async setPassword(jobId: string, password: string): Promise<void> {
    const now = new Date();
    const existing = await this.get(jobId);
    if (existing.password === null && existing.attempts === 0) {
      await this.bq().insertOne(TABLE, {
        job_id: jobId,
        password,
        attempts: existing.attempts,
        created_at: now,
        updated_at: now,
      });
    } else {
      await this.bq().execute(
        `UPDATE ${FULL_TABLE} SET password = @password, updated_at = @updated_at WHERE job_id = @job_id`,
        { job_id: jobId, password, updated_at: now },
        await this.bq().inferTypes({ job_id: jobId, password, updated_at: now }, TABLE)
      );
    }
  }

  /**
   * Increments the password attempt counter and returns the new value.
   */
  async incrementAttempts(jobId: string): Promise<number> {
    const now = new Date();
    const existing = await this.get(jobId);
    const attempts = existing.attempts + 1;
    if (existing.password === null && existing.attempts === 0) {
      await this.bq().insertOne(TABLE, {
        job_id: jobId,
        password: null,
        attempts,
        created_at: now,
        updated_at: now,
      });
    } else {
      await this.bq().execute(
        `UPDATE ${FULL_TABLE} SET attempts = @attempts, updated_at = @updated_at WHERE job_id = @job_id`,
        { job_id: jobId, attempts, updated_at: now },
        await this.bq().inferTypes({ job_id: jobId, attempts, updated_at: now }, TABLE)
      );
    }
    return attempts;
  }

  /**
   * Removes the password state for a job.
   */
  async delete(jobId: string): Promise<void> {
    await this.bq().execute(`DELETE FROM ${FULL_TABLE} WHERE job_id = @job_id`, { job_id: jobId });
  }
}
