import pg from "pg";
import Config from "../system-config/Config.js";
import ServiceManager from "../ServiceManager.js";
import { InstantiationError } from "../../errors/InstantiationError.js";

const { Pool } = pg;

class MySqlManager extends ServiceManager {
  protected static instance: MySqlManager;
  private _pool: pg.Pool | null = null;

  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate MySqlManager directly. Use getInstance()");
    }
    super(enforce);
  }

  public static getInstance(): MySqlManager {
    if (!ServiceManager.instance) {
      ServiceManager.instance = new MySqlManager(Enforce);
    }
    return ServiceManager.instance as MySqlManager;
  }

  private getPool(): pg.Pool {
    if (!this._pool) {
      const config = Config.getInstance();
      this._pool = new Pool({
        connectionString: config.settings.DATABASE_URL,
        max: 50,
        idleTimeoutMillis: 1200000,
        connectionTimeoutMillis: 30000,
      });
    }
    return this._pool;
  }

  public get pool(): pg.Pool {
    return this.getPool();
  }

  public async initialize(): Promise<void> {
    console.log("Initializing MySqlManager...");
    await this.waitForDb();
    console.log("MySqlManager initialized");
  }

  public async shutdown(): Promise<void> {
    console.log("Shutting down MySqlManager...");
    await this.pool.end();
  }

  public get sequelize() {
    // Placeholder for Sequelize ORM if needed
    return {
      sync: async (options: any) => {
        console.log("Database sync placeholder");
      }
    };
  }

  /**
   * Wait for database connection to succeed (Cloud SQL proxy race condition guard).
   * Retries with exponential backoff up to 300 seconds (5 minutes).
   */
  private async waitForDb(): Promise<void> {
    const maxAttempts = 60; // 60 * 5s = 300s max for cold starts and connection recovery
    let attempt = 0;
    while (attempt < maxAttempts) {
      try {
        const client = await this.pool.connect();
        await client.query("SELECT 1");
        client.release();
        return;
      } catch (err) {
        attempt++;
        const delay = Math.min(5000 * attempt, 10000); // 5s, 10s, 10s, ...
        if (attempt < maxAttempts) {
          await new Promise(r => setTimeout(r, delay));
        }
      }
    }
    throw new Error(`Database connection failed after ${maxAttempts} attempts`);
  }

  public async getJob(jobId: string): Promise<ParseJobRow | undefined> {
    const result = await this.pool.query<ParseJobRow>(
      "SELECT * FROM parse_jobs WHERE job_id = $1",
      [jobId]
    );
    return result.rows[0];
  }

  public async getBatchJobs(batchId: string): Promise<ParseJobRow[]> {
    const result = await this.pool.query<ParseJobRow>(
      "SELECT * FROM parse_jobs WHERE batch_id = $1",
      [batchId]
    );
    return result.rows;
  }

  public async getJobParts(jobId: string): Promise<OutputPartRow[]> {
    const result = await this.pool.query<OutputPartRow>(
      "SELECT * FROM output_parts WHERE job_id = $1",
      [jobId]
    );
    return result.rows;
  }

  public async createPendingArchiveEntry(
    jobId: string,
    entryName: string,
    entrySize: number
  ): Promise<void> {
    const { randomUUID } = await import("crypto");
    await this.pool.query(
      `INSERT INTO pending_archive_entries (id, job_id, entry_name, entry_size, status)
       VALUES ($1, $2, $3, $4, 'pending')
       ON CONFLICT (job_id, entry_name) DO NOTHING`,
      [randomUUID(), jobId, entryName, entrySize]
    );
  }

  public async markPendingEntryProcessing(
    jobId: string,
    entryName: string
  ): Promise<void> {
    await this.pool.query(
      `UPDATE pending_archive_entries 
       SET status = 'processing', updated_at = NOW() 
       WHERE job_id = $1 AND entry_name = $2`,
      [jobId, entryName]
    );
  }

  public async markPendingEntryCompleted(
    jobId: string,
    entryName: string
  ): Promise<void> {
    await this.pool.query(
      `UPDATE pending_archive_entries 
       SET status = 'completed', updated_at = NOW() 
       WHERE job_id = $1 AND entry_name = $2`,
      [jobId, entryName]
    );
  }

  public async markPendingEntryFailed(
    jobId: string,
    entryName: string,
    error: string
  ): Promise<void> {
    await this.pool.query(
      `UPDATE pending_archive_entries 
       SET status = 'failed', error = $3, updated_at = NOW() 
       WHERE job_id = $1 AND entry_name = $2`,
      [jobId, entryName, error]
    );
  }

  public async getPendingEntries(jobId: string): Promise<PendingArchiveEntryRow[]> {
    const result = await this.pool.query<PendingArchiveEntryRow>(
      "SELECT * FROM pending_archive_entries WHERE job_id = $1",
      [jobId]
    );
    return result.rows;
  }

  public async getPendingEntryCount(jobId: string): Promise<{ pending: number; completed: number; failed: number }> {
    const result = await this.pool.query(
      `SELECT 
         COUNT(*) FILTER (WHERE status = 'pending') as pending,
         COUNT(*) FILTER (WHERE status = 'completed') as completed,
         COUNT(*) FILTER (WHERE status = 'failed') as failed
       FROM pending_archive_entries 
       WHERE job_id = $1`,
      [jobId]
    );
    return result.rows[0];
  }

  public async getPendingEntryTotalSize(jobId: string): Promise<number> {
    const result = await this.pool.query(
      `SELECT COALESCE(SUM(entry_size), 0) as total_bytes
       FROM pending_archive_entries 
       WHERE job_id = $1 AND status IN ('completed', 'processing')`,
      [jobId]
    );
    return parseInt(result.rows[0].total_bytes, 10);
  }

  public async createTables(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS parse_jobs (
        job_id VARCHAR(36) PRIMARY KEY,
        batch_id VARCHAR(36),
        parent_job_id VARCHAR(36),
        source_type VARCHAR(32) NOT NULL,
        source_ref TEXT NOT NULL,
        s3_url TEXT,
        size BIGINT,
        field_spec JSONB NOT NULL,
        exec_path VARCHAR(16) NOT NULL DEFAULT 'stream',
        status VARCHAR(32) NOT NULL DEFAULT 'queued',
        output_paths JSONB NOT NULL DEFAULT '[]',
        counts JSONB NOT NULL DEFAULT '{}',
        timings JSONB NOT NULL DEFAULT '{}',
        error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS ix_parse_jobs_batch_id ON parse_jobs(batch_id);
      CREATE INDEX IF NOT EXISTS ix_parse_jobs_status ON parse_jobs(status);

      CREATE TABLE IF NOT EXISTS output_parts (
        part_id VARCHAR(36) PRIMARY KEY,
        job_id VARCHAR(36) NOT NULL,
        template_id VARCHAR(36) NOT NULL,
        s3_path TEXT NOT NULL,
        row_count INTEGER NOT NULL DEFAULT 0,
        byte_size BIGINT NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS ix_output_parts_job_id ON output_parts(job_id);

      CREATE TABLE IF NOT EXISTS rubbish_log (
        id BIGSERIAL PRIMARY KEY,
        job_id VARCHAR(36) NOT NULL,
        byte_offset BIGINT NOT NULL,
        line_no BIGINT NOT NULL,
        raw_bytes TEXT NOT NULL,
        matched_template_id VARCHAR(36) NOT NULL,
        logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS ix_rubbish_job_offset ON rubbish_log(job_id, byte_offset);

      CREATE TABLE IF NOT EXISTS dead_letters (
        dlq_id VARCHAR(36) PRIMARY KEY,
        job_id VARCHAR(36) NOT NULL,
        byte_offset BIGINT NOT NULL,
        byte_length INTEGER NOT NULL,
        line_no BIGINT NOT NULL,
        raw_bytes TEXT NOT NULL,
        failure_class VARCHAR(32) NOT NULL,
        error TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        status VARCHAR(16) NOT NULL DEFAULT 'pending',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS ix_dlq_job_offset ON dead_letters(job_id, byte_offset);
      CREATE INDEX IF NOT EXISTS ix_dlq_status ON dead_letters(status);

      -- Migration: drop old fixed-column schema if detected, recreate with dynamic JSONB schema
      DO $$ BEGIN
        IF EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_name = 'parsed_records' AND column_name = 'name'
        ) THEN
          DROP TABLE parsed_records;
        END IF;
      END $$;

      CREATE TABLE IF NOT EXISTS parsed_records (
        id BIGSERIAL PRIMARY KEY,
        _job_id VARCHAR(36) NOT NULL,
        _byte_offset BIGINT NOT NULL,
        _byte_length INTEGER NOT NULL,
        _record_index INTEGER NOT NULL,
        _line_no BIGINT NOT NULL,
        _template_id VARCHAR(36) NOT NULL,
        _template_version INTEGER NOT NULL,
        _checksum VARCHAR(64) NOT NULL,
        _parsed_at TIMESTAMPTZ NOT NULL,
        _part_id VARCHAR(36) NOT NULL,
        fields JSONB NOT NULL DEFAULT '{}',
        CONSTRAINT uq_parsed_record_job_offset UNIQUE (_job_id, _byte_offset)
      );
      CREATE INDEX IF NOT EXISTS ix_parsed_records_job_id ON parsed_records(_job_id);
      CREATE INDEX IF NOT EXISTS ix_parsed_records_fields ON parsed_records USING gin(fields);

      CREATE TABLE IF NOT EXISTS templates (
        template_id VARCHAR(36) PRIMARY KEY,
        fingerprint VARCHAR(64) NOT NULL UNIQUE,
        version INTEGER NOT NULL DEFAULT 1,
        kind VARCHAR(16) NOT NULL CHECK (kind IN ('record', 'rubbish')),
        field_map JSONB,
        structure TEXT,
        length_hint INTEGER,
        signature TEXT,
        confidence NUMERIC,
        source VARCHAR(16) NOT NULL CHECK (source IN ('ai', 'bootstrap', 'user')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS ix_templates_kind ON templates(kind);
      CREATE INDEX IF NOT EXISTS ix_templates_fingerprint ON templates(fingerprint);

      CREATE TABLE IF NOT EXISTS pending_archive_entries (
        id VARCHAR(36) PRIMARY KEY,
        job_id VARCHAR(36) NOT NULL,
        entry_name TEXT NOT NULL,
        entry_size BIGINT NOT NULL,
        status VARCHAR(16) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
        error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (job_id, entry_name)
      );
      CREATE INDEX IF NOT EXISTS ix_pending_entries_job_id ON pending_archive_entries(job_id);
      CREATE INDEX IF NOT EXISTS ix_pending_entries_status ON pending_archive_entries(status);
    `);
  }
}

function Enforce(): void {}

export interface ParseJobRow {
  job_id: string;
  batch_id?: string;
  parent_job_id?: string;
  source_type: string;
  source_ref: string;
  s3_url?: string;
  size?: number;
  field_spec: any; // PostgreSQL JSONB is parsed automatically by pg
  exec_path: string;
  status: string;
  output_paths: any; // PostgreSQL JSONB is parsed automatically by pg
  counts: Record<string, any>;
  timings: Record<string, any>;
  error?: string;
  created_at: Date;
  updated_at: Date;
}

export interface OutputPartRow {
  part_id: string;
  job_id: string;
  template_id: string;
  s3_path: string;
  row_count: number;
  byte_size: number;
  created_at: Date;
}

export interface DeadLetterRow {
  dlq_id: string;
  job_id: string;
  byte_offset: number;
  byte_length: number;
  line_no: number;
  raw_bytes: string;
  failure_class: string;
  error: string;
  attempts: number;
  status: string;
  created_at: Date;
  updated_at: Date;
}

export interface PendingArchiveEntryRow {
  id: string;
  job_id: string;
  entry_name: string;
  entry_size: number;
  status: string;
  error?: string;
  created_at: Date;
  updated_at: Date;
}

export default MySqlManager;

export interface ParseJobRow {
  job_id: string;
  batch_id?: string;
  parent_job_id?: string;
  source_type: string;
  source_ref: string;
  s3_url?: string;
  size?: number;
  field_spec: any; // PostgreSQL JSONB is parsed automatically by pg
  exec_path: string;
  status: string;
  output_paths: any; // PostgreSQL JSONB is parsed automatically by pg
  counts: Record<string, any>;
  timings: Record<string, any>;
  error?: string;
  created_at: Date;
  updated_at: Date;
}

export interface OutputPartRow {
  part_id: string;
  job_id: string;
  template_id: string;
  s3_path: string;
  row_count: number;
  byte_size: number;
  created_at: Date;
}

export interface DeadLetterRow {
  dlq_id: string;
  job_id: string;
  byte_offset: number;
  byte_length: number;
  line_no: number;
  raw_bytes: string;
  failure_class: string;
  error: string;
  attempts: number;
  status: string;
  created_at: Date;
  updated_at: Date;
}

export interface PendingArchiveEntryRow {
  id: string;
  job_id: string;
  entry_name: string;
  entry_size: number;
  status: string;
  error?: string;
  created_at: Date;
  updated_at: Date;
}
