import pg from "pg";
import { settings } from "./config.js";

const { Pool } = pg;

export const pool = new Pool({
  connectionString: settings.DATABASE_URL,
  max: 15,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

export interface ParseJobRow {
  job_id: string;
  batch_id?: string;
  parent_job_id?: string;
  source_type: string;
  source_ref: string;
  s3_url?: string;
  size?: number;
  field_spec: string[];
  exec_path: string;
  status: string;
  output_paths: string[];
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

export async function getJob(jobId: string): Promise<ParseJobRow | undefined> {
  const result = await pool.query<ParseJobRow>(
    "SELECT * FROM parse_jobs WHERE job_id = $1",
    [jobId]
  );
  return result.rows[0];
}

export async function getBatchJobs(batchId: string): Promise<ParseJobRow[]> {
  const result = await pool.query<ParseJobRow>(
    "SELECT * FROM parse_jobs WHERE batch_id = $1",
    [batchId]
  );
  return result.rows;
}

export async function getJobParts(jobId: string): Promise<OutputPartRow[]> {
  const result = await pool.query<OutputPartRow>(
    "SELECT * FROM output_parts WHERE job_id = $1",
    [jobId]
  );
  return result.rows;
}

export async function createTables(): Promise<void> {
  await pool.query(`
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
  `);
}
