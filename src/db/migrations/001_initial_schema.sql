-- Migration 001: Initial schema
-- Description: Create parse_jobs, output_parts, rubbish_log, and dead_letters tables

CREATE TABLE IF NOT EXISTS parse_jobs (
  job_id VARCHAR(36) PRIMARY KEY,
  batch_id VARCHAR(36),
  parent_job_id VARCHAR(36),
  source_type VARCHAR(50) NOT NULL,
  source_ref TEXT NOT NULL,
  s3_url TEXT,
  size BIGINT,
  field_spec TEXT[] NOT NULL,
  exec_path VARCHAR(20) DEFAULT 'stream',
  status VARCHAR(50) NOT NULL DEFAULT 'pending',
  output_paths TEXT[] DEFAULT '{}',
  counts JSONB DEFAULT '{}',
  timings JSONB DEFAULT '{}',
  error TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_parse_jobs_batch_id ON parse_jobs(batch_id);
CREATE INDEX IF NOT EXISTS idx_parse_jobs_parent_job_id ON parse_jobs(parent_job_id);
CREATE INDEX IF NOT EXISTS idx_parse_jobs_status ON parse_jobs(status);
CREATE INDEX IF NOT EXISTS idx_parse_jobs_created_at ON parse_jobs(created_at);

CREATE TABLE IF NOT EXISTS output_parts (
  part_id VARCHAR(36) PRIMARY KEY,
  job_id VARCHAR(36) NOT NULL REFERENCES parse_jobs(job_id) ON DELETE CASCADE,
  template_id VARCHAR(100) NOT NULL,
  s3_path TEXT NOT NULL,
  row_count BIGINT NOT NULL,
  byte_size BIGINT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_output_parts_job_id ON output_parts(job_id);
CREATE INDEX IF NOT EXISTS idx_output_parts_template_id ON output_parts(template_id);

CREATE TABLE IF NOT EXISTS rubbish_log (
  log_id VARCHAR(36) PRIMARY KEY,
  job_id VARCHAR(36) NOT NULL REFERENCES parse_jobs(job_id) ON DELETE CASCADE,
  byte_offset BIGINT NOT NULL,
  byte_length BIGINT NOT NULL,
  line_no BIGINT,
  raw_line TEXT NOT NULL,
  failure_class VARCHAR(50),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rubbish_log_job_id ON rubbish_log(job_id);
CREATE INDEX IF NOT EXISTS idx_rubbish_log_byte_offset ON rubbish_log(byte_offset);

CREATE TABLE IF NOT EXISTS dead_letters (
  dlq_id VARCHAR(36) PRIMARY KEY,
  job_id VARCHAR(36) NOT NULL REFERENCES parse_jobs(job_id) ON DELETE CASCADE,
  byte_offset BIGINT NOT NULL,
  byte_length BIGINT NOT NULL,
  line_no BIGINT,
  raw_bytes TEXT NOT NULL,
  failure_class VARCHAR(50) NOT NULL,
  attempts INTEGER DEFAULT 0,
  status VARCHAR(20) DEFAULT 'pending',
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_job_id ON dead_letters(job_id);
CREATE INDEX IF NOT EXISTS idx_dead_letters_status ON dead_letters(status);
CREATE INDEX IF NOT EXISTS idx_dead_letters_dlq_id ON dead_letters(dlq_id);

-- Migration tracking table
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  description TEXT
);
