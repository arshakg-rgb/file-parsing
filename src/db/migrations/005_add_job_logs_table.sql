-- Migration 005: Add job_logs table
-- Description: Add job_logs audit trail table (crashes, template usage, drop/DLQ counts per job)

CREATE TABLE IF NOT EXISTS job_logs (
  id BIGSERIAL PRIMARY KEY,
  job_id VARCHAR(36) NOT NULL REFERENCES parse_jobs(job_id) ON DELETE CASCADE,
  event_type VARCHAR(32) NOT NULL,
  stage VARCHAR(32),
  template_id VARCHAR(36),
  message TEXT,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);
CREATE INDEX IF NOT EXISTS idx_job_logs_event_type ON job_logs(event_type);
