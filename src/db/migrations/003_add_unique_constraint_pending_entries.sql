-- Migration 003: Add unique constraint to pending_archive_entries
-- Description: Add UNIQUE (job_id, entry_name) to prevent duplicate pending entries
-- This prevents duplicate inserts when jobs are retried due to Cloud Rollout SIGTERM

ALTER TABLE pending_archive_entries ADD CONSTRAINT unique_job_entry UNIQUE (job_id, entry_name);

INSERT INTO schema_migrations (version, description) VALUES (3, 'Add unique constraint to pending_archive_entries');
