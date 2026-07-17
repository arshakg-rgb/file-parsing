-- Migration 003: Add unique constraint to pending_archive_entries
-- Description: Add UNIQUE (job_id, entry_name) to prevent duplicate pending entries
-- This prevents duplicate inserts when jobs are retried due to Cloud Rollout SIGTERM

-- First, clean up existing duplicates (keep the most recent row for each job_id, entry_name)
DELETE FROM pending_archive_entries a
USING pending_archive_entries b
WHERE a.id < b.id
  AND a.job_id = b.job_id
  AND a.entry_name = b.entry_name;

-- Then add the unique constraint
ALTER TABLE pending_archive_entries ADD CONSTRAINT unique_job_entry UNIQUE (job_id, entry_name);

INSERT INTO schema_migrations (version, description) VALUES (3, 'Add unique constraint to pending_archive_entries');
