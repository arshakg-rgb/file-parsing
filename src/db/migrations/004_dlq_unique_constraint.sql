-- Migration 004: Add unique constraint to dead_letters to prevent duplicate entries on restart
-- Description: Add unique constraint on (job_id, line_no) to prevent duplicate DLQ entries when jobs are killed and restarted during Cloud Rollout

ALTER TABLE dead_letters DROP CONSTRAINT IF EXISTS dead_letters_job_id_line_no_key;
ALTER TABLE dead_letters ADD CONSTRAINT dead_letters_job_id_line_no_key UNIQUE (job_id, line_no);
