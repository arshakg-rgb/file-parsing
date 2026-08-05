-- Description: Rename parse_jobs status values to user-facing names

UPDATE parse_jobs
SET status = CASE status
  WHEN 'queued' THEN 'Created'
  WHEN 'ingesting' THEN 'Ingesting'
  WHEN 'awaiting_password' THEN 'Needs Password'
  WHEN 'detecting' THEN 'Detecting'
  WHEN 'parsing' THEN 'Parsing'
  WHEN 'finalizing' THEN 'Merging Output'
  WHEN 'loading' THEN 'Saving to Database'
  WHEN 'reporting' THEN 'Reporting'
  WHEN 'done' THEN 'Completed'
  WHEN 'partial' THEN 'Partial'
  WHEN 'held' THEN 'On Hold'
  WHEN 'failed' THEN 'Failed'
  ELSE status
END
WHERE status IN (
  'queued', 'ingesting', 'awaiting_password', 'detecting', 'parsing',
  'finalizing', 'loading', 'reporting', 'done', 'partial', 'held', 'failed'
);
