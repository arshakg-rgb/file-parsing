-- Description: Rename parse_jobs timings keys to match new user-facing status names

UPDATE parse_jobs
SET timings = timings
  - 'queued_at'
  - 'awaiting_password_at'
  - 'finalizing_at'
  - 'loading_at'
  - 'done_at'
  - 'held_at'
  || jsonb_build_object(
       'created_at', timings->'queued_at',
       'needs_password_at', timings->'awaiting_password_at',
       'merging_output_at', timings->'finalizing_at',
       'saving_to_database_at', timings->'loading_at',
       'completed_at', timings->'done_at',
       'on_hold_at', timings->'held_at'
     )
WHERE timings IS NOT NULL;
