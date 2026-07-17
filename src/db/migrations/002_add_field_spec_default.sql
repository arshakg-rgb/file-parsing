-- Migration 002: Add default value for field_spec
-- Description: Add DEFAULT '[]' to field_spec column to prevent null constraint violations
-- Note: field_spec is jsonb type in production, not text[]

ALTER TABLE parse_jobs ALTER COLUMN field_spec SET DEFAULT '[]'::jsonb;

INSERT INTO schema_migrations (version, description) VALUES (2, 'Add default value for field_spec column');
