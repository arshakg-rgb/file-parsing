import dotenv from "dotenv";
import path from "path";

dotenv.config({ path: path.resolve(".env.local") });
dotenv.config({ path: path.resolve("../file-parsing-pipeline/.env.local") });

/**
 * Gets number
 * @param name - The name value
 * @param fallback - The fallback
 * @returns The numeric result
 */
function getNumber(name: string, fallback: number): number {
  const v = process.env[name];
  return v !== undefined ? Number(v) : fallback;
}

/**
 * Gets string
 * @param name - The name value
 * @param fallback - The fallback
 * @returns The string result
 */
function getString(name: string, fallback: string): string {
  return process.env[name] ?? fallback;
}

/**
 * Gets optional string
 * @param name - The name value
 * @returns The string | undefined result
 */
function getOptionalString(name: string): string | undefined {
  const v = process.env[name];
  return v === "" ? undefined : v;
}

/**
 * Application settings
 */
export const settings = {
  GCP_PROJECT_ID: getString("GCP_PROJECT_ID", "data-etl-499916"),
  GOOGLE_APPLICATION_CREDENTIALS: getOptionalString("GOOGLE_APPLICATION_CREDENTIALS"),
  BIGQUERY_PROJECT_ID: getString("BIGQUERY_PROJECT_ID", "data-etl-499916"),
  BIGQUERY_DATASET: getString("BIGQUERY_DATASET", "file_parsing"),
  BIGQUERY_LOCATION: getString("BIGQUERY_LOCATION", "us-central1"),
  DATA_BUCKET: getString("DATA_BUCKET", "datalead-osint"),
  DEPLOYMENT_BUCKET: getString("DEPLOYMENT_BUCKET", "datalead-osint"),

  QUEUE_BACKEND: getString("QUEUE_BACKEND", "pubsub"),

  INGEST_QUEUE_URL: getString("INGEST_QUEUE_URL", "fpp-ingest"),
  CLASSIFY_QUEUE_URL: getString("CLASSIFY_QUEUE_URL", "fpp-classify"),
  PARSE_QUEUE_URL: getString("PARSE_QUEUE_URL", "fpp-parse"),
  DLQ_QUEUE_URL: getString("DLQ_QUEUE_URL", "fpp-line-dlq"),
  LOAD_QUEUE_URL: getString("LOAD_QUEUE_URL", "fpp-load"),
  LOAD_QUEUE_URL_SMALL: getString("LOAD_QUEUE_URL_SMALL", "fpp-load-small"),
  LOAD_QUEUE_URL_LARGE: getString("LOAD_QUEUE_URL_LARGE", "fpp-load-large"),
  REPORT_QUEUE_URL: getString("REPORT_QUEUE_URL", "fpp-report"),
  JOB_EVENTS_QUEUE_URL: getString("JOB_EVENTS_QUEUE_URL", "fpp-job-events"),
  ARCHIVE_ENTRY_QUEUE_URL: getString("ARCHIVE_ENTRY_QUEUE_URL", "fpp-archive-entry"),

  FILE_DATABASE_URL: getString(
    "FILE_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/parsing_pipeline"
  ),

  FIRESTORE_DATABASE_ID: getString("FIRESTORE_DATABASE_ID", "file-parsing-db"),
  TEMPLATE_COLLECTION: getString("TEMPLATE_COLLECTION", "file-parsing-templates"),

  ANTHROPIC_API_KEY: getString("ANTHROPIC_API_KEY", ""),
  ANTHROPIC_MODEL: getString("ANTHROPIC_MODEL", "claude-3-sonnet-20240229"),
  BEDROCK_MODEL_ID: getString("BEDROCK_MODEL_ID", "mock"),
  AI_CLASSIFIER_URL: getOptionalString("AI_CLASSIFIER_URL"),
  AI_RATE_LIMIT_RPM: getNumber("AI_RATE_LIMIT_RPM", 60), // 60 requests per minute
  AI_RATE_LIMIT_BURST: getNumber("AI_RATE_LIMIT_BURST", 10), // Burst of 10 requests
  AI_CLASSIFY_TIMEOUT_MS: getNumber("AI_CLASSIFY_TIMEOUT_MS", 10000), // 10 s per AI probe call
  AI_INLINE_MODE: getString("AI_INLINE_MODE", "off"),
  MAX_AI_CALLS_PER_JOB: getNumber("MAX_AI_CALLS_PER_JOB", 10),

  VERTEX_MODEL: getString("VERTEX_MODEL", "gemini-2.5-flash"),
  VERTEX_LOCATION: getString("VERTEX_LOCATION", "us-central1"),

  LOKI_HOST: getOptionalString("LOKI_HOST"),
  LOKI_USERNAME: getOptionalString("LOKI_USERNAME"),
  LOKI_PASSWORD: getOptionalString("LOKI_PASSWORD"),

  FETCH_CHUNK_SIZE: getNumber("FETCH_CHUNK_SIZE", 32 * 1024 * 1024),
  MAX_QUOTED_NEWLINES: getNumber("MAX_QUOTED_NEWLINES", 0),
  MAX_LINE_BYTES: getNumber("MAX_LINE_BYTES", 1024 * 1024),
  SMALL_FILE_SINGLE_GET_THRESHOLD: getNumber(
    "SMALL_FILE_SINGLE_GET_THRESHOLD",
    128 * 1024 * 1024
  ),
  RAM_FLUSH_WATERMARK: getNumber("RAM_FLUSH_WATERMARK", 1536 * 1024 * 1024),
  MAX_MERGED_PART_BYTES: getNumber("MAX_MERGED_PART_BYTES", 1024 * 1024 * 1024),
  RUBBISH_CONFIDENCE_MIN: getNumber("RUBBISH_CONFIDENCE_MIN", 0.9),
  MATCH_RATE_FLOOR: getNumber("MATCH_RATE_FLOOR", 0.1),
  MATCH_RATE_WINDOW: getNumber("MATCH_RATE_WINDOW", 1000),

  PROBE_WINDOW_MIN_BYTES: getNumber("PROBE_WINDOW_MIN_BYTES", 64 * 1024),
  PROBE_WINDOW_MAX_BYTES: getNumber("PROBE_WINDOW_MAX_BYTES", 1024 * 1024),
  PROBE_TARGET_LINES: getNumber("PROBE_TARGET_LINES", 150),
  PROBE_COUNT_MIN: getNumber("PROBE_COUNT_MIN", 5),
  PROBE_COUNT_MAX: getNumber("PROBE_COUNT_MAX", 24),
  PROBE_SIZE_PER_COUNT: getNumber("PROBE_SIZE_PER_COUNT", 512 * 1024 * 1024),

  FAILED_LINE_RATIO_THRESHOLD: getNumber("FAILED_LINE_RATIO_THRESHOLD", 0.05),

  ARCHIVE_MAX_COMPRESSION_RATIO: getNumber("ARCHIVE_MAX_COMPRESSION_RATIO", 100),
  ARCHIVE_MAX_NESTING_DEPTH: getNumber("ARCHIVE_MAX_NESTING_DEPTH", 3),
  ARCHIVE_MAX_UNCOMPRESSED_BYTES: getNumber(
    "ARCHIVE_MAX_UNCOMPRESSED_BYTES",
    10 * 1024 * 1024 * 1024
  ),
  ARCHIVE_MAX_ENTRIES: getNumber("ARCHIVE_MAX_ENTRIES", 10000),

  ARCHIVE_PASSWORD_MAX_ATTEMPTS: getNumber("ARCHIVE_PASSWORD_MAX_ATTEMPTS", 3),

  LARGE_FILE_THRESHOLD_BYTES: getNumber("LARGE_FILE_THRESHOLD_BYTES", 500 * 1024 * 1024), // 500MB threshold for async extraction

  ALLOWED_FETCH_SIZE_BYTES: getNumber(
    "ALLOWED_FETCH_SIZE_BYTES",
    5 * 1024 * 1024 * 1024
  ),
  FETCH_TIMEOUT_SECONDS: getNumber("FETCH_TIMEOUT_SECONDS", 600),
  QUEUE_TIMEOUT_SECONDS: getNumber("QUEUE_TIMEOUT_SECONDS", 60),
  GCS_TIMEOUT_SECONDS: getNumber("GCS_TIMEOUT_SECONDS", 1200),

  RETRY_IMMEDIATE_DELAY_SECONDS: getNumber("RETRY_IMMEDIATE_DELAY_SECONDS", 0),
  RETRY_DELAYED_DELAY_SECONDS: getNumber("RETRY_DELAYED_DELAY_SECONDS", 300),
  RETRY_MAX_ATTEMPTS: getNumber("RETRY_MAX_ATTEMPTS", 2),
  STREAM_PARSER_CONCURRENCY: getNumber("STREAM_PARSER_CONCURRENCY", 3),
  STREAM_PARSER_MEMORY_SOFT_LIMIT_MB: getNumber("STREAM_PARSER_MEMORY_SOFT_LIMIT_MB", 0),
  PARSE_DB_FLUSH_BATCH_SIZE: getNumber("PARSE_DB_FLUSH_BATCH_SIZE", 2000),
  PARSE_BG_FLUSH_QUEUE_DEPTH: getNumber("PARSE_BG_FLUSH_QUEUE_DEPTH", 4),
  QUEUE_CONCURRENCY: getNumber("QUEUE_CONCURRENCY", 3),
  QUEUE_MEMORY_SOFT_LIMIT_MB: getNumber("QUEUE_MEMORY_SOFT_LIMIT_MB", 0),

  GCS_UPLOAD_CONCURRENCY: getNumber("GCS_UPLOAD_CONCURRENCY", 8),

  OUTPUT_BUFFER_FLUSH_THRESHOLD_ROWS: getNumber("OUTPUT_BUFFER_FLUSH_THRESHOLD_ROWS", 50000),
  OUTPUT_BUFFER_FLUSH_THRESHOLD_BYTES: getNumber("OUTPUT_BUFFER_FLUSH_THRESHOLD_BYTES", 64 * 1024 * 1024),
  OUTPUT_BUFFER_MAX_PENDING_FLUSHES: getNumber("OUTPUT_BUFFER_MAX_PENDING_FLUSHES", 4),
};

export type Settings = typeof settings;
