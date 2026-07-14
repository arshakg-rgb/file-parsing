import dotenv from "dotenv";
import path from "path";

// Load local .env.local first (project-specific overrides take priority)
dotenv.config({ path: path.resolve(".env.local") });
// Fall back to sibling Python project's env for shared infra settings
dotenv.config({ path: path.resolve("../file-parsing-pipeline/.env.local") });

function getNumber(name: string, fallback: number): number {
  const v = process.env[name];
  return v !== undefined ? Number(v) : fallback;
}

function getString(name: string, fallback: string): string {
  return process.env[name] ?? fallback;
}

function getOptionalString(name: string): string | undefined {
  const v = process.env[name];
  return v === "" ? undefined : v;
}

export const settings = {
  // ---- Google Cloud Platform ----
  GCP_PROJECT_ID: getString("GCP_PROJECT_ID", "data-etl-499916"),
  GOOGLE_APPLICATION_CREDENTIALS: getOptionalString("GOOGLE_APPLICATION_CREDENTIALS"),
  DATA_BUCKET: getString("DATA_BUCKET", "datalead-osint"),
  DEPLOYMENT_BUCKET: getString("DEPLOYMENT_BUCKET", "datalead-osint"),

  // ---- Queue backend: "pubsub" (GCP production) or "sqs" (LocalStack dev) ----
  QUEUE_BACKEND: getString("QUEUE_BACKEND", "sqs"),

  // ---- Pub/Sub Topics / SQS Queue URLs ----
  // For pubsub backend: short topic name (e.g. "fpp-ingest").
  // For sqs backend:    full SQS URL (e.g. "http://localhost:4566/...").
  INGEST_QUEUE_URL: getString("INGEST_QUEUE_URL", "fpp-ingest"),
  CLASSIFY_QUEUE_URL: getString("CLASSIFY_QUEUE_URL", "fpp-classify"),
  PARSE_QUEUE_URL: getString("PARSE_QUEUE_URL", "fpp-parse"),
  DLQ_QUEUE_URL: getString("DLQ_QUEUE_URL", "fpp-line-dlq"),
  LOAD_QUEUE_URL: getString("LOAD_QUEUE_URL", "fpp-load"),
  REPORT_QUEUE_URL: getString("REPORT_QUEUE_URL", "fpp-report"),
  JOB_EVENTS_QUEUE_URL: getString("JOB_EVENTS_QUEUE_URL", "fpp-job-events"),

  // ---- Postgres (Jobs DB) ----
  DATABASE_URL: getString(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/parsing_pipeline"
  ),

  // ---- Firestore (Template Registry) ----
  FIRESTORE_DATABASE_ID: getString("FIRESTORE_DATABASE_ID", "osint-fdb"),
  TEMPLATE_COLLECTION: getString("TEMPLATE_COLLECTION", "file-parsing-templates"),

  // ---- AI Classifier ----
  BEDROCK_MODEL_ID: getString("BEDROCK_MODEL_ID", "mock"),
  AI_CLASSIFIER_URL: getOptionalString("AI_CLASSIFIER_URL"),

  // ---- Stream Parser tuning ----
  FETCH_CHUNK_SIZE: getNumber("FETCH_CHUNK_SIZE", 8 * 1024 * 1024),
  SMALL_FILE_SINGLE_GET_THRESHOLD: getNumber(
    "SMALL_FILE_SINGLE_GET_THRESHOLD",
    128 * 1024 * 1024
  ),
  RAM_FLUSH_WATERMARK: getNumber("RAM_FLUSH_WATERMARK", 256 * 1024 * 1024),
  MAX_MERGED_PART_BYTES: getNumber("MAX_MERGED_PART_BYTES", 64 * 1024 * 1024),
  RUBBISH_CONFIDENCE_MIN: getNumber("RUBBISH_CONFIDENCE_MIN", 0.9),
  MATCH_RATE_FLOOR: getNumber("MATCH_RATE_FLOOR", 0.1),
  MATCH_RATE_WINDOW: getNumber("MATCH_RATE_WINDOW", 1000),

  // ---- Detect / Bootstrap ----
  PROBE_WINDOW_MIN_BYTES: getNumber("PROBE_WINDOW_MIN_BYTES", 64 * 1024),
  PROBE_WINDOW_MAX_BYTES: getNumber("PROBE_WINDOW_MAX_BYTES", 1 * 1024 * 1024),
  PROBE_TARGET_LINES: getNumber("PROBE_TARGET_LINES", 150),
  PROBE_COUNT_MIN: getNumber("PROBE_COUNT_MIN", 5),
  PROBE_COUNT_MAX: getNumber("PROBE_COUNT_MAX", 24),
  PROBE_SIZE_PER_COUNT: getNumber("PROBE_SIZE_PER_COUNT", 512 * 1024 * 1024),

  // ---- Quality gate ----
  FAILED_LINE_RATIO_THRESHOLD: getNumber("FAILED_LINE_RATIO_THRESHOLD", 0.05),

  // ---- Archive bomb guards ----
  ARCHIVE_MAX_COMPRESSION_RATIO: getNumber("ARCHIVE_MAX_COMPRESSION_RATIO", 100),
  ARCHIVE_MAX_NESTING_DEPTH: getNumber("ARCHIVE_MAX_NESTING_DEPTH", 1),
  ARCHIVE_MAX_UNCOMPRESSED_BYTES: getNumber(
    "ARCHIVE_MAX_UNCOMPRESSED_BYTES",
    10 * 1024 * 1024 * 1024
  ),
  ARCHIVE_MAX_ENTRIES: getNumber("ARCHIVE_MAX_ENTRIES", 10000),

  // ---- Archive password handling ----
  ARCHIVE_PASSWORD_MAX_ATTEMPTS: getNumber("ARCHIVE_PASSWORD_MAX_ATTEMPTS", 3),

  // ---- SSRF guard ----
  ALLOWED_FETCH_SIZE_BYTES: getNumber(
    "ALLOWED_FETCH_SIZE_BYTES",
    5 * 1024 * 1024 * 1024
  ),
  FETCH_TIMEOUT_SECONDS: getNumber("FETCH_TIMEOUT_SECONDS", 300),

  // ---- Retry schedule ----
  RETRY_IMMEDIATE_DELAY_SECONDS: getNumber("RETRY_IMMEDIATE_DELAY_SECONDS", 0),
  RETRY_DELAYED_DELAY_SECONDS: getNumber("RETRY_DELAYED_DELAY_SECONDS", 300),
  RETRY_MAX_ATTEMPTS: getNumber("RETRY_MAX_ATTEMPTS", 2),
};

export type Settings = typeof settings;
