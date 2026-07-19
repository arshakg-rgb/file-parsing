import dotenv from "dotenv";
import path from "path";
import { IAppConfig } from "@config/system-config/io/IAppConfig.js";
import { IDatabaseConfig } from "@config/system-config/io/IDatabaseConfig.js";

// Load local .env.local first (project-specific overrides take priority)
dotenv.config({ path: path.resolve(".env.local") });
// Fall back to sibling Python project's env for shared infra settings
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
 * Config is responsible for config operations.
 */
class Config {
    /**
   * Singleton instance
   * @private
   */
  private static instance: Config;
    /**
   * _app Config
   * @private
   */
  private _appConfig: IAppConfig;
    /**
   * _common Config
   * @private
   */
  private _commonConfig: unknown;
    /**
   * _auth Config
   * @private
   */
  private _authConfig: unknown;
    /**
   * _database Config
   * @private
   */
  private _databaseConfig: IDatabaseConfig;

    /**
   * Constructs a new Config instance.
   */
  private constructor() {
    this._appConfig = {
      name: getString("APP_NAME", "file-parsing-pipeline"),
      version: getString("APP_VERSION", "1.0.0"),
      environment: (getString("NODE_ENV", "development") as "development" | "staging" | "production"),
      port: getNumber("PORT", 3000),
    };

    this._commonConfig = {
      request_body_limit: getString("REQUEST_BODY_LIMIT", "10mb"),
    };

    this._authConfig = {
      sessionSecret: getString("SESSION_SECRET", "your-secret-key"),
    };

    this._databaseConfig = {
      url: getString("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/parsing_pipeline"),
      host: getString("DB_HOST", "localhost"),
      port: getNumber("DB_PORT", 5432),
      username: getString("DB_USERNAME", "postgres"),
      password: getString("DB_PASSWORD", "postgres"),
      database: getString("DB_NAME", "parsing_pipeline"),
      ssl: getString("DB_SSL", "false") === "true",
      poolSize: getNumber("DB_POOL_SIZE", 10),
    };
  }

    /**
   * Gets the single instance of the Config class.
   * @returns The single instance of the class
   */
  public static getInstance(): Config {
    if (!Config.instance) {
      Config.instance = new Config();
    }
    return Config.instance;
  }

    /**
   * Gets the app config.
   * @returns The i app config result
   */
  public get appConfig(): IAppConfig {
    return this._appConfig;
  }

    /**
   * Gets the common config.
   * @returns The unknown result
   */
  public get commonConfig(): unknown {
    return this._commonConfig;
  }

    /**
   * Gets the auth config.
   * @returns The unknown result
   */
  public get authConfig(): unknown {
    return this._authConfig;
  }

    /**
   * Gets the database config.
   * @returns The i database config result
   */
  public get databaseConfig(): IDatabaseConfig {
    return this._databaseConfig;
  }

  // Legacy settings support
  public get settings() {
    return {
      GCP_PROJECT_ID: getString("GCP_PROJECT_ID", "data-etl-499916"),
      GOOGLE_APPLICATION_CREDENTIALS: getOptionalString("GOOGLE_APPLICATION_CREDENTIALS"),
      DATA_BUCKET: getString("DATA_BUCKET", "datalead-osint"),
      DEPLOYMENT_BUCKET: getString("DEPLOYMENT_BUCKET", "datalead-osint"),
      QUEUE_BACKEND: getString("QUEUE_BACKEND", "pubsub"),
      DATABASE_URL: getString("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/parsing_pipeline"),
      INGEST_QUEUE_URL: getString("INGEST_QUEUE_URL", "fpp-ingest"),
      CLASSIFY_QUEUE_URL: getString("CLASSIFY_QUEUE_URL", "fpp-classify"),
      PARSE_QUEUE_URL: getString("PARSE_QUEUE_URL", "fpp-parse"),
      DLQ_QUEUE_URL: getString("DLQ_QUEUE_URL", "fpp-line-dlq"),
      LOAD_QUEUE_URL: getString("LOAD_QUEUE_URL", "fpp-load"),
      REPORT_QUEUE_URL: getString("REPORT_QUEUE_URL", "fpp-report"),
      JOB_EVENTS_QUEUE_URL: getString("JOB_EVENTS_QUEUE_URL", "fpp-job-events"),
      ARCHIVE_ENTRY_QUEUE_URL: getString("ARCHIVE_ENTRY_QUEUE_URL", "fpp-archive-entry"),
      FIRESTORE_DATABASE_ID: getString("FIRESTORE_DATABASE_ID", "osint-fdb"),
      TEMPLATE_COLLECTION: getString("TEMPLATE_COLLECTION", "file-parsing-templates"),
      ANTHROPIC_API_KEY: getString("ANTHROPIC_API_KEY", ""),
      ANTHROPIC_MODEL: getString("ANTHROPIC_MODEL", "claude-3-sonnet-20240229"),
      BEDROCK_MODEL_ID: getString("BEDROCK_MODEL_ID", "mock"),
      AI_CLASSIFIER_URL: getOptionalString("AI_CLASSIFIER_URL"),
      AI_RATE_LIMIT_RPM: getNumber("AI_RATE_LIMIT_RPM", 60),
      AI_RATE_LIMIT_BURST: getNumber("AI_RATE_LIMIT_BURST", 10),
      AI_CLASSIFY_TIMEOUT_MS: getNumber("AI_CLASSIFY_TIMEOUT_MS", 30000),
      VERTEX_MODEL: getString("VERTEX_MODEL", "gemini-2.5-flash"),
      VERTEX_LOCATION: getString("VERTEX_LOCATION", "us-central1"),
      LOKI_HOST: getOptionalString("LOKI_HOST"),
      LOKI_USERNAME: getOptionalString("LOKI_USERNAME"),
      LOKI_PASSWORD: getOptionalString("LOKI_PASSWORD"),
      FETCH_CHUNK_SIZE: getNumber("FETCH_CHUNK_SIZE", 8 * 1024 * 1024),
      MAX_QUOTED_NEWLINES: getNumber("MAX_QUOTED_NEWLINES", 0),
      MAX_LINE_BYTES: getNumber("MAX_LINE_BYTES", 1024 * 1024),
      SMALL_FILE_SINGLE_GET_THRESHOLD: getNumber("SMALL_FILE_SINGLE_GET_THRESHOLD", 128 * 1024 * 1024),
      RAM_FLUSH_WATERMARK: getNumber("RAM_FLUSH_WATERMARK", 256 * 1024 * 1024),
      MAX_MERGED_PART_BYTES: getNumber("MAX_MERGED_PART_BYTES", 64 * 1024 * 1024),
      RUBBISH_CONFIDENCE_MIN: getNumber("RUBBISH_CONFIDENCE_MIN", 0.9),
      MATCH_RATE_FLOOR: getNumber("MATCH_RATE_FLOOR", 0.1),
      MATCH_RATE_WINDOW: getNumber("MATCH_RATE_WINDOW", 1000),
      PROBE_WINDOW_MIN_BYTES: getNumber("PROBE_WINDOW_MIN_BYTES", 64 * 1024),
      PROBE_WINDOW_MAX_BYTES: getNumber("PROBE_WINDOW_MAX_BYTES", 1 * 1024 * 1024),
      PROBE_TARGET_LINES: getNumber("PROBE_TARGET_LINES", 150),
      PROBE_COUNT_MIN: getNumber("PROBE_COUNT_MIN", 5),
      PROBE_COUNT_MAX: getNumber("PROBE_COUNT_MAX", 24),
      PROBE_SIZE_PER_COUNT: getNumber("PROBE_SIZE_PER_COUNT", 512 * 1024 * 1024),
      FAILED_LINE_RATIO_THRESHOLD: getNumber("FAILED_LINE_RATIO_THRESHOLD", 0.05),
      ARCHIVE_MAX_COMPRESSION_RATIO: getNumber("ARCHIVE_MAX_COMPRESSION_RATIO", 100),
      ARCHIVE_MAX_NESTING_DEPTH: getNumber("ARCHIVE_MAX_NESTING_DEPTH", 1),
      ARCHIVE_MAX_UNCOMPRESSED_BYTES: getNumber("ARCHIVE_MAX_UNCOMPRESSED_BYTES", 10 * 1024 * 1024 * 1024),
      ARCHIVE_MAX_ENTRIES: getNumber("ARCHIVE_MAX_ENTRIES", 10000),
      ARCHIVE_PASSWORD_MAX_ATTEMPTS: getNumber("ARCHIVE_PASSWORD_MAX_ATTEMPTS", 3),
      LARGE_FILE_THRESHOLD_BYTES: getNumber("LARGE_FILE_THRESHOLD_BYTES", 500 * 1024 * 1024),
      ALLOWED_FETCH_SIZE_BYTES: getNumber("ALLOWED_FETCH_SIZE_BYTES", 5 * 1024 * 1024 * 1024),
      FETCH_TIMEOUT_SECONDS: getNumber("FETCH_TIMEOUT_SECONDS", 600),
      QUEUE_TIMEOUT_SECONDS: getNumber("QUEUE_TIMEOUT_SECONDS", 60),
      GCS_TIMEOUT_SECONDS: getNumber("GCS_TIMEOUT_SECONDS", 1200),
      RETRY_IMMEDIATE_DELAY_SECONDS: getNumber("RETRY_IMMEDIATE_DELAY_SECONDS", 0),
      RETRY_DELAYED_DELAY_SECONDS: getNumber("RETRY_DELAYED_DELAY_SECONDS", 300),
      RETRY_MAX_ATTEMPTS: getNumber("RETRY_MAX_ATTEMPTS", 2),
    };
  }
}

export default Config;
