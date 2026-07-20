import "reflect-metadata";
import pg from "pg";
import { Sequelize } from "sequelize-typescript";
import Config from "../system-config/Config.js";
import { ServiceManager, Enforce } from "../ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { ServerError } from "@errors/ServerError.js";
import { createLogger, Logger } from "@utils/logger/logger.js";

import ParseJob from "./models/ParseJob.js";
import DeadLetter from "./models/DeadLetter.js";
import OutputPart from "./models/OutputPart.js";
import PendingArchiveEntry from "./models/PendingArchiveEntry.js";
import ParsedRecord from "./models/ParsedRecord.js";
import RubbishLog from "./models/RubbishLog.js";
import Template from "./models/Template.js";
import SchemaMigration from "./models/SchemaMigration.js";

import type { DatabaseModels } from "./models/index.js";
import { Repositories } from "./repositories/index.js";

const { Pool } = pg;

/**
 * MySqlManager is a singleton class responsible for managing the MySQL/PostgreSQL connection.
 * It provides methods to connect to and gracefully stop the database.
 */
export class MySqlManager extends ServiceManager {
  /**
   * Singleton instance of the MySqlManager class.
   * @protected
   */
  protected static instance: MySqlManager;

  /**
   * The Sequelize instance.
   * @private
   */
  private _sequelize?: Sequelize;

  /**
   * The pg connection pool.
   * @private
   */
  private _pool?: pg.Pool;

  /**
   * The database models wrapper.
   * @private
   */
  private _models?: DatabaseModels;

  /**
   * The repository wrapper.
   * @private
   */
  private _repositories?: Repositories;

  /**
   * Logger instance.
   * @private
   */
  private logger: Logger;

  /**
   * Constructs a new MySqlManager instance.
   * @param enforce - A function to enforce the Singleton pattern.
   */
  protected constructor(enforce: () => void) {
    super(enforce);
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate MySqlManager directly. Use getInstance()");
    }
    this.logger = createLogger("MySqlManager");
  }

  /**
   * Gets the single instance of the MySqlManager class.
   * @returns The single instance of the class.
   */
  public static getInstance(): MySqlManager {
    if (!MySqlManager.instance) {
      MySqlManager.instance = new MySqlManager(Enforce);
    }
    return MySqlManager.instance;
  }

  /**
   * Builds the Sequelize instance from configuration.
   * @returns The configured Sequelize instance.
   */
  private buildSequelize(): Sequelize {
    const config = Config.getInstance().databaseConfig;

    return new Sequelize({
      database: config.database,
      username: config.username,
      password: config.password,
      host: config.host,
      port: config.port,
      dialect: "postgres",
      logging: false,
      timezone: "+02:00",
      pool: { max: config.poolSize, min: 0, acquire: 30000, idle: 1200000 },
      dialectOptions: config.ssl
        ? { ssl: { require: true, rejectUnauthorized: false } }
        : {},
    });
  }

  /**
   * Connects to the database, loads models, and verifies the connection.
   */
  public async connect(): Promise<void> {
    this.logger.info("Connecting MySqlManager...");

    this._sequelize ??= this.buildSequelize();
    await this.waitForDb();
    this.models;
    await this.sequelize.authenticate();

    this.logger.info("MySqlManager connected");
  }

  /**
   * Gracefully stops the database connection and connection pool.
   */
  public async gracefulStop(): Promise<void> {
    this.logger.info("Stopping MySqlManager...");

    if (this._pool) {
      await this._pool.end();
    }
    if (this._sequelize) {
      await this._sequelize.close();
    }
  }

  /**
   * Waits for the database to become ready (Cloud SQL proxy race condition guard).
   * Retries with exponential backoff up to 300 seconds (5 minutes).
   */
  private async waitForDb(): Promise<void> {
    const maxAttempts = 60;
    let attempt = 0;

    while (attempt < maxAttempts) {
      try {
        await this.sequelize.authenticate();
        return;
      } catch (err) {
        attempt++;
        const delay = Math.min(5000 * attempt, 10000);
        if (attempt < maxAttempts) {
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw new ServerError(
      `Database connection failed after ${maxAttempts} attempts`,
      ServerError.INTERNAL,
      500
    );
  }

  /**
   * Gets the Sequelize instance.
   * @returns The Sequelize instance.
   * @throws Will throw an error if the Sequelize instance is not initialized.
   */
  public get sequelize(): Sequelize {
    if (!this._sequelize) {
      this._sequelize = this.buildSequelize();
    }
    return this._sequelize;
  }

  /**
   * Gets the pg connection pool.
   * @returns The pg.Pool instance.
   */
  public get pool(): pg.Pool {
    if (!this._pool) {
      const config = Config.getInstance().databaseConfig;
      this._pool = new Pool({
        connectionString: Config.getInstance().settings.DATABASE_URL,
        max: config.poolSize,
        idleTimeoutMillis: 1200000,
        connectionTimeoutMillis: 30000,
      });
    }
    return this._pool;
  }

  /**
   * Gets the database models, registering them with Sequelize on first access.
   * @returns The database models result.
   */
  public get models(): DatabaseModels {
    if (!this._models) {
      this._models = {
        ParseJob,
        DeadLetter,
        OutputPart,
        PendingArchiveEntry,
        ParsedRecord,
        RubbishLog,
        Template,
        SchemaMigration,
      };
      this.sequelize.addModels(Object.values(this._models));
    }
    return this._models;
  }

  /**
   * Gets the repositories, initializing them on first access.
   * @returns The repositories result.
   */
  public get repositories(): Repositories {
    if (!this._repositories) {
      this._repositories = new Repositories(this.models);
    }
    return this._repositories;
  }
}

export interface ParseJobRow {
  job_id: string;
  batch_id?: string;
  parent_job_id?: string;
  source_type: string;
  source_ref: string;
  s3_url?: string;
  size?: number;
  field_spec: unknown;
  exec_path: string;
  status: string;
  output_paths: unknown;
  counts: Record<string, unknown>;
  timings: Record<string, unknown>;
  error?: string;
  created_at: Date;
  updated_at: Date;
}

export interface OutputPartRow {
  part_id: string;
  job_id: string;
  template_id: string;
  s3_path: string;
  row_count: number;
  byte_size: number;
  created_at: Date;
}

export interface DeadLetterRow {
  dlq_id: string;
  job_id: string;
  byte_offset: number;
  byte_length: number;
  line_no: number;
  raw_bytes: string;
  failure_class: string;
  error: string;
  attempts: number;
  status: string;
  created_at: Date;
  updated_at: Date;
}

export default MySqlManager;
