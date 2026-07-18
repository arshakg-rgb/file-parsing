import "reflect-metadata";
import pg from "pg";
import { Sequelize } from "sequelize-typescript";
import Config from "../system-config/Config.js";
import ServiceManager, { Enforce } from "../ServiceManager.js";
import { InstantiationError } from "../../errors/InstantiationError.js";
import * as dbModels from "./models/index.js";
import type { DatabaseModels } from "./models/index.js";
import { Repositories } from "./repositories/index.js";

const { Pool } = pg;

class MySqlManager extends ServiceManager {
  protected static instance: MySqlManager;
  private _pool: pg.Pool | null = null;
  private _sequelize?: Sequelize;
  private _models?: DatabaseModels;
  private _repositories?: Repositories;

  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate MySqlManager directly. Use getInstance()");
    }
    super(enforce);
  }

  public static getInstance(): MySqlManager {
    if (!MySqlManager.instance) {
      MySqlManager.instance = new MySqlManager(Enforce);
    }
    return MySqlManager.instance;
  }

  private getPool(): pg.Pool {
    if (!this._pool) {
      const config = Config.getInstance();
      this._pool = new Pool({
        connectionString: config.settings.DATABASE_URL,
        max: 50,
        idleTimeoutMillis: 1200000,
        connectionTimeoutMillis: 30000,
      });
    }
    return this._pool;
  }

  public get pool(): pg.Pool {
    return this.getPool();
  }

  public async initialize(): Promise<void> {
    console.log("Initializing MySqlManager...");
    await this.waitForDb();
    // Prime Sequelize models and test the connection
    this.models;
    await this.sequelize.authenticate();
    console.log("MySqlManager initialized");
  }

  public async shutdown(): Promise<void> {
    console.log("Shutting down MySqlManager...");
    await this.pool.end();
  }

  public get sequelize(): Sequelize {
    if (!this._sequelize) {
      const config = Config.getInstance();
      this._sequelize = new Sequelize(config.settings.DATABASE_URL, {
        dialect: "postgres",
        logging: false,
        pool: { max: 50, idle: 1200000, acquire: 30000 },
      });
    }
    return this._sequelize;
  }

  public get models(): DatabaseModels {
    if (!this._models) {
      this._models = {
        ParseJob: dbModels.ParseJob,
        DeadLetter: dbModels.DeadLetter,
        OutputPart: dbModels.OutputPart,
        PendingArchiveEntry: dbModels.PendingArchiveEntry,
        ParsedRecord: dbModels.ParsedRecord,
        RubbishLog: dbModels.RubbishLog,
        Template: dbModels.Template,
        SchemaMigration: dbModels.SchemaMigration,
      };
      this.sequelize.addModels(Object.values(this._models));
    }
    return this._models;
  }

  public get repositories(): Repositories {
    if (!this._repositories) {
      this._repositories = new Repositories(this.models);
    }
    return this._repositories;
  }

  /**
   * Wait for database connection to succeed (Cloud SQL proxy race condition guard).
   * Retries with exponential backoff up to 300 seconds (5 minutes).
   */
  private async waitForDb(): Promise<void> {
    const maxAttempts = 60; // 60 * 5s = 300s max for cold starts and connection recovery
    let attempt = 0;
    while (attempt < maxAttempts) {
      try {
        await this.sequelize.authenticate();
        return;
      } catch (err) {
        attempt++;
        const delay = Math.min(5000 * attempt, 10000); // 5s, 10s, 10s, ...
        if (attempt < maxAttempts) {
          await new Promise(r => setTimeout(r, delay));
        }
      }
    }
    throw new Error(`Database connection failed after ${maxAttempts} attempts`);
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
  field_spec: any; // PostgreSQL JSONB is parsed automatically by pg
  exec_path: string;
  status: string;
  output_paths: any; // PostgreSQL JSONB is parsed automatically by pg
  counts: Record<string, any>;
  timings: Record<string, any>;
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

export interface PendingArchiveEntryRow {
  id: string;
  job_id: string;
  entry_name: string;
  entry_size: number;
  status: string;
  error?: string;
  created_at: Date;
  updated_at: Date;
}

export default MySqlManager;
