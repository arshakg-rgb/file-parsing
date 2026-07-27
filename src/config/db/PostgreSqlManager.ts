import "reflect-metadata";
import pg from "pg";
import { Sequelize } from "sequelize-typescript";
import { URL } from "url";
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
import {IDatabaseConfig} from "@config/system-config/io/IDatabaseConfig.js";

const { Pool } = pg;

/**
 * PostgreSqlManager is a singleton class responsible for managing the MySQL/PostgreSQL connection.
 * It provides methods to connect to and gracefully stop the database.
 */
export class PostgreSqlManager extends ServiceManager
{
  /**
   * Singleton instance of the PostgreSqlManager class.
   * @protected
   */

  protected static instance: PostgreSqlManager;

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
   * Constructs a new PostgreSqlManager instance.
   * @param enforce - A function to enforce the Singleton pattern.
   */

  protected constructor(enforce: () => void)
  {
      super(enforce);

      if (enforce !== Enforce)
      {
        throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE,"Cannot instantiate PostgreSqlManager directly. Use getInstance()");
      }

      this.logger = createLogger("PostgreSqlManager");
  }

  /**
   * Gets the single instance of the PostgreSqlManager class.
   * @returns The single instance of the class.
   */

  public static getInstance(): PostgreSqlManager
  {
      if (!PostgreSqlManager.instance)
      {
        PostgreSqlManager.instance = new PostgreSqlManager(Enforce);
      }

      return PostgreSqlManager.instance;
  }

  /**
   * Builds the Sequelize instance from configuration.
   * @returns The configured Sequelize instance.
   */

  private buildSequelize(): Sequelize
  {
      const config: IDatabaseConfig = Config.getInstance().databaseConfig;
      const databaseUrl = process.env.FILE_DATABASE_URL || config.url;

      return new Sequelize(databaseUrl, {
        dialect: "postgres",
        logging: false,
        timezone: "+02:00",
        pool: { max: config.poolSize, min: 0, acquire: 30000, idle: 1200000 },
        dialectOptions: this.buildSslOptions(databaseUrl),
      });
  }

  /**
   * Derives SSL options from a Postgres connection URL.
   * @param url - The connection URL.
   * @returns Dialect options for SSL, or an empty object.
   */

  private buildSslOptions(url: string): Record<string, unknown>
  {
    try
    {
        const parsed = new URL(url);
        const sslMode: string = parsed.searchParams.get("sslmode") || "";
        const ssl: string = parsed.searchParams.get("ssl") || "";

        if (sslMode === "require" || sslMode === "verify-ca" || sslMode === "verify-full" || ssl === "true" || ssl === "require")
        {
          return { ssl: { require: true, rejectUnauthorized: false } };
        }
    }
    catch(err)
    {
      this.logger.warn(`buildSslOptions: could not parse connection URL, defaulting to no SSL options: ${(err as Error).message}`);
    }

      return {};
  }

  /**
   * Connects to the database, loads models, and verifies the connection.
   */

  public async connect(): Promise<void>
  {
      this.logger.info("Connecting PostgreSqlManager...");

      this._sequelize ??= this.buildSequelize();
      await this.waitForDb();
      this.models;
      await this.sequelize.authenticate();

      this.logger.info("PostgreSqlManager connected");
  }

  /**
   * Gracefully stops the database connection and connection pool.
   */
  public async gracefulStop(): Promise<void>
  {
    this.logger.info("Stopping PostgreSqlManager...");

    if (this._pool)
    {
      await this._pool.end();
    }

    if (this._sequelize)
    {
      await this._sequelize.close();
    }
  }

  /**
   * Waits for the database to become ready (Cloud SQL proxy race condition guard).
   * Retries with exponential backoff up to 300 seconds (5 minutes).
   */

  private async waitForDb(): Promise<void>
  {
      const maxAttempts = 60;
      let attempt: number = 0;

      while (attempt < maxAttempts)
      {
        try
        {
            await this.sequelize.authenticate();
            return;
        }
        catch (err)
        {
            attempt++;
            const delay: number = Math.min(5000 * attempt, 10000);

            if (attempt < maxAttempts)
            {
              await new Promise((resolve) => setTimeout(resolve, delay));
            }
        }
    }

    throw new ServerError(ServerError.INTERNAL,`Database connection failed after ${maxAttempts} attempts`);
  }

  /**
   * Gets the Sequelize instance.
   * @returns The Sequelize instance.
   * @throws Will throw an error if the Sequelize instance is not initialized.
   */

  public get sequelize(): Sequelize
  {
      if (!this._sequelize)
      {
        this._sequelize = this.buildSequelize();
      }

      return this._sequelize;
  }

  /**
   * Gets the pg connection pool.
   * @returns The pg.Pool instance.
   */

  public get pool(): pg.Pool
  {
      if (!this._pool)
      {
        const config: IDatabaseConfig = Config.getInstance().databaseConfig;
        const databaseUrl = process.env.FILE_DATABASE_URL || config.url;
        this._pool = new Pool({
          connectionString: databaseUrl,
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

  public get models(): DatabaseModels
  {
      if (!this._models)
      {
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

  public get repositories(): Repositories
  {
      if (!this._repositories)
      {
        this._repositories = new Repositories(this.models);
      }
      return this._repositories;
    }
}



export default PostgreSqlManager;
