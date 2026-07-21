import { InstantiationError } from "@errors/InstantiationError.js";
import { pino } from "pino";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { ValidationResult } from "./ConfigValidator.js";
import {
    validateAppConfig,
    validateMysqlConfig,
    validateAuthConfig,
    validateCommonConfig,
    validateSocketConfig,
    validateRedisConfig
} from "./ConfigValidator.js";
import { IMysqlConfig } from "./io/IMysqlConfig.js";
import { IAppConfig } from "./io/IAppConfig.js";
import { IAuthConfig } from "./io/IAuthConfig.js";
import { ICommonConfig } from "./io/ICommonConfig.js";
import { ISocketConfig } from "./io/ISocketConfig.js";
import { IRedisConfig } from "./io/IRedisConfig.js";
import { settings } from "@shared/Settings.js";

const __filename: string = fileURLToPath(import.meta.url);
const __dirname: string = path.dirname(__filename);

const logger: pino.Logger = pino();

function getEnvAppConfig(): IAppConfig
{
    return {
        name: process.env.APP_NAME || "file-parsing-pipeline",
        version: process.env.APP_VERSION || "1.0.0",
        environment: (process.env.NODE_ENV || "production") as IAppConfig["environment"],
        port: process.env.PORT ? Number(process.env.PORT) : 8080,
        origins: {
            enabled: process.env.CORS_ENABLED !== "false",
            domains: process.env.CORS_DOMAINS ? process.env.CORS_DOMAINS.split(",") : ["*"]
        }
    };
}

function getEnvMysqlConfig(): IMysqlConfig
{
    return {
        url: process.env.FILE_DATABASE_URL || process.env.DATABASE_URL || "postgresql://postgres:postgres@localhost:5432/parsing_pipeline",
        poolSize: process.env.DATABASE_POOL_SIZE ? Number(process.env.DATABASE_POOL_SIZE) : 10
    };
}

function getEnvAuthConfig(): IAuthConfig
{
    return { sessionSecret: process.env.SESSION_SECRET || "insecure-placeholder" };
}

function getEnvCommonConfig(): ICommonConfig
{
    return { request_body_limit: process.env.REQUEST_BODY_LIMIT || "10mb" };
}

export default class Config
{
    private static instance: Config;
    private readonly _mysqlConfig: IMysqlConfig;
    private readonly _socketConfig: ISocketConfig;
    private readonly _appConfig: IAppConfig;
    private readonly _redisConfig: IRedisConfig;
    private readonly _authConfig: IAuthConfig;
    private readonly _commonConfig: ICommonConfig;

    /**
     * Constructs a new instance of the Config class.
     * @param enforce - A function to enforce the Singleton pattern.
     * @throws If the enforce function is not provided or configuration files cannot be read.
     */

    constructor(enforce: () => void)
    {
        if (enforce !== Enforce)
        {
            throw new InstantiationError("Error: Instantiation failed: Use Config.getInstance() instead of new.");
        }

        try
        {
            this._appConfig = this.readConfigFile<IAppConfig>("app.json", validateAppConfig, getEnvAppConfig());
            this._mysqlConfig = this.readConfigFile<IMysqlConfig>("mysql.json", validateMysqlConfig, getEnvMysqlConfig());
            this._socketConfig = this.readConfigFile<ISocketConfig>("socket.json", validateSocketConfig, {});
            this._redisConfig = this.readConfigFile<IRedisConfig>("redis-config.json", validateRedisConfig, {});
            this._authConfig = this.readConfigFile<IAuthConfig>("auth.json", validateAuthConfig, getEnvAuthConfig());
            this._commonConfig = this.readConfigFile<ICommonConfig>("common.json", validateCommonConfig, getEnvCommonConfig());
        }
        catch (error)
        {
            const err: Error = error instanceof Error ? error : new Error(String(error));
            logger.error(`Error reading config file ${err.message}`);
            throw err;
        }
    }

    /**
     * Gets the single instance of the Config class.
     * @returns The single instance of the Config class.
     */

    public static getInstance(): Config
    {
        if (!Config.instance)
        {
            Config.instance = new Config(Enforce);
        }

        return Config.instance;
    }

    /**
     * Gets the common configuration.
     * @returns The common configuration.
     */

    public get commonConfig(): ICommonConfig
    {
        return this._commonConfig;
    }

    /**
     * Gets the socket configuration.
     * @returns The socket configuration.
     */

    public get socketConfig(): ISocketConfig
    {
        return this._socketConfig;
    }

    /**
     * Gets the Redis configuration.
     * @returns The Redis configuration
     */

    public get redisConfig(): IRedisConfig
    {
        return this._redisConfig;
    }

    /**
     * Gets the PostgreSQL configuration.
     * @returns The PostgreSQL configuration.
     */

    public get postgresConfig(): IMysqlConfig
    {
        return this._mysqlConfig;
    }

    /**
     * Alias for postgresConfig to maintain compatibility with MySqlManager.
     * @returns The PostgreSQL configuration.
     */

    public get databaseConfig(): IMysqlConfig
    {
        return this._mysqlConfig;
    }

    /**
     * Gets the application configuration.
     * @returns The application configuration.
     */

    public get appConfig(): IAppConfig
    {
        return this._appConfig;
    }

    /**
     * Gets the authentication configuration.
     * @returns The authentication configuration.
     */

    public get authConfig(): IAuthConfig
    {
        return this._authConfig;
    }

    /**
     * Legacy env-based settings for compatibility.
     * @returns The settings object.
     */

    public get settings()
    {
        return settings;
    }


    /**
     * Reads and validates a configuration image.
     * @param fileName - The name of the configuration image.
     * @param validate - The validation function for the configuration data.
     * @param fallback - The fallback value to use if the configuration file is not found.
     * @returns The validated configuration data.
     * @throws If the configuration image cannot be parsed or validation fails.
     */

    private readConfigFile = <T>(fileName: string, validate: (data: {}) => ValidationResult<T>, fallback: T): T =>
    {
        const fileFullName: string = path.join(__dirname, "..", "..", "..", "configs", fileName);

        if (!fs.existsSync(fileFullName))
        {
            logger.warn(`Config file '${fileName}' not found; using environment fallback`);
            return fallback;
        }

        const rawData: string = fs.readFileSync(fileFullName, "utf-8");
        const jsonData = JSON.parse(rawData);

        const validationResult: ValidationResult<T> = validate(jsonData);

        if (validationResult.error)
        {
            logger.error(`Error validating config file '${fileName}' ${validationResult.error.message}`);
            throw validationResult.error;
        }

        return validationResult.value;
    };
}

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void
{
}
