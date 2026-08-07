import { InstantiationError } from "@errors/InstantiationError.js";
import pino from "pino";
import fs from "fs";
import path from "path";
import { ValidationResult } from "joi";
import {
    validateAppConfig,
    validateAuthConfig,
    validateCommonConfig,
    validateSocketConfig,
    validateRedisConfig
} from "./ConfigValidator.js";
import { IAppConfig } from "./io/IAppConfig.js";
import { IAuthConfig } from "./io/IAuthConfig.js";
import { ICommonConfig } from "./io/ICommonConfig.js";
import { ISocketConfig } from "./io/ISocketConfig.js";
import { IRedisConfig } from "./io/IRedisConfig.js";
import { settings } from "@shared/Settings.js";
const logger: pino.Logger = pino();

export default class Config
{
    private static instance: Config;
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
            throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use Config.getInstance() instead of new.");
        }

        try
        {
            this._appConfig = this.readConfigFile<IAppConfig>("app.json", validateAppConfig);
            this._socketConfig = this.readConfigFile<ISocketConfig>("socket.json", validateSocketConfig);
            this._redisConfig = this.readConfigFile<IRedisConfig>("redis-config.json", validateRedisConfig);
            this._authConfig = this.readConfigFile<IAuthConfig>("auth.json", validateAuthConfig);
            this._commonConfig = this.readConfigFile<ICommonConfig>("common.json", validateCommonConfig);
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
     * @returns The validated configuration data.
     * @throws If the configuration image is not found or validation fails.
     */

    private readConfigFile = <T>(fileName: string, validate: (data: {}) => ValidationResult<T>): T =>
    {
        const candidates: string[] = [
            path.resolve(process.cwd(), "configs", fileName),
            path.resolve(__dirname, "..", "..", "..", "configs", fileName),
            path.resolve(__dirname, "..", "..", "configs", fileName)
        ];

        const fileFullName = candidates.find((candidate) => fs.existsSync(candidate));

        if (!fileFullName)
        {
            throw new Error(`Config file '${fileName}' not found. Searched: ${candidates.join(", ")}`);
        }

        const rawData: string = fs.readFileSync(fileFullName, "utf-8");
        const jsonData = JSON.parse(rawData);

        const validationResult: ValidationResult = validate(jsonData);

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
