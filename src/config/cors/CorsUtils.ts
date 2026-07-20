import Config from "@config/system-config/Config.js";
import { IAppConfig } from "@config/system-config/io/IAppConfig.js";
import { ServerError } from "@errors/ServerError.js";
import { createLogger, Logger } from "@utils/logger/Log.js";
import cors, { CorsOptions, type CorsOptionsDelegate, CorsRequest } from "cors";
import dotenv from "dotenv";

dotenv.config();

const logger: Logger = createLogger("CorsUtils");

/**
 * Utility class for setting up CORS for the application.
 */
export class CorsUtils
{
    /**
     * Sets up CORS for the application.
     *
     * @returns The CORS middleware.
     */

    public static setupCors(): (req: CorsRequest, res: { statusCode?: number | undefined; setHeader(key: string, value: string): unknown; end(): unknown; }, next: (err?: unknown) => void) => void
    {
        try
        {
            const allowCors: RegExp[] = this.getAllowedDomainsRegExp();

            const corsOptions: CorsOptions = {
                optionsSuccessStatus: 200,
                origin: allowCors.length > 0 ? allowCors : "*",
                credentials: true
            };

            return cors(corsOptions);
        }
        catch (error)
        {
            const errMessage = `Error setting up CORS ${error instanceof Error ? error.message : String(error)}`;
            logger.error(errMessage);
            throw new ServerError(errMessage, ServerError.INTERNAL);
        }
    }

    /**
     * Sets up CORS for Socket.IO.
     *
     * @returns The CORS options for Socket.IO.
     */
    public static setupCorsSocket(): CorsOptions | CorsOptionsDelegate
    {
        try
        {
            const allowCors: RegExp[] = this.getAllowedDomainsRegExp();

            return {
                origin: allowCors.length > 0 ? allowCors : "*",
                credentials: true
            };
        }
        catch (error)
        {
            logger.error(`Error setting up CORS for Socket.IO ${error instanceof Error ? error.message : String(error)}`);

            return { origin: "*", credentials: true };
        }
    }

    /**
     * Gets the allowed CORS domains.
     *
     * @returns The allowed CORS domains.
     */
    public static getAllowedDomains(): string[]
    {
        try
        {
            const appConfig: IAppConfig = Config.getInstance().appConfig;

            return process.env.CORS_DOMAINS ? process.env.CORS_DOMAINS.split(",") : appConfig.origins.domains;
        }
        catch (error)
        {
            logger.error(`Error getting CORS domains ${error instanceof Error ? error.message : String(error)}`);

            return [];
        }
    }

    /**
     * Gets the allowed CORS domains as regular expressions.
     *
     * @returns The allowed CORS domains.
     */
    private static getAllowedDomainsRegExp(): RegExp[]
    {
        try
        {
            const appConfig: IAppConfig = Config.getInstance().appConfig;
            const enabled: boolean = process.env.CORS_ENABLED ? process.env.CORS_ENABLED === "true" : appConfig.origins.enabled;

            if (!enabled)
            {
                return [];
            }

            const domains: string[] = process.env.CORS_DOMAINS ? process.env.CORS_DOMAINS.split(",") : appConfig.origins.domains;

            const allowCors: RegExp[] = [];

            for (const regex of domains)
            {
                try
                {
                    allowCors.push(new RegExp(regex.trim()));
                }
                catch (exception)
                {
                    logger.error("Invalid CORS regex", { pattern: regex, error: String(exception) });
                }
            }

            return allowCors;
        }
        catch (error)
        {
            logger.error(`Error getting CORS domains RegExp: ${error instanceof Error ? error.message : String(error)}`);

            return [];
        }
    }
}

export default CorsUtils;
