import { constants as HttpStatuses } from "http2";
import pino from "pino";
import {CustomError} from "@errors/CustomError.js";
import {createLogger} from "@utils/logger/Log.js";

const logger: pino.Logger = createLogger(module);

/**
 * Class representing an HTTP error.
 * Extends the CustomError class.
 */
export class HttpError extends CustomError
{
    /**
     * Error code for not supported HTTP methods errors.
     */
    public static NOT_SUPPORTED_METHOD: string = "NOT_SUPPORTED_METHOD";

    /**
     * Error code for not found errors.
     */
    public static NOT_FOUND: string = "NOT_FOUND";

    /**
     * Error code for not authorized errors.
     */
    public static NOT_AUTHORIZED: string = "NOT_AUTHORIZED";

    /**
     * Error code for not first attempt errors.
     */
    public static NOT_FIRST_ATTEMPT: string = "NOT_FIRST_ATTEMPT";

    /**
     * Error code for not first attempt errors.
     */
    public static REQUEST_TIMEOUT: string = "REQUEST_TIMEOUT";

    /**
     * Constructs a new instance of the HttpError class.
     * @param code - The error code.
     * @param message - The error message.
     * @param fields - Optional array of fields related to the error.
     * @param data - Optional additional data associated with the error.
     * @param info - Optional additional information associated with the error.
     */
    constructor(code: string, message: string, fields?: string[], info?: Record<string, unknown>, data?: Record<string, unknown>)
    {
        super("HttpError");
        this.code = code;
        this.message = message;
        this.status = this.getStatus();
        this.fields = fields;
        this.info = info;
        this.data = data;
    }

    /**
     * Gets the HTTP status code based on the error code.
     * @returns The HTTP status code.
     */
    private getStatus(): number
    {
        const statusMap: Record<string, number> = {
            [HttpError.NOT_SUPPORTED_METHOD]: HttpStatuses.HTTP_STATUS_METHOD_NOT_ALLOWED,
            [HttpError.NOT_FOUND]: HttpStatuses.HTTP_STATUS_NOT_FOUND,
            [HttpError.NOT_AUTHORIZED]: HttpStatuses.HTTP_STATUS_UNAUTHORIZED,
            [HttpError.NOT_FIRST_ATTEMPT]: HttpStatuses.HTTP_STATUS_UNAUTHORIZED,
            [HttpError.REQUEST_TIMEOUT]: HttpStatuses.HTTP_STATUS_REQUEST_TIMEOUT
        };

        const status: number | undefined = this.code ? statusMap[this.code] : undefined;

        if (!status)
        {
            logger.warn(`Unknown error status code ${this.code}`);

            return HttpStatuses.HTTP_STATUS_BAD_REQUEST;
        }

        return status;
    }
}
