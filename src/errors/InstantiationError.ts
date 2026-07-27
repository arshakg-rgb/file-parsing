import { constants as HttpStatuses } from "http2";
import pino from "pino";
import {CustomError} from "@errors/CustomError.js";
import {createLogger} from "@utils/logger/Log.js";

const logger: pino.Logger = createLogger(import.meta.url);

export class InstantiationError extends CustomError
{
    /**
     * Error code for not instantiable errors.
     */

    public static NOT_INSTANTIABLE: string = "NOT_INSTANTIABLE";

    /**
     * Constructs a new instance of the InstantiationError class.
     * @param code - The error code.
     * @param message - The error message.
     * @param fields - Optional array of fields related to the error.
     * @param data - Optional additional data associated with the error.
     * @param info - Optional additional information associated with the error.
     */

    constructor(code: string, message: string, fields?: string[], info?: Record<string, any>, data?: Record<string, any>)
    {
        super("InstantiationError");
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
            [InstantiationError.NOT_INSTANTIABLE]: HttpStatuses.HTTP_STATUS_CONFLICT
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
