import { constants as HttpStatuses } from "http2";
import pino from "pino";
import {CustomError} from "@errors/CustomError.js";
import {createLogger} from "@utils/logger/Log.js";

const logger: pino.Logger = createLogger(module);

/**
 * Class representing a server error.
 * Extends the CustomError class.
 */
export class ServerError extends CustomError
{
  /**
   * Error code for internal server errors.
   */
  public static INTERNAL: string = "INTERNAL";

  /**
   * Error code for impl timeout errors.
   */
  public static REQUEST_TIMEOUT: string = "REQUEST_TIMEOUT";

  /**
   * Error code for not found errors.
   */
  public static NOT_FOUND: string = "NOT_FOUND";

  /**
   * Error code for conflict errors.
   */
  public static CONFLICT: string = "CONFLICT";

  /**
   * Error code for bad gateway errors (third-party service failures).
   */
  public static BAD_GATEWAY: string = "BAD_GATEWAY";

  /**
   * Error code for forbidden errors.
   */
  public static FORBIDDEN: string = "FORBIDDEN";

  /**
   * Constructs a new instance of the ServerError class.
   * @param code - The error code.
   * @param message - The error message.
   * @param fields - Optional array of fields related to the error.
   * @param data - Optional additional data associated with the error.
   */
  constructor(code: string, message: string, fields?: string[], data?: Record<string, unknown>)
  {
    super("ServerError");
    this.code = code;
    this.message = message;
    this.status = this.getStatus();
    this.fields = fields;
    this.data = data;
  }

  /**
   * Gets the HTTP status code based on the error code.
   * @returns The HTTP status code.
   */
  private getStatus(): number
  {
    const statusMap: Record<string, number> = {
      [ServerError.INTERNAL]: HttpStatuses.HTTP_STATUS_INTERNAL_SERVER_ERROR,
      [ServerError.NOT_FOUND]: HttpStatuses.HTTP_STATUS_NOT_FOUND,
      [ServerError.REQUEST_TIMEOUT]: HttpStatuses.HTTP_STATUS_GATEWAY_TIMEOUT,
      [ServerError.CONFLICT]: HttpStatuses.HTTP_STATUS_CONFLICT,
      [ServerError.BAD_GATEWAY]: HttpStatuses.HTTP_STATUS_BAD_GATEWAY,
      [ServerError.FORBIDDEN]: HttpStatuses.HTTP_STATUS_FORBIDDEN
    };

    const retStatus: number = statusMap[this.code || ""] || HttpStatuses.HTTP_STATUS_BAD_REQUEST;

    if (!statusMap[this.code || ""])
    {
      logger.warn(`Unknown error status code ${this.code}`);
    }

    return retStatus;
  }
}
