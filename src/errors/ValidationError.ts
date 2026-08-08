import { constants as HttpStatuses } from "http2";
import pino from "pino";
import {CustomError} from "@errors/CustomError.js";
import {createLogger} from "@utils/logger/Log.js";

const logger: pino.Logger = createLogger(module);

/**
 * Class representing a validation error.
 * Extends the CustomError class.
 */
export class ValidationError extends CustomError
{
  /**
   * Error code for input data validation.
   */

  public static INPUT: string = "INPUT";

  /**
   * Error code for output data validation
   */

  public static OUTPUT: string = "OUTPUT";

  /**
   * Error code for duplicate entry errors.
   */

  public static DUPLICATE_ENTRY: string = "DUPLICATE_ENTRY";

  /**
   * Constructs a new instance of the ValidationError class.
   * @param code - The error code.
   * @param message - The error message.
   * @param fields - Optional array of fields related to the error.
   * @param data - Optional additional data associated with the error.
   * @param info - Optional additional information associated with the error.
   */

  constructor(code: string, message: string, fields?: string[], info?: Record<string, unknown>, data?: Record<string, unknown>)
  {
    super("ValidationError");
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
      [ValidationError.INPUT]: HttpStatuses.HTTP_STATUS_BAD_REQUEST,
      [ValidationError.OUTPUT]: HttpStatuses.HTTP_STATUS_INTERNAL_SERVER_ERROR,
      [ValidationError.DUPLICATE_ENTRY]: HttpStatuses.HTTP_STATUS_CONFLICT
    };

    const retStatus: number = statusMap[this.code || ""] || HttpStatuses.HTTP_STATUS_BAD_REQUEST;

    if (!statusMap[this.code || ""])
    {
      logger.warn(`Unknown error status code ${this.code}`);
    }

    return retStatus;
  }
}
