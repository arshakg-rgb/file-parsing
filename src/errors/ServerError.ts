import { CustomError } from "./CustomError.js";

/**
 * Class representing a server error error.
 */
export class ServerError extends CustomError
{
    /**
   * The internal value
   */

  static readonly INTERNAL = "INTERNAL_ERROR";
    /**
   * The database value
   */

  static readonly DATABASE = "DATABASE_ERROR";
    /**
   * The service unavailable value
   */

  static readonly SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE";

    /**
   * Constructs a new ServerError instance.
   * @param message - The message
   * @param code - The code
   * @param statusCode - The status code
   * @param details - The details
   */
  constructor(message: string, code: string = ServerError.INTERNAL, statusCode: number = 500, details?: unknown)
    {
    super(message, code, statusCode, details);
  }
}
