import { CustomError } from "./CustomError.js";

/**
 * Class representing a http error error.
 */

export class HttpError extends CustomError
{
    /**
   * Constructs a new HttpError instance.
   * @param message - The message
   * @param statusCode - The status code
   * @param details - The details
   */

  constructor(message: string, statusCode: number = 500, details?: unknown)
  {
    super(message, "HTTP_ERROR", statusCode, details);
  }
}
