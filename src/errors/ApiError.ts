import { CustomError } from "./CustomError.js";

/**
 * Class representing a api error error.
 */
export class ApiError extends CustomError
{
    /**
   * Constructs a new ApiError instance.
   * @param message - The message
   * @param statusCode - The status code
   * @param details - The details
   */
  constructor(message: string, statusCode: number = 500, details?: unknown)
    {
    super(message, "API_ERROR", statusCode, details);
  }
}
