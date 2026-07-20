import { ErrorInfo } from "@errors/io/ErrorInfo.js";

/**
 * Class representing a custom error error.
 */

export class CustomError extends Error
{
    /**
   * Code
   */

  public readonly code: string;
    /**
   * Status Code
   */

  public readonly statusCode: number;
    /**
   * Details
   */

  public readonly details?: unknown;

    /**
   * Constructs a new CustomError instance.
   * @param message - The message
   * @param code - The code
   * @param statusCode - The status code
   * @param details - The details
   */

  constructor(message: string, code: string = "CUSTOM_ERROR", statusCode: number = 500, details?: unknown)
    {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
    this.statusCode = statusCode;
    this.details = details;
    Error.captureStackTrace(this, this.constructor);
  }

    /**
   * Performs the to j s o n operation.
   * @returns The error info result
   */

  toJSON(): ErrorInfo
    {
    return {
      message: this.message,
      code: this.code,
      statusCode: this.statusCode,
      details: this.details,
    };
  }
}
