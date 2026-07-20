import { Response } from "express";

/**
 * Pagination metadata for list responses.
 */
export interface IPagination {
  current: number;
  total: number;
  count: number;
}

/**
 * Error detail shape used in the Reviro-style response envelope.
 */
export interface IResponseError {
  code: string;
  name: string;
  message: string;
  service?: string;
  forwarded?: string[];
  fields?: unknown[];
  info?: unknown;
  stack?: string[];
}

/**
 * Standardized HTTP response builder.
 *
 * Produces a consistent JSON envelope:
 * ```json
 * { "success": true, "data": {}, "pages": {}, "errors": [] }
 * ```
 */
export class ServiceResponse {
  private readonly res: Response;
  private statusCode: number = 200;
  private data: unknown;
  private pages?: IPagination;
  private errors: IResponseError[] = [];

  /**
   * Creates a new ServiceResponse bound to an Express response.
   * @param res - The Express response object.
   */
  constructor(res: Response) {
    this.res = res;
  }

  /**
   * Sets the HTTP status code for the response.
   * @param status - The HTTP status code.
   * @returns The ServiceResponse instance for chaining.
   */
  public setStatus(status: number): this {
    this.statusCode = status;
    return this;
  }

  /**
   * Sets the response payload and optional pagination metadata.
   * @param outcome - The data payload.
   * @param pages - Optional pagination metadata.
   * @returns The ServiceResponse instance for chaining.
   */
  public setOutcome(outcome: unknown, pages?: IPagination): this {
    this.data = outcome;
    this.pages = pages;
    return this;
  }

  /**
   * Sets the response errors.
   * @param errors - The error details.
   * @returns The ServiceResponse instance for chaining.
   */
  public setErrors(errors: IResponseError[]): this {
    this.errors = errors;
    return this;
  }

  /**
   * Sends the JSON response.
   */
  public send(): void {
    const success = this.errors.length === 0;
    const body: {
      success: boolean;
      data?: unknown;
      pages?: IPagination;
      errors: IResponseError[];
    } = {
      success,
      errors: this.errors,
    };

    if (success) {
      body.data = this.data;
      if (this.pages) {
        body.pages = this.pages;
      }
    }

    this.res.status(this.statusCode).json(body);
  }
}
