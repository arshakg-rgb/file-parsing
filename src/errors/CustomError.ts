/**
 * Abstract class representing a custom error.
 * This class extends the built-in Error class and provides additional properties for error handling.
 */
export abstract class CustomError extends Error
{
  public status: number | undefined;
  public code?: string;
  public name: string;
  public message: string;
  public data?: Record<string, any>;
  public info?: Record<string, any>;
  public fields?: string[];

  /**
   * Constructs a new CustomError instance.
   *
   * @param name - The name of the error.
   * @param message - Error message (default empty).
   * @param code - Optional error code.
   * @param fields - Optional array of fields related to the error.
   * @param data - Optional additional data associated with the error.
   * @param info - Optional additional information associated with the error.
   */

  protected constructor(name: string, message: string = "", code?: string, fields?: string[], info?: Record<string, any>, data?: Record<string, any>,)
  {
    super(message);
    this.name = name;
    this.message = message;
    this.code = code;
    this.fields = fields;
    this.data = data;
    this.info = info;

    Error.captureStackTrace(this, this.constructor);
  }
}
