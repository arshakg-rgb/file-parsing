import { ErrorInfo } from "./io/ErrorInfo.js";

export class CustomError extends Error 
{
  public readonly code: string;
  public readonly statusCode: number;
  public readonly details?: any;

  constructor(message: string, code: string = "CUSTOM_ERROR", statusCode: number = 500, details?: any) 
{
    super(message);
    this.name = this.constructor.name;
    this.code = code;
    this.statusCode = statusCode;
    this.details = details;
    Error.captureStackTrace(this, this.constructor);
  }

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
