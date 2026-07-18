import { CustomError } from "./CustomError.js";

export class ValidationError extends CustomError 
{
  static readonly INPUT = "VALIDATION_ERROR";
  static readonly MISSING_FIELD = "MISSING_FIELD";
  static readonly INVALID_FORMAT = "INVALID_FORMAT";

  constructor(message: string, code: string = ValidationError.INPUT, details?: any) 
{
    super(message, code, 400, details);
  }
}
