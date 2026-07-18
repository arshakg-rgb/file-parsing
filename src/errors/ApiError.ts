import { CustomError } from "./CustomError.js";

export class ApiError extends CustomError 
{
  constructor(message: string, statusCode: number = 500, details?: any) 
{
    super(message, "API_ERROR", statusCode, details);
  }
}
