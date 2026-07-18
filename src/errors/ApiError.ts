import { CustomError } from "./CustomError.js";

export class ApiError extends CustomError {
  constructor(message: string, statusCode: number = 500, details?: unknown) {
    super(message, "API_ERROR", statusCode, details);
  }
}
