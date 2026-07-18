import { CustomError } from "./CustomError.js";

export class HttpError extends CustomError {
  constructor(message: string, statusCode: number = 500, details?: unknown) {
    super(message, "HTTP_ERROR", statusCode, details);
  }
}
