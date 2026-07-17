import { CustomError } from "./CustomError.js";

export class ServerError extends CustomError {
  static readonly INTERNAL = "INTERNAL_ERROR";
  static readonly DATABASE = "DATABASE_ERROR";
  static readonly SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE";

  constructor(message: string, code: string = ServerError.INTERNAL, statusCode: number = 500, details?: any) {
    super(message, code, statusCode, details);
  }
}
