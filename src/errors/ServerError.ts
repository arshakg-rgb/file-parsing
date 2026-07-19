import { CustomError } from "./CustomError.js";

/**
 * Class representing a server error error.
 */
export class ServerError extends CustomError {
    /**
   * The i n t e r n a l value
   */
  static readonly INTERNAL = "INTERNAL_ERROR";
    /**
   * The d a t a b a s e value
   */
  static readonly DATABASE = "DATABASE_ERROR";
    /**
   * The s e r v i c e_ u n a v a i l a b l e value
   */
  static readonly SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE";

    /**
   * Constructs a new ServerError instance.
   * @param message - The message
   * @param code - The code
   * @param statusCode - The status code
   * @param details - The details
   */
  constructor(message: string, code: string = ServerError.INTERNAL, statusCode: number = 500, details?: unknown) {
    super(message, code, statusCode, details);
  }
}
