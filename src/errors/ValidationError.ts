import { CustomError } from "./CustomError.js";

/**
 * Class representing a validation error error.
 */
export class ValidationError extends CustomError {
    /**
   * The i n p u t value
   */
  static readonly INPUT = "VALIDATION_ERROR";
    /**
   * The m i s s i n g_ f i e l d value
   */
  static readonly MISSING_FIELD = "MISSING_FIELD";
    /**
   * The i n v a l i d_ f o r m a t value
   */
  static readonly INVALID_FORMAT = "INVALID_FORMAT";

    /**
   * Constructs a new ValidationError instance.
   * @param message - The message
   * @param code - The code
   * @param details - The details
   */
  constructor(message: string, code: string = ValidationError.INPUT, details?: unknown) {
    super(message, code, 400, details);
  }
}
