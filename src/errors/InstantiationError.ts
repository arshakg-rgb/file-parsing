import { CustomError } from "./CustomError.js";

/**
 * Class representing a instantiation error error.
 */
export class InstantiationError extends CustomError {
    /**
   * Constructs a new InstantiationError instance.
   * @param message - The message
   */
  constructor(message: string = "Cannot instantiate singleton directly") {
    super(message, "INSTANTIATION_ERROR", 500);
  }
}
