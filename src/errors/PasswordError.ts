/**
 * PasswordError - thrown when archive password is required or invalid
 */
export class PasswordError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PasswordError";
  }
}
