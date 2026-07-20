/**
 * PasswordError - thrown when archive password is required or invalid
 */
export class PasswordError extends Error
{
    /**
   * Constructs a new PasswordError instance.
   * @param message - The message
   */

  constructor(message: string)
  {
    super(message);
    this.name = "PasswordError";
  }
}
