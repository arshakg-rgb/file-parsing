export class SSRFError extends Error
{
    /**
     * Constructs a new SSRFError instance.
     * @param message - The message
     */
    constructor(message: string) {
        super(message);
        this.name = "SSRFError";
    }
}
