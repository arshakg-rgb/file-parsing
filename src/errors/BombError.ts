/**
 * BombError - thrown when an archive is detected as a decompression bomb
 * (excessive compression ratio, entry count, or uncompressed size)
 */
export class BombError extends Error
{
    /**
     * Constructs a new BombError instance.
     * @param message - The message
     */

    constructor(message: string)
    {
        super(message);
        this.name = "BombError";
    }
}
