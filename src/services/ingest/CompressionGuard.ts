import { settings } from "@shared/Settings.js";
import { BombError } from "@errors/BombError.js";

/**
 * Enforces the compression-ratio / max-uncompressed-size zip-bomb guard.
 *
 * Stateless — used only through its static method, so no getInstance() is
 * needed here.
 */
export class CompressionGuard
{
    static checkRatio(compressed: number, uncompressed: number): void
    {
        if (compressed > 0 && uncompressed / compressed > settings.ARCHIVE_MAX_COMPRESSION_RATIO)
        {
            throw new BombError(`Compression ratio ${(uncompressed / compressed).toFixed(0)}:1 exceeds cap ${settings.ARCHIVE_MAX_COMPRESSION_RATIO}:1`);
        }

        if (uncompressed > settings.ARCHIVE_MAX_UNCOMPRESSED_BYTES)
        {
            throw new BombError(`Uncompressed size ${uncompressed} exceeds cap ${settings.ARCHIVE_MAX_UNCOMPRESSED_BYTES}`);
        }
    }
}
