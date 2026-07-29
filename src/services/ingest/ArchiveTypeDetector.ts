import {MAGIC_7Z, MAGIC_GZ, MAGIC_RAR, MAGIC_ZIP} from "@service/ingest/io/IIngest";

/**
 * Detects archive type from magic bytes.
 *
 * This class is stateless and only ever used through its static method, so it
 * intentionally has no getInstance()/constructor — there is nothing to
 * instantiate.
 */
export class ArchiveTypeDetector
{
    static detect(header: Buffer): string | null
    {
        if (header.slice(0, 4).equals(MAGIC_ZIP))
        {
            return "zip";
        }

        if (header.slice(0, 2).equals(MAGIC_GZ))
        {
            return "gz";
        }

        if (header.slice(0, 6).equals(MAGIC_7Z))
        {
            return "7z";
        }

        if (header.slice(0, 4).equals(MAGIC_RAR))
        {
            return "rar";
        }

        if (header.length > 262 && header.slice(257, 262).toString() === "ustar")
        {
            return "tar";
        }

        return null;
    }
}
