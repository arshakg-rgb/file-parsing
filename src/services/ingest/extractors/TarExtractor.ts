import { extract as extractTar } from "tar";
import { BaseArchiveExtractor } from "./BaseArchiveExtractor.js";
import { TempFileManager } from "../TempFileManager.js";
import {InstantiationError} from "@errors/InstantiationError";

export class TarExtractor extends BaseArchiveExtractor
{
    private static instance: TarExtractor;

    private constructor(enforce: () => void)
    {
        super();

        if (enforce !== Enforce)
        {
            throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use TarExtractor.getInstance() instead of new.");
        }
    }

    /**
     * Gets the singleton instance of TarExtractor.
     *
     * @returns The singleton instance of TarExtractor.
     */

    static getInstance(): TarExtractor
    {
        if (!TarExtractor.instance)
        {
            TarExtractor.instance = new TarExtractor(Enforce);
        }

        return TarExtractor.instance;
    }

    async extract(jobId: string, raw: Buffer, compressedSize: number, fieldSpec: string[], batchId: string): Promise<Record<string, unknown>[]>
    {
        const tmp: string = await TempFileManager.createTempFile(raw, ".tar");
        const extractDir: string = await TempFileManager.createTempDir();
        await extractTar({ file: tmp, cwd: extractDir });
        const out: Record<string, unknown>[] = await this.entryStore.collectExtractedFiles(extractDir, jobId, compressedSize, fieldSpec, batchId);
        await TempFileManager.removeDir(extractDir);
        await TempFileManager.removeFile(tmp);
        return out;
    }
}

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void
{
}
