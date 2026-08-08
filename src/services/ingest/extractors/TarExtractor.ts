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

    async extractFromFile(jobId: string, filePath: string, compressedSize: number, fieldSpec: string[], batchId: string): Promise<Record<string, unknown>[]>
    {
        const extractDir: string = await TempFileManager.createTempDir();
        try
        {
            await extractTar({ file: filePath, cwd: extractDir });
            const out: Record<string, unknown>[] = await this.entryStore.collectExtractedFiles(extractDir, jobId, compressedSize, fieldSpec, batchId);
            return out;
        }
        finally
        {
            await TempFileManager.removeDir(extractDir);
        }
    }
}

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void
{
}
