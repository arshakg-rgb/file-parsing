import Seven from "node-7z";
import { once } from "node:events";
import { BaseArchiveExtractor } from "./BaseArchiveExtractor.js";
import { TempFileManager } from "../TempFileManager.js";
import {InstantiationError} from "@errors/InstantiationError";

export class SevenZipExtractor extends BaseArchiveExtractor
{
    private static instance: SevenZipExtractor;

    private constructor(enforce: () => void)
    {
        super();

        if (enforce !== Enforce)
        {
            throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use SevenZipExtractor.getInstance() instead of new.");
        }
    }

    /**
     * Gets the singleton instance of SevenZipExtractor.
     *
     * @returns The singleton instance of SevenZipExtractor.
     */

    static getInstance(): SevenZipExtractor
    {
        if (!SevenZipExtractor.instance)
        {
            SevenZipExtractor.instance = new SevenZipExtractor(Enforce);
        }

        return SevenZipExtractor.instance;
    }

    async extract(jobId: string, raw: Buffer, compressedSize: number, fieldSpec: string[], batchId: string, password?: string): Promise<Record<string, unknown>[]>
    {
        const tmp: string = await TempFileManager.createTempFile(raw, ".7z");
        const extractDir: string = await TempFileManager.createTempDir();
        const stream: NodeJS.ReadableStream = Seven.extractFull(tmp, extractDir, { password: password || undefined });
        await once(stream, "end");
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
