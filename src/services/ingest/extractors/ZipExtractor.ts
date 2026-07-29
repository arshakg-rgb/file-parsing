import NodeStreamZip from "node-stream-zip";
import { settings } from "@shared/Settings.js";
import { BombError } from "@errors/BombError.js";
import { BaseArchiveExtractor } from "./BaseArchiveExtractor.js";
import { CompressionGuard } from "../CompressionGuard.js";
import { TempFileManager } from "../TempFileManager.js";
import {InstantiationError} from "@errors/InstantiationError";

export class ZipExtractor extends BaseArchiveExtractor
{
    private static instance: ZipExtractor;

    private constructor(enforce: () => void)
    {
        super();

        if (enforce !== Enforce)
        {
            throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use ZipExtractor.getInstance() instead of new.");
        }
    }

    /**
     * Gets the singleton instance of ZipExtractor.
     *
     * @returns The singleton instance of ZipExtractor.
     */

    static getInstance(): ZipExtractor
    {
        if (!ZipExtractor.instance)
        {
            ZipExtractor.instance = new ZipExtractor(Enforce);
        }

        return ZipExtractor.instance;
    }

    async extract(jobId: string, raw: Buffer, compressedSize: number, fieldSpec: string[], batchId: string, password?: string): Promise<Record<string, unknown>[]>
    {
        const tmp: string = await TempFileManager.createTempFile(raw, ".zip");
        const zip = new NodeStreamZip.async({ file: tmp, password: password || undefined });
        const entries = await zip.entries();

        if (Object.keys(entries).length > settings.ARCHIVE_MAX_ENTRIES)
        {
            throw new BombError(`ZIP has ${Object.keys(entries).length} entries > cap ${settings.ARCHIVE_MAX_ENTRIES}`);
        }

        const out: Record<string, unknown>[] = [];
        let totalUncompressed: number = 0;

        for (const [name, entry] of Object.entries(entries))
        {
            if (entry.isDirectory) continue;
            const data: Buffer = await zip.entryData(name);
            totalUncompressed += data.length;
            CompressionGuard.checkRatio(compressedSize, totalUncompressed);
            const [url, size] = await this.entryStore.storeEntry(jobId, name, Buffer.from(data));
            out.push(this.entryStore.makeEntryEvent(jobId, batchId, url, name, size, fieldSpec));
        }

        await zip.close();
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
