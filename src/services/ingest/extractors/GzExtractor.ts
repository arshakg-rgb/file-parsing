import zlib from "zlib";
import { promisify } from "util";
import { BaseArchiveExtractor } from "./BaseArchiveExtractor.js";
import { CompressionGuard } from "../CompressionGuard.js";
import {InstantiationError} from "@errors/InstantiationError";
const gunzip = promisify(zlib.gunzip);

export class GzExtractor extends BaseArchiveExtractor
{
    private static instance: GzExtractor;

    private constructor(enforce: () => void)
    {
        super();

        if (enforce !== Enforce)
        {
            throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use GzExtractor.getInstance() instead of new.");
        }
    }

    /**
     * Gets the singleton instance of GzExtractor.
     *
     * @returns The singleton instance of GzExtractor.
     */

    static getInstance(): GzExtractor
    {
        if (!GzExtractor.instance)
        {
            GzExtractor.instance = new GzExtractor(Enforce);
        }

        return GzExtractor.instance;
    }

    async extract(jobId: string, raw: Buffer, compressedSize: number, fieldSpec: string[], batchId: string): Promise<Record<string, unknown>[]>
    {
        const data = await gunzip(raw);
        CompressionGuard.checkRatio(compressedSize, data.length);
        const name = `decompressed_${jobId}.dat`;
        const [url, size] = await this.entryStore.storeEntry(jobId, name, data);
        return [this.entryStore.makeEntryEvent(jobId, batchId, url, name, size, fieldSpec)];
    }
}

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void
{
}
