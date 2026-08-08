import fs from "node:fs";
import fsPromises from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { randomUUID } from "node:crypto";
import zlib from "zlib";
import { BaseArchiveExtractor } from "./BaseArchiveExtractor.js";
import { CompressionGuard } from "../CompressionGuard.js";
import {InstantiationError} from "@errors/InstantiationError";

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

    async extractFromFile(jobId: string, filePath: string, compressedSize: number, fieldSpec: string[], batchId: string): Promise<Record<string, unknown>[]>
    {
        const outPath: string = path.join(os.tmpdir(), `${randomUUID()}.dat`);
        try
        {
            const gunzip = zlib.createGunzip();
            await pipeline(fs.createReadStream(filePath), gunzip, fs.createWriteStream(outPath));
            const stat = await fsPromises.stat(outPath);
            CompressionGuard.checkRatio(compressedSize, stat.size);
            const name = `decompressed_${jobId}.dat`;
            const [url, size] = await this.entryStore.storeEntryFromFile(jobId, name, outPath);
            return [this.entryStore.makeEntryEvent(jobId, batchId, url, name, size, fieldSpec)];
        }
        finally
        {
            await fsPromises.unlink(outPath).catch(() => {});
        }
    }
}

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void
{
}
