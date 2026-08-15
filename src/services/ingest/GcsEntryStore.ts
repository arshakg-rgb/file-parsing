import { randomUUID } from "crypto";
import fs from "fs/promises";
import path from "path";
import { settings } from "@shared/Settings.js";
import { CompressionGuard } from "./CompressionGuard.js";
import {InstantiationError} from "@errors/InstantiationError";
import {GcsUtils} from "@shared/GcsUtils";

/**
 * Handles persisting extracted entries to GCS and building the resulting
 * entry events. Shared by every extractor so storage/event-shape logic
 * lives in exactly one place.
 */

export class GcsEntryStore
{
    private static instance: GcsEntryStore;

    private constructor(enforce: () => void)
    {
        if (enforce !== Enforce)
        {
            throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use GcsEntryStore.getInstance() instead of new.");
        }
    }

    /**
     * Gets the singleton instance of GcsEntryStore.
     *
     * @returns The singleton instance of GcsEntryStore.
     */

    static getInstance(): GcsEntryStore
    {
        if (!GcsEntryStore.instance)
        {
            GcsEntryStore.instance = new GcsEntryStore(Enforce);
        }

        return GcsEntryStore.instance;
    }

    public sourceKeyFor(jobId: string, filename?: string): string
    {
        const baseName = filename
            ? (path.basename(filename).replace(/[#\s]+/g, "_") || "source")
            : "source";
        return `ingested/${jobId}/${baseName}`;
    }

    public async storeEntry(jobId: string, entryName: string, data: Buffer): Promise<[string, number]>
    {
        const safeName: string = path.basename(entryName).replace(/[#\s]+/g, "_") || "entry";
        const entryId = randomUUID();
        const s3Key = `ingested/${jobId}/entries/${entryId}/${safeName}`;
        await GcsUtils.getInstance().putObject(settings.DATA_BUCKET, s3Key, data);
        return [`gs://${settings.DATA_BUCKET}/${s3Key}`, data.length];
    }

    public async storeEntryFromFile(jobId: string, entryName: string, filePath: string): Promise<[string, number]>
    {
        const safeName: string = path.basename(entryName).replace(/[#\s]+/g, "_") || "entry";
        const entryId = randomUUID();
        const s3Key = `ingested/${jobId}/entries/${entryId}/${safeName}`;
        const stat = await fs.stat(filePath);
        await GcsUtils.getInstance().putObjectFromFile(settings.DATA_BUCKET, s3Key, filePath);
        return [`gs://${settings.DATA_BUCKET}/${s3Key}`, stat.size];
    }

    public makeEntryEvent(parentJobId: string, batchId: string, s3Url: string, name: string, size: number, fieldSpec: string[])
    {
        return { parent_job_id: parentJobId, batch_id: batchId, entry_s3_url: s3Url, entry_name: name, entry_size: size, field_spec: fieldSpec };
    }

    /**
     * Walk a directory of already-extracted files (recursively), enforcing the
     * compression-ratio cap as files accumulate, and storing each one to GCS as
     * an entry event. Shared by TarExtractor and SevenZipExtractor, which differ
     * only in how they populate extractDir.
     */
    public async collectExtractedFiles(extractDir: string, jobId: string, compressedSize: number, fieldSpec: string[], batchId: string): Promise<Record<string, unknown>[]>
    {
        const out: Record<string, unknown>[] = [];
        let totalUncompressed: number = 0;
        const files: string[] = await fs.readdir(extractDir, { recursive: true });
        for (const rel of files) {
            const fpath: string = path.join(extractDir, rel as string);
            const stat = await fs.stat(fpath);

            if (stat.isFile())
            {
                totalUncompressed += stat.size;
                CompressionGuard.checkRatio(compressedSize, totalUncompressed);
                const [url, size] = await this.storeEntryFromFile(jobId, rel as string, fpath);
                out.push(this.makeEntryEvent(jobId, batchId, url, rel as string, size, fieldSpec));
            }
        }
        return out;
    }
}

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void
{
}
