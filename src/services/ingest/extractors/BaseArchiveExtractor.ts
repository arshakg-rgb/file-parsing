import { GcsEntryStore } from "../GcsEntryStore.js";
import { TempFileManager } from "../TempFileManager.js";

/**
 * Common contract for the non-RAR (buffer-based) archive extractors.
 *
 * Subclasses are singletons (each exposes its own static getInstance()); this
 * base class wires up the shared GcsEntryStore singleton and provides a
 * buffer-to-file bridge so callers can use either the legacy buffer API or the
 * new file-path streaming API.
 */

export abstract class BaseArchiveExtractor
{
    protected readonly entryStore: GcsEntryStore;

    protected constructor()
    {
        this.entryStore = GcsEntryStore.getInstance();
    }

    abstract extractFromFile(
        jobId: string,
        filePath: string,
        compressedSize: number,
        fieldSpec: string[],
        batchId: string,
        password?: string
    ): Promise<Record<string, unknown>[]>;

    /**
   * Legacy buffer entry point. Writes the buffer to a temp file and delegates
   * to the streaming file implementation.
   */
    async extract(jobId: string, raw: Buffer, compressedSize: number, fieldSpec: string[], batchId: string, password?: string): Promise<Record<string, unknown>[]>
    {
        const tmp: string = await TempFileManager.createTempFile(raw, ".tmp");
        try
        {
            return await this.extractFromFile(jobId, tmp, compressedSize, fieldSpec, batchId, password);
        }
        finally
        {
            await TempFileManager.removeFile(tmp);
        }
    }
}
