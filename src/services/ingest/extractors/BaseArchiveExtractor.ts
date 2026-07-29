import { GcsEntryStore } from "../GcsEntryStore.js";

/**
 * Common contract for the non-RAR (buffer-based) archive extractors.
 *
 * Subclasses are singletons (each exposes its own static getInstance()); this
 * base class just wires up the shared GcsEntryStore singleton so subclasses
 * never need to take it as a constructor argument.
 */

export abstract class BaseArchiveExtractor
{
    protected readonly entryStore: GcsEntryStore;

    protected constructor()
    {
        this.entryStore = GcsEntryStore.getInstance();
    }

    abstract extract(jobId: string, raw: Buffer, compressedSize: number, fieldSpec: string[], batchId: string, password?: string): Promise<Record<string, unknown>[]>;
}
