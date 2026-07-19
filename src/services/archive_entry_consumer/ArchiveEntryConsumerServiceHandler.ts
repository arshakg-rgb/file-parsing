import { IArchiveEntryConsumer, ArchiveEntryRequest, ArchiveEntryResponse } from "@service/archive_entry_consumer/io/IArchiveEntryConsumer.js";
import ArchiveEntryConsumerServiceImpl from "@service/archive_entry_consumer/impl/ArchiveEntryConsumerServiceImpl.js";

/**
 * Legacy ArchiveEntryConsumerService class - now a thin wrapper around ArchiveEntryConsumerServiceImpl
 * This maintains backward compatibility while using the new service pattern
 */
export class ArchiveEntryConsumerService implements IArchiveEntryConsumer {
    /**
   * Service
   * @private
   */
  private service: ArchiveEntryConsumerServiceImpl;

    /**
   * Constructs a new ArchiveEntryConsumerService instance.
   */
  constructor() {
    this.service = ArchiveEntryConsumerServiceImpl.getInstance();
  }

    /**
   * Processes entry
   * @param req - The HTTP request object
   * @returns A promise that resolves to the result
   */
  async processEntry(req: ArchiveEntryRequest): Promise<ArchiveEntryResponse> {
    return this.service.processEntry(req);
  }

    /**
   * Extracts single rar entry
   * @param jobId - The job identifier
   * @param archiveS3Url - The archive s3 url
   * @param entryName - The entry name
   * @param password - The password
   * @param fieldSpec - The field spec
   * @returns A promise that resolves to the result
   */
  async extractSingleRarEntry(
    jobId: string,
    archiveS3Url: string,
    entryName: string,
    password: string | undefined,
    fieldSpec: string[]
  ): Promise<{ s3Url: string; size: number }> {
    return this.service.extractSingleRarEntry(jobId, archiveS3Url, entryName, password, fieldSpec);
  }
}

// Re-export the new service for direct use
export { default as ArchiveEntryConsumerServiceImpl } from "@service/archive_entry_consumer/impl/ArchiveEntryConsumerServiceImpl.js";
export { IArchiveEntryConsumer, ArchiveEntryRequest, ArchiveEntryResponse } from "@service/archive_entry_consumer/io/IArchiveEntryConsumer.js";

// Backward compatibility wrappers
const archiveService = new ArchiveEntryConsumerService();

/**
 * Extracts single rar entry
 * @param jobId - The job identifier
 * @param archiveS3Url - The archive s3 url
 * @param entryName - The entry name
 * @param password - The password
 * @param fieldSpec - The field spec
 * @returns A promise that resolves to the result
 */
export async function extractSingleRarEntry(
  jobId: string,
  archiveS3Url: string,
  entryName: string,
  password: string | undefined,
  fieldSpec: string[]
): Promise<{ s3Url: string; size: number }> {
  return archiveService.extractSingleRarEntry(jobId, archiveS3Url, entryName, password, fieldSpec);
}

export default ArchiveEntryConsumerService;
