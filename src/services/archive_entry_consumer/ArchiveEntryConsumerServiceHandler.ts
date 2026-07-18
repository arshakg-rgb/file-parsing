import { IArchiveEntryConsumer, ArchiveEntryRequest, ArchiveEntryResponse } from "./io/IArchiveEntryConsumer.js";
import ArchiveEntryConsumerServiceImpl from "./impl/ArchiveEntryConsumerServiceImpl.js";

/**
 * Legacy ArchiveEntryConsumerService class - now a thin wrapper around ArchiveEntryConsumerServiceImpl
 * This maintains backward compatibility while using the new service pattern
 */
export class ArchiveEntryConsumerService implements IArchiveEntryConsumer {
  private service: ArchiveEntryConsumerServiceImpl;

  constructor() {
    this.service = ArchiveEntryConsumerServiceImpl.getInstance();
  }

  async processEntry(req: ArchiveEntryRequest): Promise<ArchiveEntryResponse> {
    return this.service.processEntry(req);
  }

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
export { default as ArchiveEntryConsumerServiceImpl } from "./impl/ArchiveEntryConsumerServiceImpl.js";
export { IArchiveEntryConsumer, ArchiveEntryRequest, ArchiveEntryResponse } from "./io/IArchiveEntryConsumer.js";

// Backward compatibility wrappers
const archiveService = new ArchiveEntryConsumerService();

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
