import { settings } from "@shared/Settings.js";
import { parseGcsUrl as parseS3Url, readFull } from "@shared/GcsUtils.js";
import { BombError } from "@errors/BombError.js";
import { ArchiveTypeDetector } from "./ArchiveTypeDetector.js";
import { GcsTransferService } from "./GcsTransferService.js";
import { ZipExtractor } from "./extractors/ZipExtractor.js";
import { GzExtractor } from "./extractors/GzExtractor.js";
import { TarExtractor } from "./extractors/TarExtractor.js";
import { SevenZipExtractor } from "./extractors/SevenZipExtractor.js";
import { RarExtractor } from "./extractors/RarExtractor.js";
import {InstantiationError} from "@errors/InstantiationError";

/**
 * Top-level orchestrator: wires together the transfer service and every
 * format-specific extractor, and dispatches extractArchiveToS3 calls to the
 * right one based on archiveType. This is the single entry point consumers
 * should depend on.
 *
 * Usage: IngestServiceImpl.getInstance().extractArchiveToS3(...)
 * There is no free-function export anymore — every caller must go through
 * the singleton.
 */

export class IngestServiceImpl
{
  private static instance: IngestServiceImpl;

  private constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Error: Instantiation failed: Use IngestServiceImpl.getInstance() instead of new.");
    }
  }

  /**
   * Gets the singleton instance of IngestServiceImpl.
   *
   * @returns The singleton instance of IngestServiceImpl.
   */

  static getInstance(): IngestServiceImpl
  {
    if (!IngestServiceImpl.instance)
    {
      IngestServiceImpl.instance = new IngestServiceImpl(Enforce);
    }

    return IngestServiceImpl.instance;
  }

  detectArchiveType(header: Buffer): string | null
  {
    return ArchiveTypeDetector.detect(header);
  }

  async fetchUrlToS3(jobId: string, url: string): Promise<[string, number]>
  {
    return GcsTransferService.getInstance().fetchUrlToS3(jobId, url);
  }

  async listS3Prefix(prefixUrl: string): Promise<[string, number][]>
  {
    return GcsTransferService.getInstance().listS3Prefix(prefixUrl);
  }

  async extractArchiveToS3(jobId: string, s3Url: string, archiveType: string, fieldSpec: string[], batchId: string, password?: string, depth = 0): Promise<Record<string, unknown>[]>
  {
    if (depth > settings.ARCHIVE_MAX_NESTING_DEPTH)
    {
      throw new BombError(`Archive nesting depth ${depth} exceeds maximum ${settings.ARCHIVE_MAX_NESTING_DEPTH}`);
    }

    const [bucket, key] = parseS3Url(s3Url);

    if (archiveType === "rar")
    {
      return RarExtractor.getInstance().extract(jobId, s3Url, bucket, key, fieldSpec, batchId, password, depth);
    }

    const raw: Buffer = await readFull(bucket, key);
    const compressedSize: number = raw.length;

    switch (archiveType)
    {
      case "zip":
        return ZipExtractor.getInstance().extract(jobId, raw, compressedSize, fieldSpec, batchId, password);
      case "gz":
        return GzExtractor.getInstance().extract(jobId, raw, compressedSize, fieldSpec, batchId);
      case "tar":
        return TarExtractor.getInstance().extract(jobId, raw, compressedSize, fieldSpec, batchId);
      case "7z":
        return SevenZipExtractor.getInstance().extract(jobId, raw, compressedSize, fieldSpec, batchId, password);
      default:
        throw new Error(`Unsupported archive type: ${archiveType}`);
    }
  }
}

/**
 * Function to enforce the Singleton pattern.
 */
function Enforce(): void
{
}
