import { SourceType } from "@shared/models/job.js";

/**
 * Source types that resolve by copying/reading an already-uploaded GCS object
 */

export const UPLOAD_LIKE_SOURCE_TYPES = [SourceType.UPLOAD, SourceType.ARCHIVE_ENTRY];

/**
 * Matches a gs:// or s3:// URL
 */

export const GCS_OR_S3_URL_PATTERN = /^gs:\/\/|^s3:\/\//i;

/**
 * Matches an http(s) URL
 */

export const HTTP_URL_PATTERN = /^https?:\/\//i;

/**
 * Substrings in an extraction error that indicate a password/encryption problem rather than a generic failure
 */

export const PASSWORD_ERROR_KEYWORDS = ["password", "encrypted", "bad password"];

/**
 * Max attempts when retrying an eventually-consistent GCS objectSize lookup
 * for upload sources. Default gives a ~15 minute window so clients uploading
 * very large files have time to finish the PUT before ingest gives up.
 */

export const OBJECT_SIZE_MAX_ATTEMPTS = 180;
export const OBJECT_SIZE_RETRY_DELAY_MS = 5000;

/** Archive magic-byte signatures used by ArchiveTypeDetector. */
export const MAGIC_ZIP = Buffer.from("PK\x03\x04");
export const MAGIC_GZ = Buffer.from("\x1f\x8b");
export const MAGIC_7Z = Buffer.from("7z\xbc\xaf\x27\x1c");
export const MAGIC_RAR = Buffer.from("Rar!");

/**
 * RAR extraction runs full-file CLI-based extraction (see RarExtractor for
 * rationale), so its memory-safety limits are enforced directly there rather
 * than via the compression-ratio check used by the other formats.
 */
export const RAR_MAX_ARCHIVE_SIZE = 2.5 * 1024 * 1024 * 1024; // 2.5GB limit for RAR with 4Gi memory + GCS FUSE
export const RAR_MAX_INLINE_FILE_SIZE = 2 * 1024 * 1024 * 1024;
export const RAR_MAX_TOTAL_UNCOMPRESSED = 10 * 1024 * 1024 * 1024;

export interface RarFileEntry {
    name: string;
    size: number;
}
