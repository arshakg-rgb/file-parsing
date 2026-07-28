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
 */

export const OBJECT_SIZE_MAX_ATTEMPTS = 5;
export const OBJECT_SIZE_RETRY_DELAY_MS = 1000;
