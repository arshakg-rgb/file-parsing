export interface ArchiveEntryRequest {
  job_id: string;
  batchId: string;
  archive_s3_url: string;
  entry_name: string;
  entry_size: number;
  field_spec: string[];
  password?: string;
  archive_type: string;
  nesting_depth: number;
}

export interface ArchiveEntryResponse {
  success: boolean;
  error?: string;
}

export interface IArchiveEntryConsumer {
  processEntry(req: ArchiveEntryRequest): Promise<ArchiveEntryResponse>;
}

export interface NestedArchiveEntry {
  pending?: boolean;
  entry_name?: string;
  entry_size?: number;
  [key: string]: unknown;
}

export const LogEvent = {
  PROCESSING: "archive_entry_processing",
  NESTED_DETECTION_FAILED: "archive_entry_nested_detection_failed",
  NESTED_DETECTED: "archive_entry_nested_detected",
  NESTED_FAILED: "archive_entry_nested_failed",
  NESTED_CLEANUP_FAILED: "archive_entry_nested_cleanup_failed",
  COMPLETED: "archive_entry_completed",
  FAILED: "archive_entry_failed",
  DOWNLOAD_START: "archive_entry_download_start",
  DOWNLOAD_STREAM_ERROR: "archive_entry_download_stream_error",
  DOWNLOAD_WRITE_ERROR: "archive_entry_download_write_error",
  DOWNLOAD_COMPLETE: "archive_entry_download_complete",
  EXTRACT_START: "archive_entry_extract_start",
  EXTRACT_STDOUT_ERROR: "archive_entry_extract_stdout_error",
  EXTRACT_WRITE_ERROR: "archive_entry_extract_write_error",
  EXTRACT_SPAWN_ERROR: "archive_entry_extract_spawn_error",
  EXTRACT_FAILED: "archive_entry_extract_failed",
  EXTRACT_COMPLETE: "archive_entry_extract_complete",
  CLEANUP: "archive_entry_cleanup",
  CLEANUP_FAILED: "archive_entry_cleanup_failed",
} as const;

export interface ArchiveEntryConsumerOptions {
  queueUrl: string;
  maxMessages?: number;
  errorBackoffMs?: number;
}
