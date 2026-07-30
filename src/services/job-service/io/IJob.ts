import { SourceType, JobStatus, JobCounts, JobTimings } from "@shared/models/job.js";

/**
 * Job creation request payload.
 */
export interface ICreateJobRequest {
  source_type: SourceType;
  source_ref?: string;
  field_spec: unknown;
  batch_id?: string;
  column_map?: unknown;
}

/**
 * Job creation response payload.
 */
export interface ICreateJobResponse {
  job_id: string;
  status: JobStatus;
  presigned_put_url?: string;
  message_id?: string;
}

/**
 * Single-call upload-and-create request payload.
 */
export interface IUploadAndCreateJobRequest {
  source_buffer: Buffer;
  mimetype: string;
  field_spec: unknown;
  column_map?: unknown;
  batch_id?: string;
}

/**
 * Job response payload.
 */
export interface IJobResponse {
  job_id: string;
  batch_id: string | null | undefined;
  status: string;
  counts: JobCounts;
  timings: JobTimings;
  error: string | null | undefined;
}

/**
 * Stuck jobs response payload.
 */
export interface IStuckJobsResponse {
  stuck_jobs: unknown[];
  count: number;
  threshold_minutes: number;
}

/**
 * Password submission request payload.
 */
export interface IProvidePasswordRequest {
  password: string;
}

/**
 * Manual retry request payload.
 */
export interface IRetryJobRequest {
  target_status: JobStatus;
}

/**
 * Manual fail request payload.
 */
export interface IMarkFailedRequest {
  reason?: string;
}

/**
 * Upload parsed CSV to a user-supplied destination request payload.
 */
export interface IUploadCsvRequest {
  destination_url: string;
}

/**
 * Upload parsed CSV response payload.
 */
export interface IUploadCsvResponse {
  csv_output_path: string | null | undefined;
  destination_url: string;
  bytes: number;
}

/**
 * Download parsed CSV response payload.
 */
export interface IDownloadCsvResponse {
  csv_output_path: string;
  filename: string;
  download_url: string;
}

/**
 * A single job_logs audit-trail entry.
 */
export interface IJobLogEntry {
  event_type: string;
  stage: string | null | undefined;
  template_id: string | null | undefined;
  message: string | null | undefined;
  metadata: Record<string, unknown> | null | undefined;
  created_at: Date | undefined;
}
