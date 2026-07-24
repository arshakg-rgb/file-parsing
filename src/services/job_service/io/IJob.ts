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
 * Job response payload.
 */
export interface IJobResponse {
  job_id: string;
  batch_id: string | null | undefined;
  status: string;
  counts: JobCounts;
  timings: JobTimings;
  output_paths: string[];
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
