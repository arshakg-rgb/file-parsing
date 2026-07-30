export interface JobRequest {
  job_id: string;
  [key: string]: unknown;
}

export interface JobResponse {
  success: boolean;
  error?: string;
  data?: unknown;
}

export interface IJobService {
  processJob(req: JobRequest): Promise<JobResponse>;
  start(): Promise<void>;
  stop(): Promise<void>;
}

/**
 * Default HTTP port used when the PORT environment variable is not set.
 */

export const DEFAULT_PORT = 8080;

/**
 * Max number of messages to pull per receive call in the event consumer loop.
 */

export const EVENT_BATCH_SIZE = 10;

/**
 * Visibility timeout / long-poll wait (seconds) for the event consumer loop.
 */

export const EVENT_POLL_WAIT_SECONDS = 5;

/**
 * Backoff delay (ms) after an unexpected failure in the event consumer loop.
 */

export const EVENT_LOOP_ERROR_BACKOFF_MS = 5_000;

/**
 * Error message fragments that indicate a job event is unrecoverable and should be
 * acknowledged (deleted) rather than retried, since retrying can never succeed.
 */

export const NON_RETRYABLE_JOB_ERROR_MARKERS = ["not found", "cannot transition"] as const;
