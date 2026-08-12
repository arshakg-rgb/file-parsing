/**
 * The gcs retries
 */

export const GCS_RETRIES = 3;

/**
 * The gcs timeout ms
 */

export const GCS_TIMEOUT_MS = 7200000;

/**
 * The gcs large copy threshold (100 MB)
 */
export const GCS_LARGE_COPY_THRESHOLD_BYTES = 100 * 1024 * 1024;

/**
 * The gcs stream copy progress log interval (100 MB)
 */
export const GCS_COPY_LOG_INTERVAL_BYTES = 100 * 1024 * 1024;


export interface LineState {
    inQuote: boolean;
}
