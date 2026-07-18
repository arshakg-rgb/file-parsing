export interface RetryRequest {
  job_id: string;
  [key: string]: unknown;
}

export interface RetryResponse {
  success: boolean;
  error?: string;
  data?: unknown;
}

export interface IRetry {
  processRetry(req: RetryRequest): Promise<RetryResponse>;
}
