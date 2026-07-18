export interface RetryRequest {
  job_id: string;
  [key: string]: any;
}

export interface RetryResponse {
  success: boolean;
  error?: string;
  data?: any;
}

export interface IRetry {
  processRetry(req: RetryRequest): Promise<RetryResponse>;
}
