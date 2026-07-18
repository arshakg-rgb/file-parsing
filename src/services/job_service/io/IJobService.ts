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
