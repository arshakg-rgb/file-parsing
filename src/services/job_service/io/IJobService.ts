export interface JobRequest {
  job_id: string;
  [key: string]: any;
}

export interface JobResponse {
  success: boolean;
  error?: string;
  data?: any;
}

export interface IJobService {
  processJob(req: JobRequest): Promise<JobResponse>;
  start(): Promise<void>;
  stop(): Promise<void>;
}
