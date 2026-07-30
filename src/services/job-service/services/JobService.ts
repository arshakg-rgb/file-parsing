import { ICreateJobRequest, ICreateJobResponse, IJobResponse, IStuckJobsResponse, IProvidePasswordRequest, IMarkFailedRequest, IRetryJobRequest } from "@service/job-service/io/IJob.js";

/**
 * Job Service business-logic interface.
 */
export interface JobService {
  createJob(request: ICreateJobRequest): Promise<ICreateJobResponse>;
  findStuckJobs(thresholdMinutes: number): Promise<IStuckJobsResponse>;
  getJob(jobId: string): Promise<IJobResponse | null>;
  getBatchJobs(batchId: string): Promise<IJobResponse[]>;
  providePassword(jobId: string, request: IProvidePasswordRequest): Promise<void>;
  releaseHold(jobId: string): Promise<void>;
  markFailed(jobId: string, request: IMarkFailedRequest): Promise<void>;
  retryJob(jobId: string, request: IRetryJobRequest): Promise<void>;
}
