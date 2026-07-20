import { ICreateJobRequest, ICreateJobResponse, IJobResponse, IStuckJobsResponse, IProvidePasswordRequest, IMarkFailedRequest, IRetryJobRequest } from "@service/job_service/io/IJob.js";
import { JobStatus } from "@shared/models/job.js";

/**
 * Job Service business-logic interface.
 */
export interface JobServiceService {
  createJob(request: ICreateJobRequest): Promise<ICreateJobResponse>;
  findStuckJobs(thresholdMinutes: number): Promise<IStuckJobsResponse>;
  getJob(jobId: string): Promise<IJobResponse | null>;
  getBatchJobs(batchId: string): Promise<IJobResponse[]>;
  providePassword(jobId: string, request: IProvidePasswordRequest): Promise<void>;
  releaseHold(jobId: string): Promise<void>;
  markFailed(jobId: string, request: IMarkFailedRequest): Promise<void>;
  retryJob(jobId: string, request: IRetryJobRequest): Promise<void>;
}
