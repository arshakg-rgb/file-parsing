import { IJobService, JobRequest, JobResponse } from "@service/job_service/io/IJobService.js";
import JobServiceImpl from "@service/job_service/impl/JobServiceImpl.js";

/**
 * Legacy JobService class - now a thin wrapper around JobServiceImpl
 * This maintains backward compatibility while using the new service pattern
 */
export class JobService implements IJobService {
    /**
   * Service
   * @private
   */
  private service: JobServiceImpl;

    /**
   * Constructs a new JobService instance.
   */
  constructor() {
    this.service = JobServiceImpl.getInstance();
  }

    /**
   * Processes job
   * @param req - The HTTP request object
   * @returns A promise that resolves to the result
   */
  async processJob(req: JobRequest): Promise<JobResponse> {
    return this.service.processJob(req);
  }

    /**
   * Starts the operation
   */
  async start(): Promise<void> {
    return this.service.start();
  }

    /**
   * Stops the operation
   */
  async stop(): Promise<void> {
    return this.service.stop();
  }

    /**
   * Performs the event consumer loop operation.
   */
  eventConsumerLoop(): Promise<void> {
    return this.service.eventConsumerLoop();
  }

    /**
   * Initializes database
   */
  initializeDatabase(): Promise<void> {
    return this.service.initializeDatabase();
  }
}

// Re-export the new service for direct use
export { default as JobServiceImpl } from "@service/job_service/impl/JobServiceImpl.js";
export { IJobService, JobRequest, JobResponse } from "@service/job_service/io/IJobService.js";

export default JobService;
