import { IJobService, JobRequest, JobResponse } from "./io/IJobService.js";
import JobServiceImpl from "./impl/JobServiceImpl.js";

/**
 * Legacy JobService class - now a thin wrapper around JobServiceImpl
 * This maintains backward compatibility while using the new service pattern
 */
export class JobService implements IJobService 
{
  private service: JobServiceImpl;

  constructor() 
{
    this.service = JobServiceImpl.getInstance();
  }

  async processJob(req: JobRequest): Promise<JobResponse> 
{
    return this.service.processJob(req);
  }

  async start(): Promise<void> 
{
    return this.service.start();
  }

  async stop(): Promise<void> 
{
    return this.service.stop();
  }

  eventConsumerLoop(): Promise<void> 
{
    return this.service.eventConsumerLoop();
  }

  initializeDatabase(): Promise<void> 
{
    return this.service.initializeDatabase();
  }
}

export { default as JobServiceImpl } from "./impl/JobServiceImpl.js";
export { IJobService, JobRequest, JobResponse } from "./io/IJobService.js";

export default JobService;
