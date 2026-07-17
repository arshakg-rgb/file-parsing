import { IJobService, JobEvent, JobServiceConfig } from "./io/IJobService.js";
import { InstantiationError } from "../../errors/InstantiationError.js";

export class JobService implements IJobService {
  private static instance: JobService;
  private config: JobServiceConfig;
  private isRunning: boolean = false;

  private constructor(enforce: () => void, config: JobServiceConfig) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate JobService directly. Use getInstance()");
    }
    this.config = config;
  }

  public static getInstance(config?: JobServiceConfig): JobService {
    if (!JobService.instance) {
      if (!config) {
        throw new Error("JobService requires config on first instantiation");
      }
      JobService.instance = new JobService(Enforce, config);
    }
    return JobService.instance;
  }

  public async initialize(): Promise<void> {
    console.log("Initializing JobService...");
    // Database initialization will be done here
    // await initializeDatabase();
  }

  public async startEventConsumer(): Promise<void> {
    if (this.isRunning) {
      console.warn("Event consumer is already running");
      return;
    }

    this.isRunning = true;
    console.log("Starting event consumer loop...");
    
    // Event consumer loop will be implemented here
    // await this.eventConsumerLoop();
  }

  public async handleEvent(event: JobEvent): Promise<void> {
    console.log("Handling event:", event);
    // Event handling logic will be implemented here
  }

  public async shutdown(): Promise<void> {
    console.log("Shutting down JobService...");
    this.isRunning = false;
  }
}

function Enforce(): void {}
