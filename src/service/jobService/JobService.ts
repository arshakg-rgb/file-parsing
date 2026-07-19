import { IJobService, JobEvent, JobServiceConfig } from "./io/IJobService.js";
import { InstantiationError } from "@errors/InstantiationError.js";

/**
 * Performs the enforce operation.
 */
function Enforce(): void {}

/**
 * JobService provides service-level operations.
 */
export class JobService implements IJobService {
    /**
   * Singleton instance
   * @private
   */
  private static instance: JobService;
    /**
   * Config
   * @private
   */
  private config: JobServiceConfig;
    /**
   * Is Running
   * @private
   */
  private isRunning: boolean = false;

    /**
   * Constructs a new JobService instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @param config - The configuration object
   * @throws Error if instantiated directly
   */
  private constructor(enforce: () => void, config: JobServiceConfig) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate JobService directly. Use getInstance()");
    }
    this.config = config;
  }

    /**
   * Gets the single instance of the JobService class.
   * @returns The single instance of the class
   */
  public static getInstance(config?: JobServiceConfig): JobService {
    if (!JobService.instance) {
      if (!config) {
        throw new Error("JobService requires config on first instantiation");
      }
      JobService.instance = new JobService(Enforce, config);
    }
    return JobService.instance;
  }

    /**
   * Initializes the service
   */
  public async initialize(): Promise<void> {
    console.log("Initializing JobService...");
    // Database initialization will be done here
    // await initializeDatabase();
  }

    /**
   * Starts event consumer
   */
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

    /**
   * Handles event
   * @param event - The event
   */
  public async handleEvent(event: JobEvent): Promise<void> {
    console.log("Handling event:", event);
    // Event handling logic will be implemented here
  }

    /**
   * Stops the service gracefully
   */
  public async shutdown(): Promise<void> {
    console.log("Shutting down JobService...");
    this.isRunning = false;
  }
}

