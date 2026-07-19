import { IJobService, JobEvent, JobServiceConfig } from "../io/IJobService.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { receiveMessages, deleteMessage } from "@shared/QueueService.js";
import Config from "@config/system-config/Config.js";

/**
 * Performs the enforce operation.
 */
function Enforce(): void {}

/**
 * The config
 */
const config = Config.getInstance();

/**
 * JobServiceImpl implements the service interface.
 */
export class JobServiceImpl implements IJobService {
    /**
   * Singleton instance
   * @private
   */
  private static instance: JobServiceImpl;
    /**
   * Is Running
   * @private
   */
  private isRunning: boolean = false;

    /**
   * Constructs a new JobServiceImpl instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate JobServiceImpl directly. Use getInstance()");
    }
  }

    /**
   * Gets the single instance of the JobServiceImpl class.
   * @returns The single instance of the class
   */
  public static getInstance(): JobServiceImpl {
    if (!JobServiceImpl.instance) {
      JobServiceImpl.instance = new JobServiceImpl(Enforce);
    }
    return JobServiceImpl.instance;
  }

    /**
   * Initializes the service
   */
  public async initialize(): Promise<void> {
    console.log("Initializing JobServiceImpl...");
    // Database initialization
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
    await this.eventConsumerLoop();
  }

    /**
   * Performs the event consumer loop operation.
   */
  private async eventConsumerLoop(): Promise<void> {
    while (this.isRunning) {
      try {
        const messages = await receiveMessages<JobEvent>(
          config.settings.JOB_EVENTS_QUEUE_URL,
          (body) => JSON.parse(body) as JobEvent,
          10,
          5
        );
        
        for (const { payload, receiptHandle } of messages) {
          try {
            await this.handleEvent(payload);
            await deleteMessage(config.settings.JOB_EVENTS_QUEUE_URL, receiptHandle);
          } catch (exc) {
            const errorStr = String(exc);
            console.error("event_processing_error", { error: errorStr, body: payload });
          }
        }
      } catch (exc) {
        console.error("event_consumer_loop_error", { error: String(exc) });
        await new Promise((r) => setTimeout(r, 5000));
      }
    }
  }

    /**
   * Handles event
   * @param event - The event
   */
  public async handleEvent(event: JobEvent): Promise<void> {
    console.log("Handling event:", event);
    // Import and use the actual event handler from stateMachine
    // const { handleEvent } = await import("../stateMachine.js");
    // await handleEvent(event);
  }

    /**
   * Stops the service gracefully
   */
  public async shutdown(): Promise<void> {
    console.log("Shutting down JobServiceImpl...");
    this.isRunning = false;
  }
}

