import express, { Request, Response, NextFunction } from "express";
import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import MySqlManager from "@config/db/MySqlManager.js";
import { receiveMessages, deleteMessage } from "@shared/QueueService.js";
import { JobEvent, EventType } from "@shared/models/events.js";
import { handleEvent } from "@service/job_service/stateMachine.js";
import { JobServiceRouter } from "@service/job_service/JobServiceRouter.js";
import { createTables } from "@shared/DatabaseManager.js";
import { JobService } from "@service/job_service/JobService.js";
import { IJobService, JobRequest, JobResponse } from "@service/job_service/io/IJobService.js";

/**
 * JobServiceImpl is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class JobServiceImpl extends ServiceManager implements JobService {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: JobServiceImpl;
    /**
   * The Express application instance
   * @private
   */
  private app: express.Express;
    /**
   * Db Manager
   * @private
   */
  private dbManager: MySqlManager;

    /**
   * Constructs a new JobServiceImpl instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate JobServiceImpl directly. Use getInstance()");
    }
    super(enforce);
    
    this.app = express();
    this.dbManager = MySqlManager.getInstance();
    this.setupApp();
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
   * Sets up app
   */
  private setupApp(): void {
    this.app.use(express.json());
    this.app.use("/v1", JobServiceRouter.getInstance().getRouter());

    this.app.get("/health", (_req: Request, res: Response) => {
      res.json({ status: "healthy", timestamp: new Date().toISOString() });
    });

    this.app.get("/health/db", async (_req: Request, res: Response) => {
      try {
        await this.dbManager.sequelize.authenticate();
        res.json({ status: "healthy", database: "connected", timestamp: new Date().toISOString() });
      } catch (err) {
        res.status(500).json({ 
          status: "unhealthy", 
          database: "disconnected",
          timestamp: new Date().toISOString(),
          error: err instanceof Error ? err.message : String(err)
        });
      }
    });

    this.app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
      console.error("error", err);
      res.status(500).json({ detail: err.message });
    });
  }

    /**
   * Processes job
   * @param req - The HTTP request object
   * @returns A promise that resolves to the result
   */
  public async processJob(req: JobRequest): Promise<JobResponse> {
    // Placeholder implementation
    return { success: true };
  }

    /**
   * Performs the event consumer loop operation.
   */
  public async eventConsumerLoop(): Promise<void> {
    const config = this.getConfig();
    while (true) {
      try {
        const messages = await receiveMessages<JobEvent>(
          config.settings.JOB_EVENTS_QUEUE_URL,
          (body) => JSON.parse(body) as JobEvent,
          10,
          5
        );
        for (const { payload, receiptHandle } of messages) {
          try {
            await handleEvent(payload);
            await deleteMessage(config.settings.JOB_EVENTS_QUEUE_URL, receiptHandle);
          } catch (exc) {
            const errorStr = String(exc);
            if (errorStr.includes("Job") && (errorStr.includes("not found") || errorStr.includes("cannot transition"))) {
              console.error("event_processing_error_ack", { error: errorStr, body: payload, action: "ack_to_prevent_retry" });
              await deleteMessage(config.settings.JOB_EVENTS_QUEUE_URL, receiptHandle);
            } else {
              console.error("event_processing_error", { error: errorStr, body: payload });
            }
          }
        }
      } catch (exc) {
        console.error("event_consumer_loop_error", { error: String(exc) });
        await new Promise((r) => setTimeout(r, 5000));
      }
    }
  }

    /**
   * Initializes database
   */
  public async initializeDatabase(): Promise<void> {
    try {
      console.log("Running database migration...");
      await this.dbManager.initialize();
      await createTables();
      console.log("Database migration completed successfully");
    } catch (err) {
      console.error("Database migration failed:", err);
      throw err;
    }
  }

    /**
   * Starts the operation
   */
  public async start(): Promise<void> {
    const config = this.getConfig();
    const PORT = process.env.PORT || 8080;
    
    // Start listening immediately for health checks
    this.app.listen(PORT, () => {
      console.log(`Job Service listening on port ${PORT}`);
    });
    
    // Initialize database and start consumer loop in background
    await this.initializeDatabase();
    console.log("Database initialized successfully");
    this.eventConsumerLoop();
  }

    /**
   * Stops the operation
   */
  public async stop(): Promise<void> {
    // Placeholder for cleanup
  }
}

export default JobServiceImpl;
