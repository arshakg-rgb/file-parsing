import express, { Request, Response, NextFunction } from "express";
import { Server as HttpServer } from "node:http";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import MySqlManager from "@config/db/MySqlManager.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
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
   * Http server
   * @private
   */
  private server?: HttpServer;
    /**
   * Db Manager
   * @private
   */
  private dbManager: MySqlManager;
    /**
   * Logger
   * @private
   */
  private logger: Logger;

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
    this.logger = createLogger("JobServiceImpl");
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
      this.logger.error("express_error", { error: err.message, stack: err.stack });
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
              this.logger.error("event_processing_error_ack", { error: errorStr, body: payload, action: "ack_to_prevent_retry" });
              await deleteMessage(config.settings.JOB_EVENTS_QUEUE_URL, receiptHandle);
            } else {
              this.logger.error("event_processing_error", { error: errorStr, body: payload });
            }
          }
        }
      } catch (exc) {
        this.logger.error("event_consumer_loop_error", { error: String(exc) });
        await new Promise((r) => setTimeout(r, 5000));
      }
    }
  }

    /**
   * Initializes database
   */
  public async initializeDatabase(): Promise<void> {
    try {
      this.logger.info("db_migration_starting");
      await this.dbManager.initialize();
      await createTables();
      this.logger.info("db_migration_complete");
    } catch (err) {
      this.logger.error("db_migration_failed", { error: String(err) });
      throw err;
    }
  }

  /**
   * Connects the service by starting the HTTP server.
   */
  public async connect(): Promise<void> {
    const PORT = process.env.PORT || 8080;

    return new Promise((resolve) => {
      this.server = this.app.listen(PORT, () => {
        this.logger.info(`Job Service listening on port ${PORT}`);
        resolve();
      });
    });
  }

  /**
   * Stops the HTTP server gracefully.
   */
  public async gracefulStop(): Promise<void> {
    if (this.server) {
      await new Promise<void>((resolve, reject) => {
        this.server!.close((err) => {
          if (err) reject(err);
          else resolve();
        });
      });
    }
  }

    /**
   * Starts the operation
   */
  public async start(): Promise<void> {
    await this.initialize();
    await this.initializeDatabase();
    this.logger.info("Database initialized successfully");
    this.eventConsumerLoop();
  }

    /**
   * Stops the operation
   */
  public async stop(): Promise<void> {
    await this.shutdown();
  }
}

export default JobServiceImpl;
