import pino from "pino";
import express, { Request, Response, NextFunction } from "express";
import { Server as HttpServer } from "node:http";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import PostgreSqlManager from "@config/db/PostgreSqlManager.js";
import { createLogger } from "@utils/logger/Log.js";
import {receiveMessages, deleteMessage, QueueMessage} from "@shared/QueueService.js";
import { JobEvent } from "@shared/models/events.js";
import { handleEvent } from "@service/job-service/stateMachine.js";
import { JobServiceRouter } from "@service/job-service/JobServiceRouter.js";
import { createTables } from "@shared/DatabaseManager.js";
import {
  DEFAULT_PORT,
  EVENT_BATCH_SIZE,
  EVENT_LOOP_ERROR_BACKOFF_MS, EVENT_POLL_WAIT_SECONDS,
  IJobService,
  JobResponse,
  NON_RETRYABLE_JOB_ERROR_MARKERS
} from "@service/job-service/io/IJobService.js";
import Config from "@config/system-config/Config";

/**
 * JobServiceManager is a singleton class responsible for managing the job service. It wires up
 * the HTTP API, database, and background event consumer, and exposes lifecycle hooks to
 * initialize and gracefully stop the service.
 */
class JobServiceManager extends ServiceManager implements IJobService
{
  /**
   *  Singleton instance.
   */

  protected static instance: JobServiceManager;

  /**
   * The Express application instance.
   */

  private readonly app: express.Express;

  /**
   *  Underlying HTTP server, set once {@link connect} has been called.
   */

  private server?: HttpServer;

  /**
   *  Database manager.
   */

  private readonly dbManager: PostgreSqlManager;

  /**
   * Logger scoped to this module.
   */

  private readonly logger: pino.Logger;

  /**
   * Constructs a new JobServiceManager instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws {InstantiationError} if instantiated directly instead of via {@link getInstance}
   */

  protected constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Cannot instantiate JobServiceManager directly. Use getInstance()");
    }
    super(enforce);

    this.app = express();
    this.dbManager = PostgreSqlManager.getInstance();
    this.logger = createLogger(module);

    this.setupApp();
  }

  /**
   * Gets the single instance of the JobServiceManager class, creating it on first access.
   * @returns The single instance of the class
   */

  public static getInstance(): JobServiceManager
  {
    if (!JobServiceManager.instance)
    {
      JobServiceManager.instance = new JobServiceManager(Enforce);
    }

    return JobServiceManager.instance;
  }

  /**
   * Registers middleware and routes on the Express application.
   */

  private setupApp(): void
  {
    this.app.use(express.json());
    this.app.use("/v1", JobServiceRouter.getInstance().getRouter());

    this.app.get("/health", (_req: Request, res: Response) => {
      res.json({ status: "healthy", timestamp: new Date().toISOString() });
    });

    this.app.get("/health/db", (_req: Request, res: Response) => {
      void this.handleDbHealthCheck(res);
    });

    this.app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
      this.logger.error({ error: err.message, stack: err.stack }, "express_error");
      res.status(500).json({ detail: err.message });
    });
  }

  /**
   * Checks database connectivity and writes the result to the response.
   * @param res - The Express response to write the health status to
   */

  private async handleDbHealthCheck(res: Response): Promise<void>
  {
    try
    {
      await this.dbManager.sequelize.authenticate();
      res.json({ status: "healthy", database: "connected", timestamp: new Date().toISOString() });
    }
    catch (err)
    {
      res.status(500).json({
        status: "unhealthy",
        database: "disconnected",
        timestamp: new Date().toISOString(),
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  /**
   * Processes a job.
   * @returns A promise that resolves to the job response
   */

  public async processJob(): Promise<JobResponse>
  {
    return { success: true };
  }

  /**
   * Determines whether a job-processing error is permanent (e.g. the job no longer exists,
   * or is in a state that can't accept the requested transition) and therefore should be
   * acknowledged instead of retried.
   * @param errorMessage - The stringified error raised while handling the event
   */

  private isNonRetryableJobError(errorMessage: string): boolean
  {
    return (
        errorMessage.includes("Job") &&
        NON_RETRYABLE_JOB_ERROR_MARKERS.some((marker) => errorMessage.includes(marker))
    );
  }

  /**
   * Handles a single job event: processes it and deletes it from the queue on success.
   * Non-retryable failures are also deleted (acknowledged) to avoid poison-pill retries;
   * all other failures are logged and left on the queue for a future retry.
   * @param queueUrl - The URL of the queue the message was received from
   * @param payload - The parsed job event payload
   * @param receiptHandle - The receipt handle used to delete the message from the queue
   */

  private async processEventMessage(queueUrl: string, payload: JobEvent, receiptHandle: string): Promise<void>
  {
    try
    {
      await handleEvent(payload);
      await deleteMessage(queueUrl, receiptHandle);
    }
    catch (exc)
    {
      const errorStr: string = String(exc);

      if (this.isNonRetryableJobError(errorStr))
      {
        this.logger.error({ error: errorStr, body: payload, action: "ack_to_prevent_retry" }, "event_processing_error_ack");
        await deleteMessage(queueUrl, receiptHandle);
      }
      else
      {
        this.logger.error({ error: errorStr, body: payload }, "event_processing_error");
      }
    }
  }

  /**
   * Continuously polls the job events queue and processes each received event.
   * Runs until the process is terminated; unexpected failures are logged and followed
   * by a fixed backoff before the loop resumes polling.
   */

  public async eventConsumerLoop(): Promise<void>
  {
    const config: Config = this.getConfig();

    while (true)
    {
      try {
        const messages: QueueMessage<JobEvent>[] = await receiveMessages<JobEvent>(
            config.settings.JOB_EVENTS_QUEUE_URL,
            (body) => JSON.parse(body) as JobEvent,
            EVENT_BATCH_SIZE,
            EVENT_POLL_WAIT_SECONDS
        );

        for (const { payload, receiptHandle } of messages)
        {
          await this.processEventMessage(config.settings.JOB_EVENTS_QUEUE_URL, payload, receiptHandle);
        }
      }
      catch (exc)
      {
        this.logger.error({ error: String(exc) }, "event_consumer_loop_error");
        await this.delay(EVENT_LOOP_ERROR_BACKOFF_MS);
      }
    }
  }

  /**
   * Resolves after the given number of milliseconds.
   * @param ms - Delay in milliseconds
   */
  private delay(ms: number): Promise<void>
  {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Initializes the database connection and ensures required tables exist.
   * @throws Re-throws any error encountered during migration, after logging it
   */

  public async initializeDatabase(): Promise<void>
  {
    try
    {
      this.logger.info("Running database migration...");
      await this.dbManager.initialize();
      await createTables();
      this.logger.info("Database migration completed successfully");
    }
    catch (err)
    {
      this.logger.error({ error: err instanceof Error ? err.message : String(err) }, "Database migration failed");
      throw err;
    }
  }

  /**
   * Resolves the port the HTTP server should listen on.
   * @returns The configured port, falling back to {@link DEFAULT_PORT} if unset or invalid
   */

  private resolvePort(): number
  {
    const rawPort: string = process.env.PORT;
    const parsedPort: number = rawPort ? Number(rawPort) : NaN;
    return Number.isInteger(parsedPort) ? parsedPort : DEFAULT_PORT;
  }

  /**
   * Connects the service by starting the HTTP server.
   */

  public async connect(): Promise<void>
  {
    const port: number = this.resolvePort();

    return new Promise((resolve) =>
    {
      this.server = this.app.listen(port, () => {
        this.logger.info(`Job Service listening on port ${port}`);
        resolve();
      });
    });
  }

  /**
   * Stops the HTTP server gracefully, if it was started.
   */

  public async gracefulStop(): Promise<void>
  {
    if (!this.server)
    {
      return;
    }

    await new Promise<void>((resolve, reject) =>
    {
      this.server!.close((err) =>
      {
        if (err)
        {
          reject(err);
        }
        else
        {
          resolve();
        }
      });
    });
  }

  /**
   * Starts the service: runs base initialization, migrates the database, then launches
   * the background event consumer loop (intentionally not awaited so it can run
   * indefinitely alongside the HTTP server).
   */
  public async start(): Promise<void>
  {
    await this.initialize();
    await this.initializeDatabase();
    this.logger.info("Database initialized successfully");

    void this.eventConsumerLoop();
  }

  /**
   * Stops the service.
   */
  public async stop(): Promise<void>
  {
    await this.shutdown();
  }
}

export default JobServiceManager;
