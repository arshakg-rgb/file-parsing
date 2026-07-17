import express, { Request, Response, NextFunction } from "express";
import Config from "../../config/system-config/Config.js";
import ServiceManager from "../../config/ServiceManager.js";
import { InstantiationError } from "../../errors/InstantiationError.js";
import MySqlManager from "../../config/db/MySqlManager.js";
import { receiveMessages, deleteMessage } from "../../shared/queueUtils.js";
import { JobEvent, EventType } from "../../shared/models/events.js";
import { handleEvent } from "./stateMachine.js";
import { router } from "./router.js";
import { createTables } from "../../shared/db.js";

class JobService extends ServiceManager {
  protected static instance: JobService;
  private app: express.Express;
  private dbManager: MySqlManager;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate JobService directly. Use getInstance()");
    }
    super(enforce);
    
    this.app = express();
    this.dbManager = MySqlManager.getInstance();
    this.setupApp();
  }

  public static getInstance(): JobService {
    if (!ServiceManager.instance) {
      ServiceManager.instance = new JobService(Enforce);
    }
    return ServiceManager.instance as JobService;
  }

  private setupApp(): void {
    this.app.use(express.json());
    this.app.use("/v1", router);

    this.app.get("/health", (_req: Request, res: Response) => {
      res.json({ status: "healthy", timestamp: new Date().toISOString() });
    });

    this.app.get("/health/db", async (_req: Request, res: Response) => {
      try {
        await this.dbManager.pool.query("SELECT 1");
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

  public start(): void {
    const config = this.getConfig();
    const PORT = process.env.PORT || 8080;
    
    // Start listening immediately for health checks
    this.app.listen(PORT, () => {
      console.log(`Job Service listening on port ${PORT}`);
    });
    
    // Initialize database and start consumer loop in background
    this.initializeDatabase().then(() => {
      console.log("Database initialized successfully");
      this.eventConsumerLoop();
    }).catch((err) => {
      console.error("Failed to initialize database:", err);
      console.error("Server will continue running without database connectivity");
    });
  }
}

function Enforce(): void {}

// Auto-start the service when this module is loaded
const jobService = JobService.getInstance();
jobService.start();

export default JobService;
