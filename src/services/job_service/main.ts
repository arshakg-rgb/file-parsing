import express, { Request, Response, NextFunction } from "express";
import { settings } from "../../shared/config.js";
import { receiveMessages, deleteMessage } from "../../shared/queueUtils.js";
import { JobEvent, EventType } from "../../shared/models/events.js";
import { handleEvent } from "./stateMachine.js";
import { router } from "./router.js";
import { pool, createTables } from "../../shared/db.js";

const app = express();
app.use(express.json());
app.use("/v1", router);

app.get("/health", (_req: Request, res: Response) => {
  res.json({ status: "healthy", timestamp: new Date().toISOString() });
});

app.get("/health/db", async (_req: Request, res: Response) => {
  try {
    await pool.query("SELECT 1");
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

app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  console.error("error", err);
  res.status(500).json({ detail: err.message });
});

async function eventConsumerLoop(): Promise<void> {
  while (true) {
    try {
      const messages = await receiveMessages<JobEvent>(
        settings.JOB_EVENTS_QUEUE_URL,
        (body) => JSON.parse(body) as JobEvent,
        10,
        5
      );
      for (const { payload, receiptHandle } of messages) {
        try {
          await handleEvent(payload);
          await deleteMessage(settings.JOB_EVENTS_QUEUE_URL, receiptHandle);
        } catch (exc) {
          console.error("event_processing_error", { error: String(exc), body: payload });
        }
      }
    } catch (exc) {
      console.error("event_consumer_loop_error", { error: String(exc) });
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

const PORT = process.env.PORT || 8000;

async function initializeDatabase(): Promise<void> {
  try {
    console.log("Running database migration...");
    await createTables();
    console.log("Database migration completed successfully");
  } catch (err) {
    console.error("Database migration failed:", err);
    throw err;
  }
}

app.listen(PORT, async () => {
  console.log(`Job Service listening on port ${PORT}`);
  try {
    await initializeDatabase();
    eventConsumerLoop();
  } catch (err) {
    console.error("Failed to initialize database:", err);
    process.exit(1);
  }
});
