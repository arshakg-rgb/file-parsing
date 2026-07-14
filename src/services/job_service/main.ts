import express, { Request, Response, NextFunction } from "express";
import { settings } from "../../shared/config.js";
import { receiveMessages, deleteMessage } from "../../shared/queueUtils.js";
import { JobEvent, EventType } from "../../shared/models/events.js";
import { handleEvent } from "./stateMachine.js";
import { router } from "./router.js";
import { startHealthCheckServer } from "../../shared/health.js";

const app = express();
app.use(express.json());
app.use("/v1", router);

app.get("/health", (_req: Request, res: Response) => {
  res.json({ status: "healthy", timestamp: new Date().toISOString() });
});

app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  console.error("error", err);
  res.status(500).json({ detail: err.message });
});

if (process.env.HEALTH_CHECK_PORT) {
  startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
}

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
app.listen(PORT, () => {
  console.log(`Job Service listening on port ${PORT}`);
  eventConsumerLoop();
});
