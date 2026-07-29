import pino from "pino";
import express, { Request, Response, NextFunction } from "express";
import { settings } from "@shared/Settings.js";
import FirestoreManager from "@config/firestore/FirestoreManager.js";
import { ClassifyRequest, TemplateKind } from "@shared/models/template.js";
import { mockClassify } from "./mock.js";
import { TemplateRegistryFacade } from "./templateRegistry.js";
import { createLogger } from "@utils/logger/Log.js";
import {aiClassifierService} from "@service/ai_classifier/AiClassifierServiceHandler.js";

const logger = createLogger(module);

/**
 * The app
 */
const app = express();
app.use(express.json());

app.get("/health", (_req: Request, res: Response) => {
  res.json({ status: "healthy", timestamp: new Date().toISOString() });
});

app.post("/classify", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const request = req.body as ClassifyRequest;
    if (settings.BEDROCK_MODEL_ID === "mock") {
      res.json(mockClassify(request));
      return;
    }
    const result = await aiClassifierService.classifyAi(request);
    res.json(result);
  } catch (err) {
    next(err);
  }
});

app.get("/templates", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const kind = req.query.kind ? (req.query.kind as TemplateKind) : undefined;
    res.json(TemplateRegistryFacade.getInstance().listAll(kind));
  } catch (err) {
    next(err);
  }
});

app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  logger.error("unhandled_request_error", { error: err.message, stack: err.stack });
  res.status(500).json({ detail: err.message });
});

/**
 * The p o r t
 */
const PORT = Number(process.env.PORT) || 8001;

async function bootstrap(): Promise<void> {
  await FirestoreManager.getInstance().connect();
  await TemplateRegistryFacade.getInstance().warmCache();
  app.listen(PORT, () => {
    logger.info("ai_classifier_listening", { port: PORT });
  });
}

bootstrap().catch((err: unknown) => {
  logger.error("ai_classifier_bootstrap_failed", { error: err instanceof Error ? err.message : String(err) });
  process.exit(1);
});
