import express, { Request, Response, NextFunction } from "express";
import { settings } from "../../shared/config.js";
import { ClassifyRequest, TemplateKind } from "../../shared/models/template.js";
import { classifyAi } from "./AiClassifierServiceHandler.js";
import { mockClassify } from "./mock.js";
import { ensureTableExists, listAll, warmCache } from "./templateRegistry.js";

const app = express();
app.use(express.json());

ensureTableExists();

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
    const result = await classifyAi(request);
    res.json(result);
  } catch (err) {
    next(err);
  }
});

app.get("/templates", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const kind = req.query.kind ? (req.query.kind as TemplateKind) : undefined;
    res.json(listAll(kind));
  } catch (err) {
    next(err);
  }
});

app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  console.error("error", err);
  res.status(500).json({ detail: err.message });
});

const PORT = process.env.PORT || 8001;
warmCache().then(() => {
  app.listen(PORT, () => {
    console.log(`AI Classifier listening on port ${PORT}`);
  });
});
