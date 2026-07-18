import express, { Request, Response } from "express";
import multer from "multer";
import { settings } from "../../shared/config.js";
import { sendRaw } from "../../shared/queueUtils.js";
import { putObject, parseGcsUrl } from "../../shared/gcsUtils.js";
import { createLogger } from "../../utils/logger/logger.js";

const logger = createLogger("ingest-http");
const app = express();
app.use(express.json());

// Configure multer for file uploads
const upload = multer({ storage: multer.memoryStorage() });

// Health check endpoint
app.get("/health", (_req: Request, res: Response) => {
  res.json({ status: "healthy", service: "ingest" });
});

// File upload endpoint
app.post("/upload", upload.single("file"), async (req: Request, res: Response) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: "No file uploaded" });
    }

    const { field_spec, user_id } = req.body;
    if (!field_spec) {
      return res.status(400).json({ error: "field_spec is required" });
    }

    // Generate unique filename
    const filename = `${Date.now()}-${req.file.originalname}`;
    const [bucket, key] = parseGcsUrl(`gs://${settings.DATA_BUCKET}`);
    const gcsKey = `${key}/${filename}`;

    // Upload file to GCS
    await putObject(bucket, gcsKey, req.file.buffer);
    logger.info("file_uploaded", { filename, gcsKey, user_id });

    // Generate job ID
    const jobId = `job-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

    // Publish message to ingest queue
    await sendRaw(settings.INGEST_QUEUE_URL, {
      job_id: jobId,
      source_type: "s3",
      source_ref: `gs://${settings.DATA_BUCKET}/${filename}`,
      field_spec,
      user_id: user_id || "unknown",
    });

    logger.info("message_published", { jobId, gcsKey });

    res.json({
      success: true,
      job_id: jobId,
      message: "File uploaded and processing started",
      gcs_path: `gs://${settings.DATA_BUCKET}/${filename}`,
    });
  } catch (error) {
    logger.error("upload_error", { error: String(error) });
    res.status(500).json({ error: "Failed to process file" });
  }
});

// Start HTTP server
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  logger.info(`HTTP server listening on port ${PORT}`);
});
