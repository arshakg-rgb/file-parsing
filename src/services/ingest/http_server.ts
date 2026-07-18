import express, { Request, Response } from "express";
import multer from "multer";
import { settings } from "../../shared/config.js";
import { sendRaw } from "../../shared/queueUtils.js";
import { putObject, parseGcsUrl } from "../../shared/gcsUtils.js";
import { createLogger } from "../../utils/logger/logger.js";

const logger = createLogger("ingest-http");
const app = express();
app.use(express.json());

const upload = multer({ storage: multer.memoryStorage() });

app.get("/health", (_req: Request, res: Response) => 
{
  res.json({ status: "healthy", service: "ingest" });
});

app.post("/upload", upload.single("file"), async (req: any, res: Response) => 
{
  try 
{
    if (!req.file) 
{
      return res.status(400).json({ error: "No file uploaded" });
    }

    const { field_spec, user_id } = req.body;
    if (!field_spec) 
{
      return res.status(400).json({ error: "field_spec is required" });
    }

    const filename = `${Date.now()}-${req.file.originalname}`;
    const [bucket, key] = parseGcsUrl(`gs:
    const gcsKey = `${key}/${filename}`;

    await putObject(bucket, gcsKey, req.file.buffer);
    logger.info("file_uploaded", { filename, gcsKey, user_id });

    const jobId = `job-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

    await sendRaw(settings.INGEST_QUEUE_URL, {
      job_id: jobId,
      source_type: "s3",
      source_ref: `gs:
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
  }
 catch (error) 
{
    logger.error("upload_error", { error: String(error) });
    res.status(500).json({ error: "Failed to process file" });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => 
{
  logger.info(`HTTP server listening on port ${PORT}`);
});
