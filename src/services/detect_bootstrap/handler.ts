import crypto from "crypto";
import jschardet from "jschardet";
import { settings } from "../../shared/config.js";
import { EventType, JobEvent, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus, ClassifyMessage, ParseMessage, SourceType } from "../../shared/models/job.js";
import { receiveMessages, deleteMessage, sendRaw, publishEvent } from "../../shared/queueUtils.js";
import { parseGcsUrl, objectSize, readRange } from "../../shared/gcsUtils.js";
import { templateRegistry, RecordTemplate, RubbishTemplate } from "../../shared/templateRegistry.js";
import { createLogger } from "../../shared/logger.js";
import { metrics } from "../../shared/metrics.js";
import { startHealthCheckServer } from "../../shared/health.js";
import { waitForDb } from "../../shared/db.js";

const logger = createLogger("detect_bootstrap");

if (process.env.HEALTH_CHECK_PORT) {
  startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
}

interface ClassifyRequest {
  unknown_line: string;
  field_spec: string[];
  context_lines?: string[];
  job_id?: string;
}

interface ClassifyResponse {
  kind: "record-template" | "rubbish-signature" | "uncertain";
  template?: RecordTemplate | RubbishTemplate;
}

let classify: (req: ClassifyRequest) => Promise<ClassifyResponse>;
if (settings.BEDROCK_MODEL_ID === "mock") {
  const { mockClassify } = await import("../ai_classifier/mock.js");
  classify = async (req: ClassifyRequest) => {
    const resp = await mockClassify(req);
    return resp.template ? { kind: resp.kind as any, template: resp.template as any } : { kind: "uncertain" };
  };
} else {
  const { classifyAi } = await import("../ai_classifier/handler.js");
  classify = async (req: ClassifyRequest) => {
    // Convert to the expected format for the AI classifier
    const aiReq = {
      ...req,
      context_lines: req.context_lines || []
    };
    return await classifyAi(aiReq);
  };
}

function emit(jobId: string, eventType: EventType, data: Record<string, any>) {
  publishEvent(makeJobEvent(eventType, jobId, "detect_bootstrap", data));
}

export function computeWindowSize(avgRowBytes: number, maxRowBytes: number): number {
  return Math.min(
    settings.PROBE_WINDOW_MAX_BYTES,
    Math.max(settings.PROBE_WINDOW_MIN_BYTES, settings.PROBE_TARGET_LINES * avgRowBytes, 4 * maxRowBytes)
  );
}

export function computeProbeOffsets(fileSize: number, windowSize: number): number[] {
  const count = Math.max(settings.PROBE_COUNT_MIN, Math.min(settings.PROBE_COUNT_MAX, Math.floor(fileSize / settings.PROBE_SIZE_PER_COUNT)));
  if (fileSize <= windowSize) return [0];
  const offsets = Array.from({ length: count }, (_, i) => Math.floor(i * ((fileSize - windowSize) / (count - 1))));
  offsets[0] = 0;
  offsets[offsets.length - 1] = Math.max(0, fileSize - windowSize);
  return [...new Set(offsets)].sort((a, b) => a - b);
}

export function detectEncoding(raw: Buffer): string {
  const result = jschardet.detect(raw.slice(0, 65536));
  const detected = result.encoding || "utf-8";
  
  // Map unsupported encodings to supported alternatives
  const encodingMap: Record<string, string> = {
    'iso-8859-2': 'iso-8859-1',
    'windows-1252': 'cp1252',
    'latin-1': 'iso-8859-1',
    'iso-8859-3': 'iso-8859-1',
    'iso-8859-4': 'iso-8859-1',
    'iso-8859-5': 'iso-8859-1',
    'iso-8859-6': 'iso-8859-1',
    'iso-8859-7': 'iso-8859-1',
    'iso-8859-8': 'iso-8859-1',
    'iso-8859-9': 'iso-8859-1',
    'iso-8859-10': 'iso-8859-1',
    'iso-8859-13': 'iso-8859-1',
    'iso-8859-14': 'iso-8859-1',
    'iso-8859-15': 'iso-8859-1',
    'iso-8859-16': 'iso-8859-1',
  };
  
  return encodingMap[detected.toLowerCase()] || detected;
}

export function measureRowWidth(raw: Buffer, encoding: string): [number, number] {
  const text = raw.toString(encoding as BufferEncoding, 0, raw.length) || raw.toString("utf-8", 0, raw.length);
  const lines = text.split(/\r?\n/).filter((l) => l.trim());
  if (!lines.length) return [256, 512];
  const sizes = lines.map((l) => Buffer.byteLength(l, encoding as BufferEncoding));
  const avg = sizes.reduce((a, b) => a + b, 0) / sizes.length;
  return [avg, Math.max(...sizes)];
}

export function fingerprintProbe(raw: Buffer, encoding: string): string {
  const text = raw.toString(encoding as BufferEncoding, 0, raw.length) || raw.toString("utf-8", 0, raw.length);
  const lines = text.split(/\r?\n/).filter((l) => l.trim());
  if (!lines.length) return crypto.createHash("sha256").update("empty").digest("hex").slice(0, 24);
  const first = lines[0];
  for (const delim of [",", ";", "\t", "|"]) {
    const parts = first.split(delim);
    if (parts.length > 1) {
      return crypto.createHash("sha256").update(`csv|${delim}|${parts.length}|${encoding}`).digest("hex").slice(0, 24);
    }
  }
  try {
    const parsed = JSON.parse(first);
    if (typeof parsed === "object" && parsed !== null) {
      const keys = Object.keys(parsed).sort().join(",");
      return crypto.createHash("sha256").update(`json|${keys}`).digest("hex").slice(0, 24);
    }
  } catch {}
  return crypto.createHash("sha256").update(`text|${first.length}|${encoding}`).digest("hex").slice(0, 24);
}

export async function bootstrapJob(msg: ClassifyMessage): Promise<void> {
  await templateRegistry.loadFromDatabase();

  const jobId = msg.job_id;
  emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.DETECTING });
  console.log("detect_start", { jobId, s3_url: msg.s3_url, size: msg.size });

  const [bucket, key] = parseGcsUrl(msg.s3_url);
  const fileSize = msg.size || (await objectSize(bucket, key));

  const headEnd = Math.min(settings.PROBE_WINDOW_MIN_BYTES - 1, fileSize - 1);
  const headRaw = await readRange(bucket, key, 0, headEnd);
  const encoding = detectEncoding(headRaw);
  const [avgRow, maxRow] = measureRowWidth(headRaw, encoding);
  const windowSize = computeWindowSize(avgRow, maxRow);

  const offsets = computeProbeOffsets(fileSize, windowSize);
  logger.info("probing", { job_id: jobId, probe_count: offsets.length, file_size: fileSize });
  metrics.increment("detect.probe_start", 1, { probe_count: String(offsets.length) });

  const seen = new Set<string>();
  const seedTemplateIds: string[] = [];

  for (const offset of offsets) {
    const end = Math.min(offset + windowSize - 1, fileSize - 1);
    const probeRaw = await readRange(bucket, key, offset, end);
    const fp = fingerprintProbe(probeRaw, encoding);
    if (seen.has(fp)) continue;
    seen.add(fp);

    const existing = templateRegistry.getByFingerprint(fp);
    if (existing) {
      seedTemplateIds.push(existing.template_id);
      continue;
    }

    const sampleLines = extractSampleLines(probeRaw, encoding, 10);
    if (!sampleLines.length) continue;

    // Skip header lines for CSV files - use actual data lines for classification
    let dataLines = sampleLines;
    const firstLine = sampleLines[0];
    const hasHeader = /^[a-zA-Z_][a-zA-Z0-9_]*(,[a-zA-Z_][a-zA-Z0-9_]*)+$/.test(firstLine) ||
                     /^[a-zA-Z_][a-zA-Z0-9_]*(;[a-zA-Z_][a-zA-Z0-9_]*)+$/.test(firstLine) ||
                     /^[a-zA-Z_][a-zA-Z0-9_]*(\t[a-zA-Z_][a-zA-Z0-9_]*)+$/.test(firstLine);
    
    console.log("detect_header_check", { job_id: jobId, firstLine, hasHeader, sampleLinesCount: sampleLines.length });
    
    if (hasHeader && sampleLines.length > 1) {
      dataLines = sampleLines.slice(1);
      console.log("detect_header_skipped", { job_id: jobId, dataLinesCount: dataLines.length });
    }

    if (!dataLines.length) continue;

    // Parse field_spec if it's a JSON string
    let fieldSpecArray: string[] = [];
    if (typeof msg.field_spec === 'string') {
      try {
        fieldSpecArray = JSON.parse(msg.field_spec);
      } catch {
        fieldSpecArray = [];
      }
    } else {
      fieldSpecArray = msg.field_spec;
    }

    const req: ClassifyRequest = {
      unknown_line: dataLines[0],
      field_spec: fieldSpecArray,
      context_lines: dataLines.slice(1) || [],
      job_id: jobId,
    };
    console.log("detect_classify_request", { job_id: jobId, unknown_line: dataLines[0], contextLinesCount: dataLines.slice(1).length });
    let resp: ClassifyResponse;
    try {
      const aiTimeout = new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("ai_classify_timeout")), settings.AI_CLASSIFY_TIMEOUT_MS)
      );
      resp = await Promise.race([classify(req), aiTimeout]);
    } catch (aiErr) {
      logger.warn("seed_classify_skipped", { job_id: jobId, fingerprint: fp, error: String(aiErr) });
      metrics.increment("detect.ai_timeout", 1);
      continue;
    }
    if (resp.template) {
      seedTemplateIds.push(resp.template.template_id);
      logger.info("seed_template_created", { job_id: jobId, kind: resp.kind, template_id: resp.template.template_id, fingerprint: fp });
      metrics.increment("detect.template_created", 1, { kind: resp.kind });
    }
  }

  logger.info("detect_complete", { job_id: jobId, seeds: seedTemplateIds.length, probes: offsets.length });
  metrics.increment("detect.complete", 1, { seeds: String(seedTemplateIds.length) });

  const parseMsg: ParseMessage = {
    job_id: jobId,
    s3_url: msg.s3_url,
    size: fileSize,
    field_spec: msg.field_spec,
    seed_template_ids: seedTemplateIds,
  };
  console.log("detect_sending_to_parse", { job_id: jobId, queue_url: settings.PARSE_QUEUE_URL });
  await sendRaw(settings.PARSE_QUEUE_URL, parseMsg);
  console.log("detect_parse_message_sent", { job_id: jobId });
}

function extractSampleLines(raw: Buffer, encoding: string, n: number): string[] {
  const text = raw.toString(encoding as BufferEncoding, 0, raw.length) || raw.toString("utf-8", 0, raw.length);
  return text.split(/\r?\n/).filter((l) => l.trim()).slice(0, n);
}

export async function consumerLoop(): Promise<void> {
  await waitForDb();
  await templateRegistry.loadFromDatabase();
  logger.info("detect_bootstrap_consumer_started");
  while (true) {
    const messages = await receiveMessages<ClassifyMessage>(
      settings.CLASSIFY_QUEUE_URL,
      (body) => JSON.parse(body) as ClassifyMessage,
      1
    );
    for (const { payload, receiptHandle } of messages) {
      try {
        await bootstrapJob(payload);
        await deleteMessage(settings.CLASSIFY_QUEUE_URL, receiptHandle);
      } catch (exc) {
        const errMsg = String(exc);
        logger.error("detect_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
        metrics.increment("detect.error", 1);
        emit(payload.job_id, EventType.ERROR_OCCURRED, { error: errMsg });
        await deleteMessage(settings.CLASSIFY_QUEUE_URL, receiptHandle);
      }
    }
  }
}

consumerLoop();
