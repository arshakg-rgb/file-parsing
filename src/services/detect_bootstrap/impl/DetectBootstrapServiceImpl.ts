import crypto from "crypto";
import jschardet from "jschardet";
import Config from "../../../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../../../config/ServiceManager.js";
import { InstantiationError } from "../../../errors/InstantiationError.js";
import FirestoreCacheUtils from "../../../utils/cache/FirestoreCacheUtils.js";
import { EventType, JobEvent, makeJobEvent } from "../../../shared/models/events.js";
import { JobStatus, ClassifyMessage, ParseMessage } from "../../../shared/models/job.js";
import { receiveMessages, deleteMessage, sendRaw, publishEvent } from "../../../shared/queueUtils.js";
import { decode, normalizeEncoding, bufferEncodingFor, isLikelyUtf8 } from "../../../utils/normalizers/encoding.js";
import { templateRegistry } from "../../../shared/templateRegistry.js";
import { createLogger } from "../../../utils/logger/logger.js";
import { metrics } from "../../../utils/response/metrics.js";
import { startHealthCheckServer } from "../../../utils/response/health.js";
import AiClassifierService from "../../ai_classifier/handler.js";
import { mockClassify } from "../../ai_classifier/mock.js";
import { DetectBootstrapService } from "../DetectBootstrapService.js";
import { IDetectBootstrap, ClassifyRequest, ClassifyResponse } from "../io/IDetectBootstrap.js";

class DetectBootstrapServiceImpl extends ServiceManager implements DetectBootstrapService {
  protected static instance: DetectBootstrapServiceImpl;
  private logger: any;
  private gcsUtils: FirestoreCacheUtils;
  private classify: (req: ClassifyRequest) => Promise<ClassifyResponse>;

  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate DetectBootstrapServiceImpl directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("detect_bootstrap");
    this.gcsUtils = FirestoreCacheUtils.getInstance();
    
    const config = this.getConfig();
    if (config.settings.BEDROCK_MODEL_ID === "mock") {
      this.classify = async (req: ClassifyRequest) => {
        const resp = await mockClassify(req);
        return resp.template ? { kind: resp.kind as any, template: resp.template as any } : { kind: "uncertain" };
      };
    } else {
      const aiService = new AiClassifierService();
      this.classify = async (req: ClassifyRequest) => {
        const aiReq = {
          ...req,
          context_lines: req.context_lines || []
        };
        return await aiService.classifyAi(aiReq);
      };
    }
    
    if (process.env.HEALTH_CHECK_PORT) {
      startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
    }
  }

  public static getInstance(): DetectBootstrapServiceImpl {
    if (!DetectBootstrapServiceImpl.instance) {
      DetectBootstrapServiceImpl.instance = new DetectBootstrapServiceImpl(Enforce);
    }
    return DetectBootstrapServiceImpl.instance;
  }

  public getLogger(): any {
    return this.logger;
  }

  public getGcsUtils(): FirestoreCacheUtils {
    return this.gcsUtils;
  }

  public async detectBootstrap(req: ClassifyRequest): Promise<ClassifyResponse> {
    return this.classify(req);
  }

  public async classifyLine(req: ClassifyRequest): Promise<ClassifyResponse> {
    return this.classify(req);
  }

  public computeWindowSize(avgRowBytes: number, maxRowBytes: number): number {
    const config = this.getConfig();
    return Math.min(
      config.settings.PROBE_WINDOW_MAX_BYTES,
      Math.max(config.settings.PROBE_WINDOW_MIN_BYTES, config.settings.PROBE_TARGET_LINES * avgRowBytes, 4 * maxRowBytes)
    );
  }

  public computeProbeOffsets(fileSize: number, windowSize: number): number[] {
    const config = this.getConfig();
    const count = Math.max(config.settings.PROBE_COUNT_MIN, Math.min(config.settings.PROBE_COUNT_MAX, Math.floor(fileSize / config.settings.PROBE_SIZE_PER_COUNT)));
    if (fileSize <= windowSize) return [0];
    const offsets = Array.from({ length: count }, (_, i) => Math.floor(i * ((fileSize - windowSize) / (count - 1))));
    offsets[0] = 0;
    offsets[offsets.length - 1] = Math.max(0, fileSize - windowSize);
    return [...new Set(offsets)].sort((a, b) => a - b);
  }

  public detectEncoding(raw: Buffer): string {
    // Prefer UTF-8 when the bytes actually validate as UTF-8
    if (isLikelyUtf8(raw.subarray(0, 65536))) return "utf-8";
    const result = jschardet.detect(raw.slice(0, 65536));
    return normalizeEncoding(result.encoding);
  }

  public measureRowWidth(raw: Buffer, encoding: string): [number, number] {
    const text = decode(raw, encoding);
    const lines = text.split(/\r?\n/).filter((l) => l.trim());
    if (!lines.length) return [256, 512];
    const sizes = lines.map((l) => Buffer.byteLength(l, bufferEncodingFor(encoding)));
    const avg = sizes.reduce((a, b) => a + b, 0) / sizes.length;
    return [avg, Math.max(...sizes)];
  }

  public fingerprintProbe(raw: Buffer, encoding: string): string {
    const text = decode(raw, encoding);
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

  public extractSampleLines(raw: Buffer, encoding: string, n: number): string[] {
    const text = decode(raw, encoding);
    return text.split(/\r?\n/).filter((l) => l.trim()).slice(0, n);
  }

  public async bootstrapJob(msg: ClassifyMessage): Promise<void> {
    await templateRegistry.loadFromDatabase();

    const jobId = msg.job_id;
    this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.DETECTING });
    console.log("detect_start", { jobId, s3_url: msg.s3_url, size: msg.size });

    const [bucket, key] = this.gcsUtils.parseGcsUrl(msg.s3_url);
    const fileSize = msg.size || (await this.gcsUtils.objectSize(bucket, key));

    const headEnd = Math.min(this.getConfig().settings.PROBE_WINDOW_MIN_BYTES - 1, fileSize - 1);
    const headRaw = await this.gcsUtils.readRange(bucket, key, 0, headEnd);
    const encoding = this.detectEncoding(headRaw);
    const [avgRow, maxRow] = this.measureRowWidth(headRaw, encoding);
    const windowSize = this.computeWindowSize(avgRow, maxRow);

    const offsets = this.computeProbeOffsets(fileSize, windowSize);
    this.logger.info("probing", { job_id: jobId, probe_count: offsets.length, file_size: fileSize });
    metrics.increment("detect.probe_start", 1, { probe_count: String(offsets.length) });

    const seen = new Set<string>();
    const seedTemplateIds: string[] = [];

    for (const offset of offsets) {
      const end = Math.min(offset + windowSize - 1, fileSize - 1);
      const probeRaw = await this.gcsUtils.readRange(bucket, key, offset, end);
      const fp = this.fingerprintProbe(probeRaw, encoding);
      if (seen.has(fp)) continue;
      seen.add(fp);

      const existing = templateRegistry.getByFingerprint(fp);
      if (existing) {
        seedTemplateIds.push(existing.template_id);
        continue;
      }

      const sampleLines = this.extractSampleLines(probeRaw, encoding, 10);
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
        const config = this.getConfig();
        const aiTimeout = new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("ai_classify_timeout")), config.settings.AI_CLASSIFY_TIMEOUT_MS)
        );
        resp = await Promise.race([this.classify(req), aiTimeout]);
      } catch (aiErr) {
        this.logger.warn("seed_classify_skipped", { job_id: jobId, fingerprint: fp, error: String(aiErr) });
        metrics.increment("detect.ai_timeout", 1);
        continue;
      }
      if (resp.template) {
        seedTemplateIds.push(resp.template.template_id);
        this.logger.info("seed_template_created", { job_id: jobId, kind: resp.kind, template_id: resp.template.template_id, fingerprint: fp });
        metrics.increment("detect.template_created", 1, { kind: resp.kind });
      }
    }

    this.logger.info("detect_complete", { job_id: jobId, seeds: seedTemplateIds.length, probes: offsets.length });
    metrics.increment("detect.complete", 1, { seeds: String(seedTemplateIds.length) });

    const parseMsg: ParseMessage = {
      job_id: jobId,
      s3_url: msg.s3_url,
      size: fileSize,
      field_spec: msg.field_spec,
      seed_template_ids: seedTemplateIds,
    };
    const config = this.getConfig();
    console.log("detect_sending_to_parse", { job_id: jobId, queue_url: config.settings.PARSE_QUEUE_URL });
    await sendRaw(config.settings.PARSE_QUEUE_URL, parseMsg);
    console.log("detect_parse_message_sent", { job_id: jobId });
  }

  private emit(jobId: string, eventType: EventType, data: Record<string, any>) {
    publishEvent(makeJobEvent(eventType, jobId, "detect_bootstrap", data));
  }
}

export default DetectBootstrapServiceImpl;
