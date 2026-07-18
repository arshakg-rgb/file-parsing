import crypto from "crypto";
import jschardet from "jschardet";
import { settings } from "../../shared/config.js";
import { EventType, makeJobEvent } from "../../shared/models/events.js";
import { JobStatus, ClassifyMessage, ParseMessage } from "../../shared/models/job.js";
import { receiveMessages, deleteMessage, sendRaw, publishEvent } from "../../shared/queueUtils.js";
import { parseGcsUrl, objectSize, readRange } from "../../shared/gcsUtils.js";
import { decode, normalizeEncoding, bufferEncodingFor, isLikelyUtf8 } from "../../utils/normalizers/encoding.js";
import { templateRegistry, RecordTemplate, RubbishTemplate } from "../../shared/templateRegistry.js";
import { createLogger } from "../../utils/logger/logger.js";
import { metrics } from "../../utils/response/metrics.js";
import { startHealthCheckServer } from "../../utils/response/health.js";
import { waitForDb } from "../../shared/db.js";

/**
 * Classification request interface
 */
interface ClassifyRequest {
  unknown_line: string;
  field_spec: string[];
  context_lines?: string[];
  job_id?: string;
}

/**
 * Classification response interface
 */
interface ClassifyResponse {
  kind: "record-template" | "rubbish-signature" | "uncertain";
  template?: RecordTemplate | RubbishTemplate;
}

/**
 * Detect Bootstrap Service - Senior Level ORM-Style Implementation
 * 
 * This service handles file property detection and job bootstrapping.
 * It uses adaptive probing to detect file structure, encoding, and row characteristics.
 * Follows ORM-style patterns with:
 * - Class-based architecture with instance state
 * - Dependency injection for services
 * - Lifecycle management (initialize, start, stop)
 * - Repository-style methods for data operations
 * - Clean separation of concerns
 * 
 * @class DetectBootstrapService
 */
export class DetectBootstrapService 
{
  private static instance: DetectBootstrapService;
  
  private running: boolean = false;
  private totalBootstraps: number = 0;
  private totalProbes: number = 0;
  private totalTemplatesCreated: number = 0;
  
  private stats = {
    csvDetected: 0,
    jsonDetected: 0,
    textDetected: 0,
    encodingDetections: 0,
    headerSkips: 0,
    aiTimeouts: 0,
    cacheHits: 0,
    cacheMisses: 0
  };
  
  private logger = createLogger("detect_bootstrap");
  
  private classify: ((req: any) => Promise<any>) | null = null;
  
  /**
   * Private constructor for singleton pattern
   */
  private constructor() 
{
    if (process.env.HEALTH_CHECK_PORT) 
{
      startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
    }
    
    this.initializeClassifier();
  }
  
  /**
   * Get singleton instance
   */
  static getInstance(): DetectBootstrapService 
{
    if (!DetectBootstrapService.instance) 
{
      DetectBootstrapService.instance = new DetectBootstrapService();
    }
    return DetectBootstrapService.instance;
  }
  
  /**
   * Initialize the classifier based on configuration
   */
  private async initializeClassifier(): Promise<void> 
{
    if (this.classify) return;
    
    if (settings.BEDROCK_MODEL_ID === "mock") 
{
      const { mockClassify } = await import("../ai_classifier/mock.js");
      this.classify = async (req: any) => 
{
        const resp = await mockClassify(req);
        return resp.template ? { kind: resp.kind as any, template: resp.template as any } : { kind: "uncertain" };
      };
    }
 else 
{
      const { classifyAi } = await import("../ai_classifier/handler.js");
      this.classify = async (req: any) => 
{
        const aiReq = {
          ...req,
          context_lines: req.context_lines || []
        };
        return await classifyAi(aiReq);
      };
    }
  }
  
  /**
   * Initialize the service
   */
  async initialize(): Promise<void> 
{
    await waitForDb();
    await templateRegistry.loadFromDatabase();
    await this.initializeClassifier();
    this.logger.info("detect_bootstrap_initialized");
  }
  
  /**
   * Start the consumer loop
   */
  async start(): Promise<void> 
{
    if (this.running) 
{
      this.logger.warn("detect_bootstrap_already_running");
      return;
    }
    
    this.running = true;
    await this.initialize();
    this.logger.info("detect_bootstrap_started");
    
    await this.consumerLoop();
  }
  
  /**
   * Stop the service gracefully
   */
  async stop(): Promise<void> 
{
    this.running = false;
    this.logger.info("detect_bootstrap_stopping");
  }
  
  /**
   * Get service statistics
   */
  getStats() 
{
    return {
      ...this.stats,
      totalBootstraps: this.totalBootstraps,
      totalProbes: this.totalProbes,
      totalTemplatesCreated: this.totalTemplatesCreated
    };
  }

  /**
   * Emit a job event to the event system
   * 
   * @param jobId - Job identifier
   * @param eventType - Type of event to emit
   * @param data - Event payload data
   */
  private emit(jobId: string, eventType: EventType, data: Record<string, any>): void 
{
    publishEvent(makeJobEvent(eventType, jobId, "detect_bootstrap", data));
  }

  /**
   * Compute the optimal window size for probing
   * 
   * @param avgRowBytes - Average row size in bytes
   * @param maxRowBytes - Maximum row size in bytes
   * @returns Optimal window size in bytes
   */
  private computeWindowSize(avgRowBytes: number, maxRowBytes: number): number 
{
    return Math.min(
      settings.PROBE_WINDOW_MAX_BYTES,
      Math.max(settings.PROBE_WINDOW_MIN_BYTES, settings.PROBE_TARGET_LINES * avgRowBytes, 4 * maxRowBytes)
    );
  }

  /**
   * Compute probe offsets for adaptive file structure detection
   * 
   * @param fileSize - Total file size in bytes
   * @param windowSize - Size of each probe window
   * @returns Array of byte offsets to probe
   */
  private computeProbeOffsets(fileSize: number, windowSize: number): number[] 
{
    const count = Math.max(settings.PROBE_COUNT_MIN, Math.min(settings.PROBE_COUNT_MAX, Math.floor(fileSize / settings.PROBE_SIZE_PER_COUNT)));
    if (fileSize <= windowSize) return [0];
    const offsets = Array.from({ length: count }, (_, i) => Math.floor(i * ((fileSize - windowSize) / (count - 1))));
    offsets[0] = 0;
    offsets[offsets.length - 1] = Math.max(0, fileSize - windowSize);
    return [...new Set(offsets)].sort((a, b) => a - b);
  }

  /**
   * Detect file encoding from raw bytes
   * 
   * Prefers UTF-8 when bytes validate as UTF-8 to avoid jschardet misdetection.
   * 
   * @param raw - Raw file bytes
   * @returns Detected encoding label
   */
  private detectEncoding(raw: Buffer): string 
{
    this.stats.encodingDetections++;
    
    if (isLikelyUtf8(raw.subarray(0, 65536))) return "utf-8";
    const result = jschardet.detect(raw.slice(0, 65536));
    return normalizeEncoding(result.encoding);
  }

  /**
   * Measure row width statistics from raw bytes
   * 
   * @param raw - Raw file bytes
   * @param encoding - File encoding
   * @returns Tuple of [average row bytes, maximum row bytes]
   */
  private measureRowWidth(raw: Buffer, encoding: string): [number, number] 
{
    const text = decode(raw, encoding);
    const lines = text.split(/\r?\n/).filter((l) => l.trim());
    if (!lines.length) return [256, 512];
    const sizes = lines.map((l) => Buffer.byteLength(l, bufferEncodingFor(encoding)));
    const avg = sizes.reduce((a, b) => a + b, 0) / sizes.length;
    return [avg, Math.max(...sizes)];
  }

  /**
   * Generate a fingerprint for a probe window
   * 
   * @param raw - Raw probe bytes
   * @param encoding - File encoding
   * @returns SHA256 hash truncated to 24 characters
   */
  private fingerprintProbe(raw: Buffer, encoding: string): string 
{
    const text = decode(raw, encoding);
    const lines = text.split(/\r?\n/).filter((l) => l.trim());
    if (!lines.length) return crypto.createHash("sha256").update("empty").digest("hex").slice(0, 24);
    const first = lines[0];
    for (const delim of [",", ";", "\t", "|"]) 
{
      const parts = first.split(delim);
      if (parts.length > 1) 
{
        this.stats.csvDetected++;
        return crypto.createHash("sha256").update(`csv|${delim}|${parts.length}|${encoding}`).digest("hex").slice(0, 24);
      }
    }
    try 
{
      const parsed = JSON.parse(first);
      if (typeof parsed === "object" && parsed !== null) 
{
        this.stats.jsonDetected++;
        const keys = Object.keys(parsed).sort().join(",");
        return crypto.createHash("sha256").update(`json|${keys}`).digest("hex").slice(0, 24);
      }
    }
 catch 
{}
    this.stats.textDetected++;
    return crypto.createHash("sha256").update(`text|${first.length}|${encoding}`).digest("hex").slice(0, 24);
  }

  /**
   * Extract sample lines from raw bytes
   * 
   * @param raw - Raw file bytes
   * @param encoding - File encoding
   * @param n - Maximum number of lines to extract
   @returns Array of non-empty lines
   */
  private extractSampleLines(raw: Buffer, encoding: string, n: number): string[] 
{
    const text = decode(raw, encoding);
    return text.split(/\r?\n/).filter((l) => l.trim()).slice(0, n);
  }

  /**
   * Main bootstrap job handler - detects file properties and seeds templates
   * 
   * This function performs adaptive probing to:
   * 1. Detect file encoding
   * 2. Measure row characteristics
   * 3. Generate fingerprints for probe windows
   * 4. Check for existing templates by fingerprint
   * 5. Classify unknown lines to create new seed templates
   * 6. Forward to parse service with seed template IDs
   * 
   * @param msg - Classify message containing job details
   * @throws Error if bootstrapping fails
   */
  async bootstrapJob(msg: ClassifyMessage): Promise<void> 
{
    const bootstrapStartTime = Date.now();
    this.totalBootstraps++;
    
    await templateRegistry.loadFromDatabase();

    const jobId = msg.job_id;
    this.emit(jobId, EventType.JOB_STATUS_CHANGED, { new_status: JobStatus.DETECTING });
    console.log("detect_start", { jobId, s3_url: msg.s3_url, size: msg.size });

    const [bucket, key] = parseGcsUrl(msg.s3_url);
    const fileSize = msg.size || (await objectSize(bucket, key));

    const headEnd = Math.min(settings.PROBE_WINDOW_MIN_BYTES - 1, fileSize - 1);
    const headRaw = await readRange(bucket, key, 0, headEnd);
    const encoding = this.detectEncoding(headRaw);
    const [avgRow, maxRow] = this.measureRowWidth(headRaw, encoding);
    const windowSize = this.computeWindowSize(avgRow, maxRow);

    const offsets = this.computeProbeOffsets(fileSize, windowSize);
    this.totalProbes += offsets.length;
    this.logger.info("probing", { job_id: jobId, probe_count: offsets.length, file_size: fileSize });
    metrics.increment("detect.probe_start", 1, { probe_count: String(offsets.length) });

    const seen = new Set<string>();
    const seedTemplateIds: string[] = [];

    for (const offset of offsets) 
{
      const end = Math.min(offset + windowSize - 1, fileSize - 1);
      const probeRaw = await readRange(bucket, key, offset, end);
      const fp = this.fingerprintProbe(probeRaw, encoding);
      if (seen.has(fp)) 
{
        this.stats.cacheHits++;
        continue;
      }
      seen.add(fp);
      this.stats.cacheMisses++;

      const existing = templateRegistry.getByFingerprint(fp);
      if (existing) 
{
        seedTemplateIds.push(existing.template_id);
        continue;
      }

      const sampleLines = this.extractSampleLines(probeRaw, encoding, 10);
      if (!sampleLines.length) continue;

      let dataLines = sampleLines;
      const firstLine = sampleLines[0];
      const hasHeader = /^[a-zA-Z_][a-zA-Z0-9_]*(,[a-zA-Z_][a-zA-Z0-9_]*)+$/.test(firstLine) ||
                       /^[a-zA-Z_][a-zA-Z0-9_]*(;[a-zA-Z_][a-zA-Z0-9_]*)+$/.test(firstLine) ||
                       /^[a-zA-Z_][a-zA-Z_0-9_]*(\t[a-zA-Z_][a-zA-Z0-9_]*)+$/.test(firstLine);
      
      console.log("detect_header_check", { job_id: jobId, firstLine, hasHeader, sampleLinesCount: sampleLines.length });
      
      if (hasHeader && sampleLines.length > 1) 
{
        this.stats.headerSkips++;
        dataLines = sampleLines.slice(1);
        console.log("detect_header_skipped", { job_id: jobId, dataLinesCount: dataLines.length });
      }

      if (!dataLines.length) continue;

      let fieldSpecArray: string[] = [];
      if (typeof msg.field_spec === "string") 
{
        try 
{
          fieldSpecArray = JSON.parse(msg.field_spec);
        }
 catch 
{
          fieldSpecArray = [];
        }
      }
 else 
{
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
      try 
{
        const aiTimeout = new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("ai_classify_timeout")), settings.AI_CLASSIFY_TIMEOUT_MS)
        );
        resp = await Promise.race([this.classify!(req), aiTimeout]);
      }
 catch (aiErr) 
{
        this.stats.aiTimeouts++;
        this.logger.warn("seed_classify_skipped", { job_id: jobId, fingerprint: fp, error: String(aiErr) });
        metrics.increment("detect.ai_timeout", 1);
        continue;
      }
      if (resp.template) 
{
        this.totalTemplatesCreated++;
        seedTemplateIds.push(resp.template.template_id);
        this.logger.info("seed_template_created", { job_id: jobId, kind: resp.kind, template_id: resp.template.template_id, fingerprint: fp });
        metrics.increment("detect.template_created", 1, { kind: resp.kind });
      }
    }

    const bootstrapDuration = Date.now() - bootstrapStartTime;
    this.logger.info("detect_complete", { 
      job_id: jobId, 
      seeds: seedTemplateIds.length, 
      probes: offsets.length,
      duration_ms: bootstrapDuration 
    });
    metrics.increment("detect.complete", 1, { seeds: String(seedTemplateIds.length) });
    metrics.set("detect.duration_ms", bootstrapDuration);

    const parseMsg: ParseMessage = {
      job_id: jobId,
      s3_url: msg.s3_url,
      size: fileSize,
      field_spec: msg.field_spec,
      column_map: msg.column_map,
      seed_template_ids: seedTemplateIds,
    };
    console.log("detect_sending_to_parse", { job_id: jobId, queue_url: settings.PARSE_QUEUE_URL });
    try 
{
      await sendRaw(settings.PARSE_QUEUE_URL, parseMsg);
      console.log("detect_parse_message_sent", { job_id: jobId });
    }
 catch (sendErr) 
{
      this.logger.error("detect_send_to_parse_failed", { job_id: jobId, queue_url: settings.PARSE_QUEUE_URL }, sendErr instanceof Error ? sendErr : new Error(String(sendErr)));
      throw sendErr;
    }
  }

  /**
   * Main consumer loop for processing classify messages
   * 
   * Continuously polls the classify queue for messages and processes them.
   * Handles graceful shutdown and message acknowledgment.
   * 
   * @throws Error if database connection fails
   */
  private async consumerLoop(): Promise<void> 
{
    await waitForDb();
    await templateRegistry.loadFromDatabase();
    this.logger.info("detect_bootstrap_consumer_started");
    
    while (this.running) 
{
      const messages = await receiveMessages<ClassifyMessage>(
        settings.CLASSIFY_QUEUE_URL,
        (body) => JSON.parse(body) as ClassifyMessage,
        1
      );
      
      for (const { payload, receiptHandle } of messages) 
{
        try 
{
          await this.bootstrapJob(payload);
          await deleteMessage(settings.CLASSIFY_QUEUE_URL, receiptHandle);
        }
 catch (exc) 
{
          const errMsg = String(exc);
          this.logger.error("detect_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
          metrics.increment("detect.error", 1);
          this.emit(payload.job_id, EventType.ERROR_OCCURRED, { error: errMsg });
          await deleteMessage(settings.CLASSIFY_QUEUE_URL, receiptHandle);
        }
      }
    }
    
    this.logger.info("detect_bootstrap_consumer_stopped");
  }
}

const detectBootstrapService = DetectBootstrapService.getInstance();

export async function bootstrapJob(msg: ClassifyMessage): Promise<void> 
{
  return detectBootstrapService.bootstrapJob(msg);
}

detectBootstrapService.start().catch(err => 
{
  console.error("detect_bootstrap_start_failed", { error: String(err) });
  process.exit(1);
});
