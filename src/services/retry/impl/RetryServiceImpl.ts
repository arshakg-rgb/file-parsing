import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import MySqlManager from "@config/db/MySqlManager.js";
import type { DeadLetterAttributes } from "@config/db/models/DeadLetter.js";
import { DLQMessage, DLQStatus, FailureClass, JobStatus, LoadMessage } from "@shared/models/job.js";
import { receiveMessages, deleteMessage, sendMessage } from "@shared/QueueService.js";
import { ClassifyResult, LineClassifier } from "@service/stream_parser/LineClassifier.js";
import { templateRegistry } from "@shared/TemplateRegistryService.js";
import { createLogger, Logger } from "@utils/logger/logger.js";
import { metrics } from "@utils/response/metrics.js";
import { startHealthCheckServer } from "@utils/response/health.js";
import { RetryService } from "@service/retry/RetryService.js";
import { IRetry, RetryRequest, RetryResponse } from "@service/retry/io/IRetry.js";

class RetryServiceImpl extends ServiceManager implements RetryService {
  protected static instance: RetryServiceImpl;
  private logger: Logger;
  private dbManager: MySqlManager;
  private ALT_ENCODINGS = ["utf-8", "iso-8859-1", "cp1252", "utf-16"];

  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate RetryServiceImpl directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("retry");
    this.dbManager = MySqlManager.getInstance();
    
    if (process.env.HEALTH_CHECK_PORT) {
      startHealthCheckServer(parseInt(process.env.HEALTH_CHECK_PORT, 10));
    }
  }

  public static getInstance(): RetryServiceImpl {
    if (!RetryServiceImpl.instance) {
      RetryServiceImpl.instance = new RetryServiceImpl(Enforce);
    }
    return RetryServiceImpl.instance;
  }

  public getLogger(): Logger {
    return this.logger;
  }

  public getDbManager(): MySqlManager {
    return this.dbManager;
  }

  public async processRetry(req: RetryRequest): Promise<RetryResponse> {
    // Placeholder implementation
    return { success: true };
  }

  public async handleDlqEntry(msg: DLQMessage): Promise<void> {
    await templateRegistry.loadFromDatabase();

    this.logger.info("retry_attempt", {
      job_id: msg.job_id,
      byte_offset: msg.byte_offset,
      failure_class: msg.failure_class,
      attempts: msg.attempts + 1,
    });
    metrics.increment("retry.attempt", 1, { failure_class: msg.failure_class });

    if (msg.dlq_id) {
      const row = await this.getDeadLetter(msg.dlq_id);
      if (!row || row.status !== "pending") {
        this.logger.debug("retry_skip_non_pending", { dlq_id: msg.dlq_id, status: row?.status });
        return;
      }
    }

    const config = this.getConfig();
    if (msg.attempts >= config.settings.RETRY_MAX_ATTEMPTS) {
      await this.markForReview(msg);
      return;
    }

    const rawBytes = Buffer.from(msg.raw_bytes, "base64");
    let recovered: ClassifyResult | null = null;

    if (msg.failure_class === FailureClass.ENCODING_ERROR) {
      recovered = await this.retryEncoding(rawBytes, msg);
    } else if ([FailureClass.TRANSFORM_ERROR, FailureClass.EXTRACTION_ERROR].includes(msg.failure_class)) {
      recovered = await this.retryAfterTemplateUpdate(rawBytes, msg);
    } else if (msg.failure_class === FailureClass.TYPE_MISMATCH) {
      recovered = await this.retryBroadCoercion(rawBytes, msg);
    } else if (msg.failure_class === FailureClass.UNCERTAIN) {
      await this.markForReview(msg);
      return;
    }

    if (recovered && recovered.verdict === "parsed" && recovered.row) {
      await this.emitRecovered(msg, recovered);
      this.logger.info("line_recovered", { job_id: msg.job_id, byte_offset: msg.byte_offset });
      metrics.increment("retry.recovered", 1, { failure_class: msg.failure_class });
    } else {
      const delay = msg.attempts >= 1 ? config.settings.RETRY_DELAYED_DELAY_SECONDS : 0;
      await this.reEnqueue(msg, delay);
    }
  }

  private async retryEncoding(rawBytes: Buffer, msg: DLQMessage): Promise<ClassifyResult | null> {
    for (const enc of this.ALT_ENCODINGS) {
      try {
        const line = new TextDecoder(enc, { fatal: true }).decode(rawBytes);
        return await this.classifyLine(line, msg);
      } catch {
        continue;
      }
    }
    return null;
  }

  private async retryAfterTemplateUpdate(rawBytes: Buffer, msg: DLQMessage): Promise<ClassifyResult | null> {
    const line = rawBytes.toString("utf-8", 0, rawBytes.length);
    return await this.classifyLine(line, msg);
  }

  private async retryBroadCoercion(rawBytes: Buffer, msg: DLQMessage): Promise<ClassifyResult | null> {
    const line = rawBytes.toString("utf-8", 0, rawBytes.length);
    const result = await this.classifyLine(line, msg);
    if (result && result.verdict === "parsed") return result;
    const fieldSpec = await this.getFieldSpec(msg.job_id);
    return { verdict: "parsed", row: Object.fromEntries(fieldSpec.map((f) => [f, null])), template_id: "coerced" };
  }

  private async classifyLine(line: string, msg: DLQMessage): Promise<ClassifyResult | null> {
    const fieldSpec = await this.getFieldSpec(msg.job_id);
    const recordTemplates = templateRegistry.getAllRecordTemplates();
    const rubbishTemplates = templateRegistry.getAllRubbishTemplates();
    const classifier = new LineClassifier(
      msg.job_id,
      fieldSpec,
      recordTemplates,
      rubbishTemplates
    );
    const result = classifier.classify(line, msg.byte_offset, msg.byte_length);
    if (result.verdict === "parsed") return result;

    const ai = await classifier.classifyWithAI(line, []);
    if (ai.verdict === "parsed") return ai;
    return null;
  }

  private async getFieldSpec(jobId: string): Promise<string[]> {
    return this.dbManager.repositories.jobs.getFieldSpec(jobId);
  }

  private async getDeadLetter(dlqId: string): Promise<DeadLetterAttributes | null> {
    return this.dbManager.repositories.deadLetters.findById(dlqId);
  }

  private async updateDeadLetterStatus(dlqId: string | undefined, status: string, attempts?: number): Promise<void> {
    if (!dlqId) return;
    await this.dbManager.repositories.deadLetters.updateStatus(dlqId, status, { attempts });
  }

  private async markForReview(msg: DLQMessage): Promise<void> {
    await this.updateDeadLetterStatus(msg.dlq_id, "review");
    this.logger.warn("line_marked_for_review", {
      job_id: msg.job_id,
      byte_offset: msg.byte_offset,
      failure_class: msg.failure_class,
      attempts: msg.attempts,
    });
    metrics.increment("retry.marked_for_review", 1, { failure_class: msg.failure_class });
  }

  private async emitRecovered(msg: DLQMessage, result: ClassifyResult): Promise<void> {
    await this.updateDeadLetterStatus(msg.dlq_id, "recovered");
    const loadMsg: LoadMessage = {
      job_id: msg.job_id,
      recovered_row: result.row,
      byte_offset: msg.byte_offset,
      byte_length: msg.byte_length,
      line_no: msg.line_no,
      template_id: result.template_id,
      template_version: result.template_version,
    };
    const config = this.getConfig();
    await sendMessage(config.settings.LOAD_QUEUE_URL, loadMsg, 0, msg.job_id);
  }

  private async reEnqueue(msg: DLQMessage, delaySeconds: number): Promise<void> {
    const nextAttempts = msg.attempts + 1;
    await this.updateDeadLetterStatus(msg.dlq_id, "pending", nextAttempts);
    const updated: DLQMessage = { ...msg, attempts: nextAttempts, status: "pending" };
    const config = this.getConfig();
    await sendMessage(config.settings.DLQ_QUEUE_URL, updated, delaySeconds, msg.job_id);
    this.logger.info("line_re_enqueued", {
      job_id: msg.job_id,
      attempts: updated.attempts,
      delay_s: delaySeconds,
    });
    metrics.increment("retry.re_enqueued", 1);
  }

  public async consumerLoop(): Promise<void> {
    await this.dbManager.initialize();
    this.logger.info("retry_consumer_started");
    const config = this.getConfig();
    while (true) {
      const messages = await receiveMessages<DLQMessage>(
        config.settings.DLQ_QUEUE_URL,
        (body) => JSON.parse(body) as DLQMessage,
        10
      );
      for (const { payload, receiptHandle } of messages) {
        try {
          await this.handleDlqEntry(payload);
          await deleteMessage(config.settings.DLQ_QUEUE_URL, receiptHandle);
        } catch (exc) {
          const errorStr = String(exc);
          if (errorStr.includes("Job") && (errorStr.includes("not found") || errorStr.includes("cannot transition"))) {
            this.logger.error("retry_failed_ack", { job_id: payload.job_id, error: errorStr, action: "ack_to_prevent_retry" });
            metrics.increment("retry.error_ack", 1);
            await deleteMessage(config.settings.DLQ_QUEUE_URL, receiptHandle);
          } else {
            this.logger.error("retry_failed", { job_id: payload.job_id }, exc instanceof Error ? exc : new Error(String(exc)));
            metrics.increment("retry.error", 1);
          }
        }
      }
    }
  }
}

export default RetryServiceImpl;
