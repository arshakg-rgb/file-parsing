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

/**
 * RetryServiceImpl is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class RetryServiceImpl extends ServiceManager implements RetryService {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: RetryServiceImpl;
    /**
   * Logger instance
   * @private
   */
  private logger: Logger;
    /**
   * Db Manager
   * @private
   */
  private dbManager: MySqlManager;
    /**
   * A L T_ E N C O D I N G S
   * @private
   */
  private ALT_ENCODINGS = ["utf-8", "iso-8859-1", "cp1252", "utf-16"];

    /**
   * Constructs a new RetryServiceImpl instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  protected constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate RetryServiceImpl directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("retry");
    this.dbManager = MySqlManager.getInstance();
    
    // Cloud Run injects PORT; always listen on it (or 8080) so startup succeeds.
    // Also honor HEALTH_CHECK_PORT if set and different.
    const ports = new Set<number>();
    const cloudRunPort = process.env.PORT ? parseInt(process.env.PORT, 10) : 8080;
    ports.add(cloudRunPort);
    if (process.env.HEALTH_CHECK_PORT) {
      const p = parseInt(process.env.HEALTH_CHECK_PORT, 10);
      if (!isNaN(p) && p !== cloudRunPort) ports.add(p);
    }
    for (const port of ports) {
      try {
        startHealthCheckServer(port);
      } catch (err) {
        this.logger.error("health_server_start_failed", { port, error: err instanceof Error ? err.message : String(err) });
      }
    }
  }

    /**
   * Gets the single instance of the RetryServiceImpl class.
   * @returns The single instance of the class
   */
  public static getInstance(): RetryServiceImpl {
    if (!RetryServiceImpl.instance) {
      RetryServiceImpl.instance = new RetryServiceImpl(Enforce);
    }
    return RetryServiceImpl.instance;
  }

    /**
   * Gets logger
   * @returns The logger result
   */
  public getLogger(): Logger {
    return this.logger;
  }

    /**
   * Gets db manager
   * @returns The my sql manager result
   */
  public getDbManager(): MySqlManager {
    return this.dbManager;
  }

    /**
   * Processes retry
   * @param req - The HTTP request object
   * @returns A promise that resolves to the result
   */
  public async processRetry(req: RetryRequest): Promise<RetryResponse> {
    // Placeholder implementation
    return { success: true };
  }

    /**
   * Handles dlq entry
   * @param msg - The msg
   */
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

    /**
   * Retries encoding
   * @param rawBytes - The raw bytes
   * @param msg - The msg
   * @returns A promise that resolves to the result
   */
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

    /**
   * Retries after template update
   * @param rawBytes - The raw bytes
   * @param msg - The msg
   * @returns A promise that resolves to the result
   */
  private async retryAfterTemplateUpdate(rawBytes: Buffer, msg: DLQMessage): Promise<ClassifyResult | null> {
    const line = rawBytes.toString("utf-8", 0, rawBytes.length);
    return await this.classifyLine(line, msg);
  }

    /**
   * Retries broad coercion
   * @param rawBytes - The raw bytes
   * @param msg - The msg
   * @returns A promise that resolves to the result
   */
  private async retryBroadCoercion(rawBytes: Buffer, msg: DLQMessage): Promise<ClassifyResult | null> {
    const line = rawBytes.toString("utf-8", 0, rawBytes.length);
    const result = await this.classifyLine(line, msg);
    if (result && result.verdict === "parsed") return result;
    const fieldSpec = await this.getFieldSpec(msg.job_id);
    return { verdict: "parsed", row: Object.fromEntries(fieldSpec.map((f) => [f, null])), template_id: "coerced" };
  }

    /**
   * Classifies line
   * @param line - The line to process
   * @param msg - The msg
   * @returns A promise that resolves to the result
   */
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

    this.logger.info("ai_call_initiated", { job_id: msg.job_id, source: "retry", byte_offset: msg.byte_offset, line_length: line.length, context_lines: 0 });
    const ai = await classifier.classifyWithAI(line, []);
    this.logger.info("ai_call_completed", { job_id: msg.job_id, source: "retry", byte_offset: msg.byte_offset, verdict: ai.verdict, template_id: ai.template_id });
    if (ai.verdict === "parsed") return ai;
    this.logger.info("retry_ai_failed", { job_id: msg.job_id, byte_offset: msg.byte_offset, verdict: ai.verdict });
    return null;
  }

    /**
   * Gets field spec
   * @param jobId - The job identifier
   * @returns A promise that resolves to the list
   */
  private async getFieldSpec(jobId: string): Promise<string[]> {
    return this.dbManager.repositories.jobs.getFieldSpec(jobId);
  }

    /**
   * Gets dead letter
   * @param dlqId - The dlq id
   * @returns A promise that resolves to the result
   */
  private async getDeadLetter(dlqId: string): Promise<DeadLetterAttributes | null> {
    return this.dbManager.repositories.deadLetters.findById(dlqId);
  }

    /**
   * Updates dead letter status
   * @param dlqId - The dlq id
   * @param status - The status
   * @param attempts - The attempts
   */
  private async updateDeadLetterStatus(dlqId: string | undefined, status: string, attempts?: number): Promise<void> {
    if (!dlqId) return;
    await this.dbManager.repositories.deadLetters.updateStatus(dlqId, status, { attempts });
  }

    /**
   * Marks for review
   * @param msg - The msg
   */
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

    /**
   * Emits recovered
   * @param msg - The msg
   * @param result - The result
   */
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

    /**
   * Performs the re enqueue operation.
   * @param msg - The msg
   * @param delaySeconds - The delay seconds
   */
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

    /**
   * Performs the consumer loop operation.
   */
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
