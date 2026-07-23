import type { SQSClient, SQSClientConfig, Message, SendMessageCommandInput, ReceiveMessageCommandInput } from "@aws-sdk/client-sqs";
import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { createLogger, Logger } from "@utils/logger/logger.js";

export interface QueueMessage<T> {
  payload: T;
  receiptHandle: string;
}

/**
 * QueueService is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class QueueService extends ServiceManager {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: QueueService;
    /**
   * Logger instance
   * @private
   */
  private logger: Logger;
    /**
   * Q U E U E_ R E T R I E S
   * @private
   */
  private readonly QUEUE_RETRIES = 3;
    /**
   * Q U E U E_ R E T R Y_ D E L A Y
   * @private
   */
  private readonly QUEUE_RETRY_DELAY = 200;
    /**
   * Q U E U E_ T I M E O U T_ S E N D
   * @private
   */
  private readonly QUEUE_TIMEOUT_SEND = 60000;
    /**
   * Q U E U E_ T I M E O U T_ R E C E I V E
   * @private
   */
  private readonly QUEUE_TIMEOUT_RECEIVE = 120000;
  
    /**
   * Pub Publisher
   * @private
   */
  private pubPublisher: unknown = null;
    /**
   * Pub Subscriber
   * @private
   */
  private pubSubscriber: unknown = null;
    /**
   * Sqs Client
   * @private
   */
  private sqsClient: unknown = null;

    /**
   * Constructs a new QueueService instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate QueueService directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("queue-utils");
  }

    /**
   * Gets the single instance of the QueueService class.
   * @returns The single instance of the class
   */
  public static getInstance(): QueueService {
    if (!QueueService.instance) {
      QueueService.instance = new QueueService(Enforce);
    }
    return QueueService.instance;
  }

    /**
   * Checks whether retryable
   * @param err - The error that occurred
   * @returns True if the condition is met, false otherwise
   */
  private isRetryable(err: unknown): boolean {
    if (!err) return false;
    const code = (err as { code?: string | number }).code;
    if (typeof code === "number") return code === 429 || code >= 500;
    if (typeof code === "string") {
      return ["ECONNRESET", "ETIMEDOUT", "ENOTFOUND", "ECONNREFUSED", "EPIPE"].includes(code);
    }
    return true;
  }

    /**
   * Performs the with timeout operation.
   * @param fn - The fn
   * @param ms - The ms
   * @returns A promise that resolves to the result
   */
  private async withTimeout<T>(fn: () => Promise<T>, ms: number): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        const err = new Error(`Queue operation timed out after ${ms}ms`);
        (err as unknown as { code?: string }).code = "ETIMEDOUT";
        reject(err);
      }, ms);

      fn().then(
        (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        (err) => {
          clearTimeout(timer);
          reject(err);
        }
      );
    });
  }

    /**
   * Performs the with retry operation.
   * @param fn - The fn
   * @param retries - The number of retries
   * @param delay - The delay
   * @returns A promise that resolves to the result
   */
  private async withRetry<T>(fn: () => Promise<T>, retries = this.QUEUE_RETRIES, delay = this.QUEUE_RETRY_DELAY): Promise<T> {
    let lastErr: unknown;
    for (let i = 0; i <= retries; i++) {
      try {
        return await fn();
      } catch (err) {
        lastErr = err;
        if (i === retries || !this.isRetryable(err)) throw err;
        const wait = delay * 2 ** i;
        this.logger.warn("queue_retry", { attempt: i + 1, wait, error: String(err) });
        await new Promise((r) => setTimeout(r, wait));
      }
    }
    throw lastErr;
  }

    /**
   * Checks whether pub sub
   * @returns True if the condition is met, false otherwise
   */
  private isPubSub(): boolean {
    const config = this.getConfig();
    return config.settings.QUEUE_BACKEND === "pubsub";
  }

    /**
   * Gets pub publisher
   */
  private async getPubPublisher() {
    if (this.pubPublisher) return this.pubPublisher;
    const config = this.getConfig();
    const { v1 } = await import("@google-cloud/pubsub");
    this.pubPublisher = new v1.PublisherClient({
      projectId: config.settings.GCP_PROJECT_ID,
      ...(config.settings.GOOGLE_APPLICATION_CREDENTIALS ? { keyFilename: config.settings.GOOGLE_APPLICATION_CREDENTIALS } : {}),
    });
    return this.pubPublisher;
  }

    /**
   * Gets pub subscriber
   */
  private async getPubSubscriber() {
    if (this.pubSubscriber) return this.pubSubscriber;
    const config = this.getConfig();
    const { v1 } = await import("@google-cloud/pubsub");
    this.pubSubscriber = new v1.SubscriberClient({
      projectId: config.settings.GCP_PROJECT_ID,
      ...(config.settings.GOOGLE_APPLICATION_CREDENTIALS ? { keyFilename: config.settings.GOOGLE_APPLICATION_CREDENTIALS } : {}),
    });
    return this.pubSubscriber;
  }

    /**
   * Performs the topic path operation.
   * @param q - The q
   * @returns The string result
   */
  private topicPath(q: string): string {
    const config = this.getConfig();
    if (q.startsWith("projects/")) return q;
    return `projects/${config.settings.GCP_PROJECT_ID}/topics/${q.split("/").pop()!.replace(/\.fifo$/, "")}`;
  }

    /**
   * Performs the subscription path operation.
   * @param q - The q
   * @returns The string result
   */
  private subscriptionPath(q: string): string {
    const config = this.getConfig();
    if (q.includes("/subscriptions/")) return q;
    return `projects/${config.settings.GCP_PROJECT_ID}/subscriptions/${q.split("/").pop()!.replace(/\.fifo$/, "")}-sub`;
  }

    /**
   * Gets sqs client
   */
  private async getSqsClient() {
    if (this.sqsClient) return this.sqsClient;
    const { SQSClient } = await import("@aws-sdk/client-sqs");
    const cfg: SQSClientConfig & { endpoint?: string; forcePathStyle?: boolean } = { region: "us-east-1" };
    const ep = process.env.AWS_ENDPOINT_URL;
    if (ep) { cfg.endpoint = ep; cfg.forcePathStyle = true; }
    const id = process.env.AWS_ACCESS_KEY_ID;
    const sec = process.env.AWS_SECRET_ACCESS_KEY;
    if (id && sec) cfg.credentials = { accessKeyId: id, secretAccessKey: sec };
    this.sqsClient = new SQSClient(cfg);
    return this.sqsClient;
  }

    /**
   * Performs the pub send operation.
   * @param queueUrl - The queue url
   * @param payload - The payload
   * @param groupId - The group id
   * @returns A promise that resolves to the result
   */
  private async pubSend(queueUrl: string, payload: object, groupId: string): Promise<string> {
    return this.withRetry(async () => {
      const data = Buffer.from(JSON.stringify(payload)).toString("base64");
      const topic = this.topicPath(queueUrl);
      this.logger.debug("pub_send_attempt", { queueUrl, topic, groupId });
      const pub = (await this.getPubPublisher()) as unknown as { publish: (opts: { topic: string; messages: { data: string; orderingKey: string }[] }) => Promise<[{ messageIds?: string[] }]> };
      const [resp] = await this.withTimeout<[{ messageIds?: string[] }]>(
        () => pub.publish({
          topic: topic,
          messages: [{ data, orderingKey: groupId }],
        }),
        this.QUEUE_TIMEOUT_SEND
      );
      const messageId = (resp.messageIds ?? [])[0] ?? "";
      this.logger.debug("pub_send_success", { topic, messageId });
      return messageId;
    });
  }

    /**
   * Performs the pub receive operation.
   * @param queueUrl - The queue url
   * @param parser - The parser
   * @param max - The max
   * @param wait - The wait
   * @returns A promise that resolves to the list
   */
  private async pubReceive<T>(queueUrl: string, parser: (b: string) => T, max: number, wait: number): Promise<QueueMessage<T>[]> {
    const sub = (await this.getPubSubscriber()) as unknown as { pull: (opts: { subscription: string; maxMessages: number }) => Promise<[{ receivedMessages?: { ackId?: string; message?: { data?: string } }[] }]> };
    const subscription = this.subscriptionPath(queueUrl);
    const deadline = Date.now() + wait * 1000;
    while (Date.now() < deadline) {
      try {
        const [resp] = await this.withRetry<[{ receivedMessages?: { ackId?: string; message?: { data?: string } }[] }]>(() => this.withTimeout<[{ receivedMessages?: { ackId?: string; message?: { data?: string } }[] }]>(() => sub.pull({ subscription, maxMessages: max }), this.QUEUE_TIMEOUT_RECEIVE), 2);
        const msgs = resp.receivedMessages ?? [];
        if (msgs.length) {
          return msgs.map((m) => {
            const msg = m as unknown as { ackId?: string; message?: { data?: string } };
            return {
              payload: parser(Buffer.from(msg.message?.data ?? "", "base64").toString()),
              receiptHandle: msg.ackId ?? "",
            };
          });
        }
      } catch (err) {
        this.logger.warn("pub_receive_error", { error: String(err) });
      }
      await new Promise((r) => setTimeout(r, 1500));
    }
    return [];
  }

    /**
   * Performs the pub delete operation.
   * @param queueUrl - The queue url
   * @param ackId - The ack id
   */
  private async pubDelete(queueUrl: string, ackId: string): Promise<void> {
    if (!ackId) return;
    await this.withRetry(async () => {
      const sub = (await this.getPubSubscriber()) as unknown as { acknowledge: (opts: { subscription: string; ackIds: string[] }) => Promise<unknown> };
      await this.withTimeout<unknown>(
        () => sub.acknowledge({ subscription: this.subscriptionPath(queueUrl), ackIds: [ackId] }),
        this.QUEUE_TIMEOUT_SEND
      );
    });
  }

    /**
   * Performs the pub modify ack deadline operation.
   * @param queueUrl - The queue url
   * @param ackId - The ack id
   * @param deadlineSeconds - The deadline seconds
   */
  private async pubModifyAckDeadline(queueUrl: string, ackId: string, deadlineSeconds: number): Promise<void> {
    if (!ackId) return;
    await this.withRetry(async () => {
      const sub = (await this.getPubSubscriber()) as unknown as { modifyAckDeadline: (opts: { subscription: string; ackIds: string[]; ackDeadlineSeconds: number }) => Promise<unknown> };
      await this.withTimeout<unknown>(
        () => sub.modifyAckDeadline({ subscription: this.subscriptionPath(queueUrl), ackIds: [ackId], ackDeadlineSeconds: deadlineSeconds }),
        this.QUEUE_TIMEOUT_SEND
      );
    });
  }

    /**
   * Performs the sqs send operation.
   * @param queueUrl - The queue url
   * @param payload - The payload
   * @param delay - The delay
   * @param groupId - The group id
   * @returns A promise that resolves to the result
   */
  private async sqsSend(queueUrl: string, payload: object, delay: number, groupId: string): Promise<string> {
    return this.withRetry(async () => {
      const params: SendMessageCommandInput = {
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(payload),
        DelaySeconds: delay,
      };
      if (queueUrl.endsWith(".fifo")) {
        const { randomUUID } = await import("crypto");
        params.MessageGroupId = groupId;
        params.MessageDeduplicationId = randomUUID();
      }
      const client = (await this.getSqsClient()) as SQSClient;
      const resp = await this.withTimeout<{ MessageId?: string }>(
        async () => {
          const { SendMessageCommand } = await import("@aws-sdk/client-sqs");
          return client.send(new SendMessageCommand(params));
        },
        this.QUEUE_TIMEOUT_SEND
      );
      return resp.MessageId ?? "";
    });
  }

    /**
   * Performs the sqs receive operation.
   * @param queueUrl - The queue url
   * @param parser - The parser
   * @param max - The max
   * @param wait - The wait
   * @returns A promise that resolves to the list
   */
  private async sqsReceive<T>(queueUrl: string, parser: (b: string) => T, max: number, wait: number): Promise<QueueMessage<T>[]> {
    return this.withRetry(async () => {
      const { ReceiveMessageCommand } = await import("@aws-sdk/client-sqs");
      const client = (await this.getSqsClient()) as SQSClient;
      const resp = await this.withTimeout<{ Messages?: { Body?: string; ReceiptHandle?: string }[] }>(
        () => client.send(new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: Math.min(max, 10),
          WaitTimeSeconds: Math.min(wait, 20),
          AttributeNames: ["All"],
          MessageAttributeNames: ["All"],
        })),
        this.QUEUE_TIMEOUT_RECEIVE
      );
      return (resp.Messages ?? []).map((m) => ({
        payload: parser(m.Body ?? ""),
        receiptHandle: m.ReceiptHandle ?? "",
      }));
    });
  }

    /**
   * Performs the sqs delete operation.
   * @param queueUrl - The queue url
   * @param receiptHandle - The receipt handle
   */
  private async sqsDelete(queueUrl: string, receiptHandle: string): Promise<void> {
    if (!receiptHandle) return;
    await this.withRetry(async () => {
      const { DeleteMessageCommand } = await import("@aws-sdk/client-sqs");
      const client = (await this.getSqsClient()) as SQSClient;
      await this.withTimeout<unknown>(
        () => client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: receiptHandle })),
        this.QUEUE_TIMEOUT_SEND
      );
    });
  }

    /**
   * Sends message
   * @param queueUrl - The queue url
   * @param payload - The payload
   * @param delaySeconds - The delay seconds
   * @param messageGroupId - The message group id
   * @returns A promise that resolves to the result
   */
  public async sendMessage(
    queueUrl: string,
    payload: object,
    delaySeconds = 0,
    messageGroupId?: string
  ): Promise<string> {
    const gid = messageGroupId ?? ((payload as Record<string, unknown>).job_id as string | undefined) ?? "default";
    return this.isPubSub()
      ? this.pubSend(queueUrl, payload, gid)
      : this.sqsSend(queueUrl, payload, delaySeconds, gid);
  }

    /**
   * Sends raw
   * @param queueUrl - The queue url
   * @param body - The body
   * @param delaySeconds - The delay seconds
   * @returns A promise that resolves to the result
   */
  public async sendRaw(queueUrl: string, body: Record<string, unknown>, delaySeconds = 0): Promise<string> {
    return this.sendMessage(queueUrl, body, delaySeconds, (body.job_id as string | undefined) ?? "default");
  }

    /**
   * Receives messages
   * @param queueUrl - The queue url
   * @param parser - The parser
   * @param maxMessages - The max messages
   * @param waitSeconds - The wait seconds
   * @returns A promise that resolves to the list
   */
  public async receiveMessages<T extends object>(
    queueUrl: string,
    parser: (body: string) => T,
    maxMessages = 1,
    waitSeconds = 20
  ): Promise<QueueMessage<T>[]> {
    return this.isPubSub()
      ? this.pubReceive(queueUrl, parser, maxMessages, waitSeconds)
      : this.sqsReceive(queueUrl, parser, maxMessages, waitSeconds);
  }

    /**
   * Deletes message
   * @param queueUrl - The queue url
   * @param receiptHandle - The receipt handle
   */
  public async deleteMessage(queueUrl: string, receiptHandle: string): Promise<void> {
    return this.isPubSub()
      ? this.pubDelete(queueUrl, receiptHandle)
      : this.sqsDelete(queueUrl, receiptHandle);
  }

    /**
   * Modifies ack deadline
   * @param queueUrl - The queue url
   * @param receiptHandle - The receipt handle
   * @param deadlineSeconds - The deadline seconds
   */
  public async modifyAckDeadline(queueUrl: string, receiptHandle: string, deadlineSeconds: number): Promise<void> {
    if (this.isPubSub()) {
      return this.pubModifyAckDeadline(queueUrl, receiptHandle, deadlineSeconds);
    }
    // SQS doesn't need explicit deadline extension - it uses visibility timeout
    // For SQS, we would use ChangeMessageVisibility, but not needed for current use case
  }

    /**
   * Publishes event
   * @param event - The event
   * @returns A promise that resolves to the result
   */
  public async publishEvent(event: object): Promise<string | null> {
    const config = this.getConfig();
    return this.sendMessage(config.settings.JOB_EVENTS_QUEUE_URL, event, 0, ((event as Record<string, unknown>).job_id as string | undefined) ?? "default").catch((err) => {
      this.logger.warn("publish_event_failed", { error: String(err) });
      return null;
    });
  }
}

export default QueueService;

/**
 * The queue service
 */
const queueService = QueueService.getInstance();

/**
 * Sends message
 * @param queueUrl - The queue url
 * @param payload - The payload
 * @param delaySeconds - The delay seconds
 * @param messageGroupId - The message group id
 * @returns A promise that resolves to the result
 */
export async function sendMessage(
  queueUrl: string,
  payload: object,
  delaySeconds = 0,
  messageGroupId?: string
): Promise<string> {
  return queueService.sendMessage(queueUrl, payload, delaySeconds, messageGroupId);
}

/**
 * Sends raw
 * @param queueUrl - The queue url
 * @param body - The body
 * @param delaySeconds - The delay seconds
 * @returns A promise that resolves to the result
 */
export async function sendRaw(queueUrl: string, body: Record<string, unknown>, delaySeconds = 0): Promise<string> {
  return queueService.sendRaw(queueUrl, body, delaySeconds);
}

/**
 * Receives messages
 * @param queueUrl - The queue url
 * @param parser - The parser
 * @param maxMessages - The max messages
 * @param waitSeconds - The wait seconds
 * @returns A promise that resolves to the list
 */
export async function receiveMessages<T extends object>(
  queueUrl: string,
  parser: (body: string) => T,
  maxMessages = 1,
  waitSeconds = 20
): Promise<QueueMessage<T>[]> {
  return queueService.receiveMessages(queueUrl, parser, maxMessages, waitSeconds);
}

/**
 * Deletes message
 * @param queueUrl - The queue url
 * @param receiptHandle - The receipt handle
 */
export async function deleteMessage(queueUrl: string, receiptHandle: string): Promise<void> {
  return queueService.deleteMessage(queueUrl, receiptHandle);
}

/**
 * Modifies ack deadline
 * @param queueUrl - The queue url
 * @param receiptHandle - The receipt handle
 * @param deadlineSeconds - The deadline seconds
 */
export async function modifyAckDeadline(queueUrl: string, receiptHandle: string, deadlineSeconds: number): Promise<void> {
  return queueService.modifyAckDeadline(queueUrl, receiptHandle, deadlineSeconds);
}

/**
 * Publishes event
 * @param event - The event
 * @returns A promise that resolves to the result
 */
export async function publishEvent(event: object): Promise<string | null> {
  return queueService.publishEvent(event);
}
