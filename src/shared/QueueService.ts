import pino from "pino";
import type { SQSClient, SQSClientConfig, SendMessageCommandInput } from "@aws-sdk/client-sqs";
import ServiceManager from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { createLogger } from "@utils/logger/Log.js";
import {QueueMessage} from "@shared/io/IQueueService";

/**
 * QueueService is a singleton class responsible for managing the queue
 * backend (SQS or Pub/Sub, selected via config) for the application.
 * It provides methods to send, receive, delete, and manage the
 * visibility/ack-deadline of queue messages.
 *
 * Every member below declares its access modifier explicitly
 * (public / private / protected / static) rather than relying on
 * TypeScript's implicit-public default.
 *
 * There are no free functions in this module. The previous
 * module-level `Enforce()` guard and the six exported wrapper
 * functions (sendMessage, sendRaw, receiveMessages, deleteMessage,
 * modifyAckDeadline, publishEvent) have been folded into the class as
 * a private static enforcement method and public static passthroughs,
 * respectively, so the entire public surface of this module is the
 * QueueService class itself.
 */
export class QueueService extends ServiceManager
{
  /**
   * Singleton instance
   * @protected
   */
  protected static instance: QueueService;

  /**
   * Logger instance
   * @private
   */
  private readonly logger: pino.Logger;

  /**
   * Number of retry attempts for retryable queue operations
   * @private
   */
  private readonly QUEUE_RETRIES: number = 3;

  /**
   * Base delay, in milliseconds, between retry attempts (doubles per attempt)
   * @private
   */
  private readonly QUEUE_RETRY_DELAY: number = 200;

  /**
   * Timeout, in milliseconds, for send-type queue operations
   * @private
   */
  private readonly QUEUE_TIMEOUT_SEND: number = 60000;

  /**
   * Timeout, in milliseconds, for receive-type queue operations
   * @private
   */
  private readonly QUEUE_TIMEOUT_RECEIVE: number = 120000;

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
   * Private — use QueueService.getInstance() instead.
   * @param enforce - A function reference used to enforce the singleton pattern
   * @throws InstantiationError if instantiated directly
   */
  private constructor(enforce: () => void)
  {
    if (enforce !== QueueService.enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Cannot instantiate QueueService directly. Use getInstance()");
    }
    super(enforce);

    this.logger = createLogger(module);
  }

  /**
   * Private no-op used purely as an identity token to enforce that
   * QueueService can only be constructed from within getInstance().
   * TypeScript's `private constructor` already blocks external
   * instantiation at compile time; this additional runtime check
   * guards against callers that bypass the type system (e.g. via
   * `as any`) and mirrors the guard expected by ServiceManager's base
   * constructor signature.
   * @private
   */
  private static enforce(): void {}

  /**
   * Gets the single instance of the QueueService class.
   * @returns The single instance of the class
   */
  public static getInstance(): QueueService
  {
    if (!QueueService.instance)
    {
      QueueService.instance = new QueueService(QueueService.enforce);
    }
    return QueueService.instance;
  }

  /**
   * Checks whether an error is retryable.
   * @param err - The error that occurred
   * @returns True if the condition is met, false otherwise
   * @private
   */
  private isRetryable(err: unknown): boolean
  {
    if (!err) return false;
    const code = (err as { code?: string | number }).code;
    if (typeof code === "number") return code === 429 || code >= 500;
    if (typeof code === "string")
    {
      return ["ECONNRESET", "ETIMEDOUT", "ENOTFOUND", "ECONNREFUSED", "EPIPE"].includes(code);
    }
    return true;
  }

  /**
   * Races a promise-returning function against a timeout.
   * @param fn - The function to run
   * @param ms - The timeout, in milliseconds
   * @returns A promise that resolves to the result
   * @private
   */
  private async withTimeout<T>(fn: () => Promise<T>, ms: number): Promise<T>
  {
    return new Promise<T>((resolve, reject) =>
    {
      const timer = setTimeout(() =>
      {
        const err = new Error(`Queue operation timed out after ${ms}ms`);
        (err as unknown as { code?: string }).code = "ETIMEDOUT";
        reject(err);
      }, ms);

      fn().then(
          (value) =>
          {
            clearTimeout(timer);
            resolve(value);
          },
          (err) =>
          {
            clearTimeout(timer);
            reject(err);
          }
      );
    });
  }

  /**
   * Runs a function with exponential-backoff retry for retryable errors.
   * @param fn - The function to run
   * @param retries - The number of retries
   * @param delay - The base delay between retries
   * @returns A promise that resolves to the result
   * @private
   */
  private async withRetry<T>(fn: () => Promise<T>, retries: number = this.QUEUE_RETRIES, delay: number = this.QUEUE_RETRY_DELAY): Promise<T>
  {
    let lastErr: unknown;
    for (let i = 0; i <= retries; i++)
    {
      try
      {
        return await fn();
      }
      catch (err)
      {
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
   * Checks whether the configured queue backend is Pub/Sub.
   * @returns True if the condition is met, false otherwise
   * @private
   */
  private isPubSub(): boolean
  {
    const config = this.getConfig();
    return config.settings.QUEUE_BACKEND === "pubsub";
  }

  /**
   * Gets (and lazily creates) the Pub/Sub publisher client.
   * @returns A promise that resolves to the publisher client
   * @private
   */
  private async getPubPublisher(): Promise<unknown>
  {
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
   * Gets (and lazily creates) the Pub/Sub subscriber client.
   * @returns A promise that resolves to the subscriber client
   * @private
   */
  private async getPubSubscriber(): Promise<unknown>
  {
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
   * Resolves a queue identifier to a fully-qualified Pub/Sub topic path.
   * @param q - The queue identifier or full topic path
   * @returns The resolved topic path
   * @private
   */
  private topicPath(q: string): string
  {
    const config = this.getConfig();
    if (q.startsWith("projects/")) return q;
    return `projects/${config.settings.GCP_PROJECT_ID}/topics/${q.split("/").pop()!.replace(/\.fifo$/, "")}`;
  }

  /**
   * Resolves a queue identifier to a fully-qualified Pub/Sub subscription path.
   * @param q - The queue identifier or full subscription path
   * @returns The resolved subscription path
   * @private
   */
  private subscriptionPath(q: string): string
  {
    const config = this.getConfig();
    if (q.includes("/subscriptions/")) return q;
    return `projects/${config.settings.GCP_PROJECT_ID}/subscriptions/${q.split("/").pop()!.replace(/\.fifo$/, "")}-sub`;
  }

  /**
   * Gets (and lazily creates) the SQS client.
   * @returns A promise that resolves to the SQS client
   * @private
   */
  private async getSqsClient(): Promise<unknown>
  {
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
   * Sends a message via Pub/Sub.
   * @param queueUrl - The queue url
   * @param payload - The payload
   * @param groupId - The group id (used as the ordering key)
   * @returns A promise that resolves to the result
   * @private
   */
  private async pubSend(queueUrl: string, payload: object, groupId: string): Promise<string>
  {
    return this.withRetry(async () =>
    {
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
   * Receives messages via Pub/Sub.
   * @param queueUrl - The queue url
   * @param parser - The parser
   * @param max - The max
   * @param wait - The wait
   * @returns A promise that resolves to the list
   * @private
   */
  private async pubReceive<T>(queueUrl: string, parser: (b: string) => T, max: number, wait: number): Promise<QueueMessage<T>[]>
  {
    const sub = (await this.getPubSubscriber()) as unknown as { pull: (opts: { subscription: string; maxMessages: number }) => Promise<[{ receivedMessages?: { ackId?: string; message?: { data?: string } }[] }]> };
    const subscription = this.subscriptionPath(queueUrl);
    const deadline = Date.now() + wait * 1000;
    while (Date.now() < deadline)
    {
      try
      {
        const [resp] = await this.withRetry<[{ receivedMessages?: { ackId?: string; message?: { data?: string } }[] }]>(() => this.withTimeout<[{ receivedMessages?: { ackId?: string; message?: { data?: string } }[] }]>(() => sub.pull({ subscription, maxMessages: max }), this.QUEUE_TIMEOUT_RECEIVE), 2);
        const msgs = resp.receivedMessages ?? [];
        if (msgs.length)
        {
          return msgs.map((m) =>
          {
            const msg = m as unknown as { ackId?: string; message?: { data?: string } };
            return {
              payload: parser(Buffer.from(msg.message?.data ?? "", "base64").toString()),
              receiptHandle: msg.ackId ?? "",
            };
          });
        }
      }
      catch (err)
      {
        this.logger.warn({ error: String(err) }, "pub_receive_error");
      }
      await new Promise((r) => setTimeout(r, 1500));
    }
    return [];
  }

  /**
   * Deletes (acknowledges) a message via Pub/Sub.
   * @param queueUrl - The queue url
   * @param ackId - The ack id
   * @private
   */
  private async pubDelete(queueUrl: string, ackId: string): Promise<void>
  {
    if (!ackId) return;
    await this.withRetry(async () =>
    {
      const sub = (await this.getPubSubscriber()) as unknown as { acknowledge: (opts: { subscription: string; ackIds: string[] }) => Promise<unknown> };
      await this.withTimeout<unknown>(
          () => sub.acknowledge({ subscription: this.subscriptionPath(queueUrl), ackIds: [ackId] }),
          this.QUEUE_TIMEOUT_SEND
      );
    });
  }

  /**
   * Extends the ack deadline of a message via Pub/Sub.
   * @param queueUrl - The queue url
   * @param ackId - The ack id
   * @param deadlineSeconds - The deadline seconds
   * @private
   */
  private async pubModifyAckDeadline(queueUrl: string, ackId: string, deadlineSeconds: number): Promise<void>
  {
    if (!ackId) return;
    await this.withRetry(async () =>
    {
      const sub = (await this.getPubSubscriber()) as unknown as { modifyAckDeadline: (opts: { subscription: string; ackIds: string[]; ackDeadlineSeconds: number }) => Promise<unknown> };
      await this.withTimeout<unknown>(
          () => sub.modifyAckDeadline({ subscription: this.subscriptionPath(queueUrl), ackIds: [ackId], ackDeadlineSeconds: deadlineSeconds }),
          this.QUEUE_TIMEOUT_SEND
      );
    });
  }

  /**
   * Sends a message via SQS.
   * @param queueUrl - The queue url
   * @param payload - The payload
   * @param delay - The delay, in seconds
   * @param groupId - The message group id (FIFO queues only)
   * @returns A promise that resolves to the result
   * @private
   */
  private async sqsSend(queueUrl: string, payload: object, delay: number, groupId: string): Promise<string>
  {
    return this.withRetry(async () =>
    {
      const params: SendMessageCommandInput = {
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(payload),
        DelaySeconds: delay,
      };
      if (queueUrl.endsWith(".fifo"))
      {
        const { randomUUID } = await import("crypto");
        params.MessageGroupId = groupId;
        params.MessageDeduplicationId = randomUUID();
      }
      const client = (await this.getSqsClient()) as SQSClient;
      const resp = await this.withTimeout<{ MessageId?: string }>(
          async () =>
          {
            const { SendMessageCommand } = await import("@aws-sdk/client-sqs");
            return client.send(new SendMessageCommand(params));
          },
          this.QUEUE_TIMEOUT_SEND
      );
      return resp.MessageId ?? "";
    });
  }

  /**
   * Receives messages via SQS.
   * @param queueUrl - The queue url
   * @param parser - The parser
   * @param max - The max
   * @param wait - The wait
   * @returns A promise that resolves to the list
   * @private
   */
  private async sqsReceive<T>(queueUrl: string, parser: (b: string) => T, max: number, wait: number): Promise<QueueMessage<T>[]>
  {
    return this.withRetry(async () =>
    {
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
   * Deletes a message via SQS.
   * @param queueUrl - The queue url
   * @param receiptHandle - The receipt handle
   * @private
   */
  private async sqsDelete(queueUrl: string, receiptHandle: string): Promise<void>
  {
    if (!receiptHandle) return;
    await this.withRetry(async () =>
    {
      const { DeleteMessageCommand } = await import("@aws-sdk/client-sqs");
      const client = (await this.getSqsClient()) as SQSClient;
      await this.withTimeout<unknown>(
          () => client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: receiptHandle })),
          this.QUEUE_TIMEOUT_SEND
      );
    });
  }

  /**
   * Sends a message to the configured queue backend.
   * @param queueUrl - The queue url
   * @param payload - The payload
   * @param delaySeconds - The delay seconds
   * @param messageGroupId - The message group id
   * @returns A promise that resolves to the result
   */
  public async sendMessage(
      queueUrl: string,
      payload: object,
      delaySeconds: number = 0,
      messageGroupId?: string
  ): Promise<string>
  {
    const gid = messageGroupId ?? ((payload as Record<string, unknown>).job_id as string | undefined) ?? "default";
    return this.isPubSub()
        ? this.pubSend(queueUrl, payload, gid)
        : this.sqsSend(queueUrl, payload, delaySeconds, gid);
  }

  /**
   * Sends a raw message body, deriving the group id from the body's job_id.
   * @param queueUrl - The queue url
   * @param body - The body
   * @param delaySeconds - The delay seconds
   * @returns A promise that resolves to the result
   */
  public async sendRaw(queueUrl: string, body: Record<string, unknown>, delaySeconds: number = 0): Promise<string>
  {
    return this.sendMessage(queueUrl, body, delaySeconds, (body.job_id as string | undefined) ?? "default");
  }

  /**
   * Receives messages from the configured queue backend.
   * @param queueUrl - The queue url
   * @param parser - The parser
   * @param maxMessages - The max messages
   * @param waitSeconds - The wait seconds
   * @returns A promise that resolves to the list
   */
  public async receiveMessages<T extends object>(
      queueUrl: string,
      parser: (body: string) => T,
      maxMessages: number = 1,
      waitSeconds: number = 20
  ): Promise<QueueMessage<T>[]>
  {
    return this.isPubSub()
        ? this.pubReceive(queueUrl, parser, maxMessages, waitSeconds)
        : this.sqsReceive(queueUrl, parser, maxMessages, waitSeconds);
  }

  /**
   * Deletes a message from the configured queue backend.
   * @param queueUrl - The queue url
   * @param receiptHandle - The receipt handle
   */
  public async deleteMessage(queueUrl: string, receiptHandle: string): Promise<void>
  {
    return this.isPubSub()
        ? this.pubDelete(queueUrl, receiptHandle)
        : this.sqsDelete(queueUrl, receiptHandle);
  }

  /**
   * Extends the visibility/ack deadline of an in-flight message.
   * No-op for SQS in the current implementation (SQS relies on the
   * queue's visibility timeout rather than explicit extension calls).
   * @param queueUrl - The queue url
   * @param receiptHandle - The receipt handle
   * @param deadlineSeconds - The deadline seconds
   */
  public async modifyAckDeadline(queueUrl: string, receiptHandle: string, deadlineSeconds: number): Promise<void>
  {
    if (this.isPubSub())
    {
      return this.pubModifyAckDeadline(queueUrl, receiptHandle, deadlineSeconds);
    }
    // SQS doesn't need explicit deadline extension - it uses visibility timeout.
    // For SQS, ChangeMessageVisibility would be used here, but it isn't
    // required for the current use case.
  }

  /**
   * Publishes a job event to the configured job-events queue. Failures
   * are logged and swallowed (returns null) rather than propagated.
   * @param event - The event
   * @returns A promise that resolves to the result
   */
  public async publishEvent(event: object): Promise<string | null>
  {
    const config = this.getConfig();
    return this.sendMessage(config.settings.JOB_EVENTS_QUEUE_URL, event, 0, ((event as Record<string, unknown>).job_id as string | undefined) ?? "default").catch((err) =>
    {
      this.logger.warn("publish_event_failed", { error: String(err) });
      return null;
    });
  }

  /**
   * Static passthrough — sends a message to the configured queue backend.
   * @param queueUrl - The queue url
   * @param payload - The payload
   * @param delaySeconds - The delay seconds
   * @param messageGroupId - The message group id
   * @returns A promise that resolves to the result
   */
  public static async sendMessage(
      queueUrl: string,
      payload: object,
      delaySeconds: number = 0,
      messageGroupId?: string
  ): Promise<string>
  {
    return QueueService.getInstance().sendMessage(queueUrl, payload, delaySeconds, messageGroupId);
  }

  /**
   * Static passthrough — sends a raw message body.
   * @param queueUrl - The queue url
   * @param body - The body
   * @param delaySeconds - The delay seconds
   * @returns A promise that resolves to the result
   */
  public static async sendRaw(queueUrl: string, body: Record<string, unknown>, delaySeconds: number = 0): Promise<string>
  {
    return QueueService.getInstance().sendRaw(queueUrl, body, delaySeconds);
  }

  /**
   * Static passthrough — receives messages from the configured queue backend.
   * @param queueUrl - The queue url
   * @param parser - The parser
   * @param maxMessages - The max messages
   * @param waitSeconds - The wait seconds
   * @returns A promise that resolves to the list
   */
  public static async receiveMessages<T extends object>(
      queueUrl: string,
      parser: (body: string) => T,
      maxMessages: number = 1,
      waitSeconds: number = 20
  ): Promise<QueueMessage<T>[]>
  {
    return QueueService.getInstance().receiveMessages(queueUrl, parser, maxMessages, waitSeconds);
  }

  /**
   * Static passthrough — deletes a message from the configured queue backend.
   * @param queueUrl - The queue url
   * @param receiptHandle - The receipt handle
   */
  public static async deleteMessage(queueUrl: string, receiptHandle: string): Promise<void>
  {
    return QueueService.getInstance().deleteMessage(queueUrl, receiptHandle);
  }

  /**
   * Static passthrough — extends the ack deadline of an in-flight message.
   * @param queueUrl - The queue url
   * @param receiptHandle - The receipt handle
   * @param deadlineSeconds - The deadline seconds
   */
  public static async modifyAckDeadline(queueUrl: string, receiptHandle: string, deadlineSeconds: number): Promise<void>
  {
    return QueueService.getInstance().modifyAckDeadline(queueUrl, receiptHandle, deadlineSeconds);
  }

  /**
   * Static passthrough — publishes a job event.
   * @param event - The event
   * @returns A promise that resolves to the result
   */
  public static async publishEvent(event: object): Promise<string | null>
  {
    return QueueService.getInstance().publishEvent(event);
  }
}
