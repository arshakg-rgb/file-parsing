/**
 * Queue abstraction — delegates to either Google Cloud Pub/Sub or SQS.
 * Set QUEUE_BACKEND=pubsub for GCP production, QUEUE_BACKEND=sqs (default) for local dev.
 */
import { settings } from "./config.js";

const QUEUE_RETRIES = 3;
const QUEUE_RETRY_DELAY = 200;
const QUEUE_TIMEOUT_SEND = 60000; // Increased from 10s to 60s for large files
const QUEUE_TIMEOUT_RECEIVE = 120000; // Increased from 60s to 120s for Pub/Sub subscription issues

function isRetryable(err: any): boolean {
  if (!err) return false;
  const code = err.code;
  if (typeof code === "number") return code === 429 || code >= 500;
  if (typeof code === "string") {
    return ["ECONNRESET", "ETIMEDOUT", "ENOTFOUND", "ECONNREFUSED", "EPIPE"].includes(code);
  }
  return true;
}

async function withTimeout<T>(fn: () => Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      const err = new Error(`Queue operation timed out after ${ms}ms`);
      (err as any).code = "ETIMEDOUT";
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

async function withRetry<T>(fn: () => Promise<T>, retries = QUEUE_RETRIES, delay = QUEUE_RETRY_DELAY): Promise<T> {
  let lastErr: any;
  for (let i = 0; i <= retries; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (i === retries || !isRetryable(err)) throw err;
      const wait = delay * 2 ** i;
      console.warn("queue_retry", { attempt: i + 1, wait, error: String(err) });
      await new Promise((r) => setTimeout(r, wait));
    }
  }
  throw lastErr;
}

export interface QueueMessage<T> {
  payload: T;
  receiptHandle: string; // ackId (Pub/Sub) or ReceiptHandle (SQS)
}

// ─────────────────────────────────────────────────────────────────────────────
// Pub/Sub backend
// ─────────────────────────────────────────────────────────────────────────────

let _pubPublisher: any;
let _pubSubscriber: any;

async function pubPublisher() {
  if (_pubPublisher) return _pubPublisher;
  const { v1 } = await import("@google-cloud/pubsub");
  _pubPublisher = new v1.PublisherClient({
    projectId: settings.GCP_PROJECT_ID,
    ...(settings.GOOGLE_APPLICATION_CREDENTIALS ? { keyFilename: settings.GOOGLE_APPLICATION_CREDENTIALS } : {}),
  });
  return _pubPublisher;
}

async function pubSubscriber() {
  if (_pubSubscriber) return _pubSubscriber;
  const { v1 } = await import("@google-cloud/pubsub");
  _pubSubscriber = new v1.SubscriberClient({
    projectId: settings.GCP_PROJECT_ID,
    ...(settings.GOOGLE_APPLICATION_CREDENTIALS ? { keyFilename: settings.GOOGLE_APPLICATION_CREDENTIALS } : {}),
  });
  return _pubSubscriber;
}

function topicPath(q: string): string {
  if (q.startsWith("projects/")) return q;
  return `projects/${settings.GCP_PROJECT_ID}/topics/${q.split("/").pop()!.replace(/\.fifo$/, "")}`;
}

function subscriptionPath(q: string): string {
  if (q.includes("/subscriptions/")) return q;
  return `projects/${settings.GCP_PROJECT_ID}/subscriptions/${q.split("/").pop()!.replace(/\.fifo$/, "")}-sub`;
}

async function pubSend(queueUrl: string, payload: object, groupId: string): Promise<string> {
  return withRetry(async () => {
    const data = Buffer.from(JSON.stringify(payload)).toString("base64");
    const pub = await pubPublisher();
    const [resp] = await withTimeout<any>(
      () => pub.publish({
        topic: topicPath(queueUrl),
        messages: [{ data, orderingKey: groupId }],
      }),
      QUEUE_TIMEOUT_SEND
    );
    return (resp.messageIds ?? [])[0] ?? "";
  });
}

async function pubReceive<T>(queueUrl: string, parser: (b: string) => T, max: number, wait: number): Promise<QueueMessage<T>[]> {
  const sub = await pubSubscriber();
  const subscription = subscriptionPath(queueUrl);
  const deadline = Date.now() + wait * 1000;
  while (Date.now() < deadline) {
    try {
      const [resp] = await withRetry<[any]>(() => withTimeout<any>(() => sub.pull({ subscription, maxMessages: max }), QUEUE_TIMEOUT_RECEIVE), 2);
      const msgs = resp.receivedMessages ?? [];
      if (msgs.length) {
        return msgs.map((m: any) => ({
          payload: parser(Buffer.from(m.message?.data, "base64").toString()),
          receiptHandle: m.ackId ?? "",
        }));
      }
    } catch (err) {
      console.warn("pub_receive_error", { error: String(err) });
    }
    await new Promise((r) => setTimeout(r, 1500));
  }
  return [];
}

async function pubDelete(queueUrl: string, ackId: string): Promise<void> {
  if (!ackId) return;
  await withRetry(async () => {
    const sub = await pubSubscriber();
    await withTimeout<void>(
      () => sub.acknowledge({ subscription: subscriptionPath(queueUrl), ackIds: [ackId] }),
      QUEUE_TIMEOUT_SEND
    );
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// SQS backend (LocalStack for local dev)
// ─────────────────────────────────────────────────────────────────────────────

let _sqsClient: any;

async function sqsClient() {
  if (_sqsClient) return _sqsClient;
  const { SQSClient } = await import("@aws-sdk/client-sqs");
  const cfg: any = { region: "us-east-1" };
  const ep = process.env.AWS_ENDPOINT_URL;
  if (ep) { cfg.endpoint = ep; cfg.forcePathStyle = true; }
  const id = process.env.AWS_ACCESS_KEY_ID;
  const sec = process.env.AWS_SECRET_ACCESS_KEY;
  if (id && sec) cfg.credentials = { accessKeyId: id, secretAccessKey: sec };
  _sqsClient = new SQSClient(cfg);
  return _sqsClient;
}

async function sqsSend(queueUrl: string, payload: object, delay: number, groupId: string): Promise<string> {
  return withRetry(async () => {
    const { SendMessageCommand } = await import("@aws-sdk/client-sqs");
    const { randomUUID } = await import("crypto");
    const client = await sqsClient();
    const params: any = {
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify(payload),
      DelaySeconds: delay,
    };
    if (queueUrl.endsWith(".fifo")) {
      params.MessageGroupId = groupId;
      params.MessageDeduplicationId = randomUUID();
    }
    const resp = await withTimeout<any>(() => client.send(new SendMessageCommand(params)), QUEUE_TIMEOUT_SEND);
    return resp.MessageId ?? "";
  });
}

async function sqsReceive<T>(queueUrl: string, parser: (b: string) => T, max: number, wait: number): Promise<QueueMessage<T>[]> {
  return withRetry(async () => {
    const { ReceiveMessageCommand } = await import("@aws-sdk/client-sqs");
    const client = await sqsClient();
    const resp = await withTimeout<any>(
      () => client.send(new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: Math.min(max, 10),
        WaitTimeSeconds: Math.min(wait, 20),
        AttributeNames: ["All"],
        MessageAttributeNames: ["All"],
      })),
      QUEUE_TIMEOUT_RECEIVE
    );
    return (resp.Messages ?? []).map((m: any) => ({
      payload: parser(m.Body),
      receiptHandle: m.ReceiptHandle ?? "",
    }));
  });
}

async function sqsDelete(queueUrl: string, receiptHandle: string): Promise<void> {
  if (!receiptHandle) return;
  await withRetry(async () => {
    const { DeleteMessageCommand } = await import("@aws-sdk/client-sqs");
    const client = await sqsClient();
    await withTimeout<void>(
      () => client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: receiptHandle })),
      QUEUE_TIMEOUT_SEND
    );
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

const isPubSub = () => settings.QUEUE_BACKEND === "pubsub";

export async function sendMessage(
  queueUrl: string,
  payload: object,
  delaySeconds = 0,
  messageGroupId?: string
): Promise<string> {
  const gid = messageGroupId ?? (payload as any).job_id ?? "default";
  return isPubSub()
    ? pubSend(queueUrl, payload, gid)
    : sqsSend(queueUrl, payload, delaySeconds, gid);
}

export async function sendRaw(queueUrl: string, body: Record<string, any>, delaySeconds = 0): Promise<string> {
  return sendMessage(queueUrl, body, delaySeconds, body.job_id ?? "default");
}

export async function receiveMessages<T extends object>(
  queueUrl: string,
  parser: (body: string) => T,
  maxMessages = 1,
  waitSeconds = 20
): Promise<QueueMessage<T>[]> {
  return isPubSub()
    ? pubReceive(queueUrl, parser, maxMessages, waitSeconds)
    : sqsReceive(queueUrl, parser, maxMessages, waitSeconds);
}

export async function deleteMessage(queueUrl: string, receiptHandle: string): Promise<void> {
  return isPubSub()
    ? pubDelete(queueUrl, receiptHandle)
    : sqsDelete(queueUrl, receiptHandle);
}

export async function publishEvent(event: object): Promise<string | null> {
  return sendMessage(settings.JOB_EVENTS_QUEUE_URL, event, 0, (event as any).job_id ?? "default").catch((err) => {
    console.warn("publish_event_failed", { error: String(err) });
    return null;
  });
}
