/**
 * One-time setup: create Pub/Sub topics + subscriptions for all pipeline queues.
 * Run once per GCP project: npm run setup:gcp
 */
import { PubSub } from "@google-cloud/pubsub";
import { settings } from "../shared/Settings.js";

const TOPICS = [
  "fpp-ingest",
  "fpp-classify",
  "fpp-parse",
  "fpp-line-dlq",
  "fpp-load",
  "fpp-report",
  "fpp-job-events",
];

async function ensureTopic(pubsub: PubSub, name: string): Promise<void> {
  try {
    await pubsub.createTopic({ name: `projects/${settings.GCP_PROJECT_ID}/topics/${name}`, messageRetentionDuration: { seconds: 604800 } });
    console.log(`Created topic: ${name}`);
  } catch (err: unknown) {
    const e = err as { code?: number };
    if (e.code === 6) {
      console.log(`Topic already exists: ${name}`);
    } else {
      throw err;
    }
  }
}

async function ensureSubscription(pubsub: PubSub, topicName: string): Promise<void> {
  const subName = `${topicName}-sub`;
  try {
    await pubsub.createSubscription(
      `projects/${settings.GCP_PROJECT_ID}/topics/${topicName}`,
      `projects/${settings.GCP_PROJECT_ID}/subscriptions/${subName}`,
      {
        enableMessageOrdering: true,
        ackDeadlineSeconds: 300,
        retryPolicy: {
          minimumBackoff: { seconds: 1 },
          maximumBackoff: { seconds: 60 },
        },
      }
    );
    console.log(`Created subscription: ${subName}`);
  } catch (err: unknown) {
    const e = err as { code?: number };
    if (e.code === 6) {
      console.log(`Subscription already exists: ${subName}`);
    } else {
      throw err;
    }
  }
}

async function main() {
  const pubsub = new PubSub({
    projectId: settings.GCP_PROJECT_ID,
    ...(settings.GOOGLE_APPLICATION_CREDENTIALS
      ? { keyFilename: settings.GOOGLE_APPLICATION_CREDENTIALS }
      : {}),
  });

  console.log(`Setting up Pub/Sub for project: ${settings.GCP_PROJECT_ID}`);

  for (const topic of TOPICS) {
    await ensureTopic(pubsub, topic);
    await ensureSubscription(pubsub, topic);
  }

  console.log("Done");
}

main().catch((e) => { console.error(e); process.exit(1); });
