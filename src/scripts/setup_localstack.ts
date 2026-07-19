import { S3Client, CreateBucketCommand, ListBucketsCommand } from "@aws-sdk/client-s3";
import { SQSClient, CreateQueueCommand, ListQueuesCommand, GetQueueUrlCommand, SetQueueAttributesCommand } from "@aws-sdk/client-sqs";
import { DynamoDBClient, CreateTableCommand, DescribeTableCommand } from "@aws-sdk/client-dynamodb";

/**
 * The e n d p o i n t
 */
const ENDPOINT = "http://localhost:4566";
/**
 * The r e g i o n
 */
const REGION = "us-east-1";
/**
 * The c r e d e n t i a l s
 */
const CREDENTIALS = { accessKeyId: "test", secretAccessKey: "test" };

/**
 * The s3
 */
const s3 = new S3Client({ endpoint: ENDPOINT, region: REGION, credentials: CREDENTIALS, forcePathStyle: true });
/**
 * The sqs
 */
const sqs = new SQSClient({ endpoint: ENDPOINT, region: REGION, credentials: CREDENTIALS });
/**
 * The dynamodb
 */
const dynamodb = new DynamoDBClient({ endpoint: ENDPOINT, region: REGION, credentials: CREDENTIALS });

/**
 * Waits for for localstack
 */
async function waitForLocalstack(): Promise<void> {
  console.log("Waiting for LocalStack...");
  for (let i = 0; i < 30; i++) {
    try {
      await s3.send(new ListBucketsCommand({}));
      console.log("LocalStack ready");
      return;
    } catch {
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  throw new Error("LocalStack did not start within 60 seconds");
}

/**
 * Creates buckets
 */
async function createBuckets() {
  for (const bucket of ["datalead-osint"]) {
    try {
      await s3.send(new CreateBucketCommand({ Bucket: bucket }));
      console.log(`Created bucket: ${bucket}`);
    } catch (err: unknown) {
      const e = err as { name?: string };
      if (e.name !== "BucketAlreadyExists") console.log(`Bucket already exists: ${bucket}`);
    }
  }
}

/**
 * Creates queues
 */
async function createQueues() {
  const queues = ["fpp-ingest.fifo", "fpp-classify.fifo", "fpp-parse.fifo", "fpp-line-dlq.fifo", "fpp-load.fifo", "fpp-report.fifo", "fpp-job-events.fifo"];
  for (const q of queues) {
    try {
      await sqs.send(new CreateQueueCommand({
        QueueName: q,
        Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false", VisibilityTimeout: "600" },
      }));
      console.log(`Created queue: ${q}`);
    } catch (err: unknown) {
      const e = err as { name?: string; message?: string };
      if (e.message?.includes("already exists") || e.name === "QueueAlreadyExists") {
        console.log(`Queue already exists: ${q}`);
        const url = await sqs.send(new GetQueueUrlCommand({ QueueName: q }));
        await sqs.send(new SetQueueAttributesCommand({ QueueUrl: url.QueueUrl, Attributes: { VisibilityTimeout: "600" } }));
        console.log(`Updated visibility timeout: ${q}`);
      }
    }
  }
}

/**
 * Creates dynamo table
 */
async function createDynamoTable() {
  try {
    await dynamodb.send(new DescribeTableCommand({ TableName: "file-parsing-templates" }));
    console.log("DynamoDB table already exists: file-parsing-templates");
  } catch {
    await dynamodb.send(new CreateTableCommand({
      TableName: "file-parsing-templates",
      KeySchema: [
        { AttributeName: "fingerprint", KeyType: "HASH" },
        { AttributeName: "template_id", KeyType: "RANGE" },
      ],
      AttributeDefinitions: [
        { AttributeName: "fingerprint", AttributeType: "S" },
        { AttributeName: "template_id", AttributeType: "S" },
      ],
      BillingMode: "PAY_PER_REQUEST",
    }));
    console.log("Created DynamoDB table: file-parsing-templates");
  }
}

/**
 * Main entry point of the application
 */
async function main() {
  await waitForLocalstack();
  await createBuckets();
  await createQueues();
  await createDynamoTable();
  console.log("Done");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
