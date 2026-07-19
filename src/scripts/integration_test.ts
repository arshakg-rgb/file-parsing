import { randomUUID } from "crypto";
import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { SQSClient, SendMessageCommand, ReceiveMessageCommand, DeleteMessageCommand, GetQueueUrlCommand } from "@aws-sdk/client-sqs";
import { setTimeout as sleep } from "timers/promises";

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
 * The d a t a_ b u c k e t
 */
const DATA_BUCKET = "datalead-osint";
/**
 * The i n g e s t_ q u e u e
 */
const INGEST_QUEUE = "fpp-ingest.fifo";
/**
 * The r e p o r t_ q u e u e
 */
const REPORT_QUEUE = "fpp-report.fifo";

/**
 * The s a m p l e_ c s v
 */
const SAMPLE_CSV = `id,name,email,created_at
1,John Doe,john@example.com,2024-01-15
2,Jane Smith,jane@example.com,2024-01-16
3,Bob Johnson,bob@example.com,2024-01-17
4,Alice Williams,alice@example.com,2024-01-18
5,Charlie Brown,charlie@example.com,2024-01-19`;

/**
 * Gets queue url
 * @param queueName - The queue name
 * @returns A promise that resolves to the result
 */
async function getQueueUrl(queueName: string): Promise<string> {
  const resp = await sqs.send(new GetQueueUrlCommand({ QueueName: queueName }));
  return resp.QueueUrl || "";
}

/**
 * Uploads test file
 * @returns A promise that resolves to the result
 */
async function uploadTestFile(): Promise<string> {
  const key = `test/integration-${randomUUID()}.csv`;
  await s3.send(new PutObjectCommand({ Bucket: DATA_BUCKET, Key: key, Body: SAMPLE_CSV }));
  console.log(`Uploaded test file: s3://${DATA_BUCKET}/${key}`);
  return key;
}

/**
 * Sends ingest message
 * @param s3Key - The s3 key
 * @returns A promise that resolves to the result
 */
async function sendIngestMessage(s3Key: string): Promise<string> {
  const queueUrl = await getQueueUrl(INGEST_QUEUE);
  const jobId = randomUUID();
  const message = {
    job_id: jobId,
    source_type: "s3",
    source_ref: `s3://${DATA_BUCKET}/${s3Key}`,
    field_spec: ["id", "name", "email", "created_at"],
  };
  await sqs.send(new SendMessageCommand({
    QueueUrl: queueUrl,
    MessageBody: JSON.stringify(message),
    MessageGroupId: jobId,
    MessageDeduplicationId: jobId,
  }));
  console.log(`Sent ingest message for job: ${jobId}`);
  return jobId;
}

/**
 * Waits for for report
 * @param jobId - The job identifier
 * @param timeoutMs - The timeout in milliseconds
 * @returns A promise that resolves to the result
 */
async function waitForReport(jobId: string, timeoutMs = 120000): Promise<unknown> {
  const queueUrl = await getQueueUrl(REPORT_QUEUE);
  const startTime = Date.now();
  
  while (Date.now() - startTime < timeoutMs) {
    const resp = await sqs.send(new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 5,
    }));
    
    for (const msg of resp.Messages || []) {
      const body = JSON.parse(msg.Body!);
      if (body.job_id === jobId) {
        await sqs.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: msg.ReceiptHandle! }));
        return body;
      }
    }
    
    await sleep(2000);
  }
  
  throw new Error(`Timeout waiting for report for job ${jobId}`);
}

/**
 * Verifies output
 * @param jobId - The job identifier
 */
async function verifyOutput(jobId: string): Promise<void> {
  const reportKey = `reports/${jobId}/report.json`;
  try {
    const resp = await s3.send(new GetObjectCommand({ Bucket: DATA_BUCKET, Key: reportKey }));
    const report = JSON.parse(await resp.Body!.transformToString());
    
    console.log("Report generated:", {
      job_id: report.job_id,
      status: report.status,
      parsed: report.counts.parsed,
      failed_total: report.counts.failed_total,
      output_parts: report.output_parts.length,
    });
    
    if (report.counts.parsed !== 5) {
      throw new Error(`Expected 5 parsed rows, got ${report.counts.parsed}`);
    }
    
    if (report.counts.failed_total !== 0) {
      throw new Error(`Expected 0 failed rows, got ${report.counts.failed_total}`);
    }
    
    if (report.output_parts.length === 0) {
      throw new Error("Expected at least one output part");
    }
    
    console.log("Integration test PASSED");
  } catch (err) {
    console.error("Integration test FAILED:", err);
    throw err;
  }
}

/**
 * Runs integration test
 */
async function runIntegrationTest(): Promise<void> {
  console.log("Starting integration test...");
  
  try {
    const s3Key = await uploadTestFile();
    const jobId = await sendIngestMessage(s3Key);
    const report = await waitForReport(jobId);
    await verifyOutput(jobId);
  } catch (err) {
    console.error("Integration test error:", err);
    process.exit(1);
  }
}

runIntegrationTest();
