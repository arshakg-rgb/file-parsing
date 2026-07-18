import { randomUUID } from "crypto";
import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { SQSClient, SendMessageCommand, ReceiveMessageCommand, DeleteMessageCommand, GetQueueUrlCommand } from "@aws-sdk/client-sqs";
import { setTimeout as sleep } from "timers/promises";

const ENDPOINT = "http://localhost:4566";
const REGION = "us-east-1";
const CREDENTIALS = { accessKeyId: "test", secretAccessKey: "test" };

const s3 = new S3Client({ endpoint: ENDPOINT, region: REGION, credentials: CREDENTIALS, forcePathStyle: true });
const sqs = new SQSClient({ endpoint: ENDPOINT, region: REGION, credentials: CREDENTIALS });

const DATA_BUCKET = "datalead-osint";
const INGEST_QUEUE = "fpp-ingest.fifo";
const REPORT_QUEUE = "fpp-report.fifo";

const SAMPLE_CSV = `id,name,email,created_at
1,John Doe,john@example.com,2024-01-15
2,Jane Smith,jane@example.com,2024-01-16
3,Bob Johnson,bob@example.com,2024-01-17
4,Alice Williams,alice@example.com,2024-01-18
5,Charlie Brown,charlie@example.com,2024-01-19`;

async function getQueueUrl(queueName: string): Promise<string> {
  const resp = await sqs.send(new GetQueueUrlCommand({ QueueName: queueName }));
  return resp.QueueUrl || "";
}

async function uploadTestFile(): Promise<string> {
  const key = `test/integration-${randomUUID()}.csv`;
  await s3.send(new PutObjectCommand({ Bucket: DATA_BUCKET, Key: key, Body: SAMPLE_CSV }));
  console.log(`Uploaded test file: s3://${DATA_BUCKET}/${key}`);
  return key;
}

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
