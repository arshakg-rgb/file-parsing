#!/usr/bin/env tsx
/**
 * Production end-to-end test for the file parsing pipeline
 * Tests the full pipeline flow: queued → ingesting → detecting → parsing → loading → reporting → done
 *
 * Usage: npx tsx src/scripts/e2e_test_prod.ts
 */

const JOB_SERVICE_URL = "https://job-service-81405680629.us-central1.run.app/v1/jobs";

// Test CSV content
const TEST_CSV = `email,name,surname,phone
john.doe@example.com,John,Doe,555-1234
jane.smith@example.com,Jane,Smith,555-5678`;

async function createJob(fieldSpec: string[]): Promise<{ job_id: string; presigned_put_url: string }> {
  const response = await fetch(`${JOB_SERVICE_URL}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ source_type: "upload", field_spec: JSON.stringify(fieldSpec) }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to create job: ${response.status} ${response.statusText} - ${errorText}`);
  }

  const data = await response.json() as { job_id: string; presigned_put_url: string };
  console.log(`Presigned URL: ${data.presigned_put_url}`);
  return data;
}

async function uploadFile(presignedUrl: string, content: string): Promise<number> {
  const response = await fetch(presignedUrl, {
    method: "PUT",
    headers: { "Content-Type": "application/octet-stream" },
    body: content,
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to upload file: ${response.status} ${response.statusText} - ${errorText}`);
  }

  return response.status;
}

async function getJobStatus(jobId: string): Promise<any> {
  const response = await fetch(`${JOB_SERVICE_URL}/${jobId}`);

  if (!response.ok) {
    throw new Error(`Failed to get job status: ${response.status} ${response.statusText}`);
  }

  return response.json();
}

async function main() {
  console.log("Starting production end-to-end test...");

  try {
    // Create job
    console.log("Creating job...");
    const { job_id, presigned_put_url } = await createJob(["email", "name", "surname", "phone"]);
    console.log(`Job created: ${job_id}`);

    // Upload file
    console.log("Uploading test file...");
    const uploadStatus = await uploadFile(presigned_put_url, TEST_CSV);
    console.log(`File uploaded: HTTP ${uploadStatus}`);

    // Poll job status
    console.log("Polling job status (every 5s)...");
    let attempts = 0;
    const maxAttempts = 60; // 5 minutes max

    while (attempts < maxAttempts) {
      attempts++;
      await new Promise(r => setTimeout(r, 5000));

      const job = await getJobStatus(job_id);
      console.log(`[${attempts}] Status: ${job.status}, Parsed: ${job.counts?.parsed || 0}, Error: ${job.error || 'none'}`);

      if (job.status === "done") {
        console.log("✅ Job completed successfully!");
        process.exit(0);
      }

      if (job.status === "failed") {
        console.log(`❌ Job failed!`);
        console.log(`Error: ${job.error}`);
        process.exit(1);
      }
    }

    console.log("❌ Test timed out");
    process.exit(1);

  } catch (error) {
    console.error("❌ Test failed:", error);
    process.exit(1);
  }
}

main();
