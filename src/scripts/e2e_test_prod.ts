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

/**
 * Creates job from g c s
 * @param gcsUrl - The gcs url
 * @param fieldSpec - The field spec
 * @returns A promise that resolves to the result
 */
async function createJobFromGCS(gcsUrl: string, fieldSpec: string[]): Promise<{ job_id: string }> {
  const response = await fetch(`${JOB_SERVICE_URL}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ 
      source_type: "s3", 
      source_ref: gcsUrl,
      field_spec: JSON.stringify(fieldSpec) 
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to create job: ${response.status} ${response.statusText} - ${errorText}`);
  }

  const data = await response.json() as { job_id: string };
  console.log(`Job created: ${data.job_id}`);
  return data;
}

/**
 * Uploads file
 * @param presignedUrl - The presigned url
 * @param content - The content
 * @returns A promise that resolves to the result
 */
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

interface JobStatusResponse {
  status: string;
  error?: string;
  counts?: { parsed?: number };
}

/**
 * Gets job status
 * @param jobId - The job identifier
 * @returns A promise that resolves to the result
 */
async function getJobStatus(jobId: string): Promise<JobStatusResponse> {
  const response = await fetch(`${JOB_SERVICE_URL}/${jobId}`);

  if (!response.ok) {
    throw new Error(`Failed to get job status: ${response.status} ${response.statusText}`);
  }

  return response.json() as Promise<JobStatusResponse>;
}

/**
 * Main entry point of the application
 */
async function main() {
  console.log("Starting production end-to-end test...");

  try {
    // GCS URL for the large file
    const GCS_URL = "https://storage.googleapis.com/datalead-osint/archive/0fd825b9-824c-4d4e-b261-3182475c48c2/CSV samples/first-PassengerDetails.csv";
    
    // Create job from GCS
    console.log("Creating job from GCS...");
    const { job_id } = await createJobFromGCS(GCS_URL, ["field1", "field2", "field3"]);
    console.log(`Job created: ${job_id}`);

    // Poll job status
    console.log("Polling job status (every 5s)...");
    let attempts = 0;
    const maxAttempts = 120; // 10 minutes max for large file

    while (attempts < maxAttempts) {
      attempts++;
      await new Promise(r => setTimeout(r, 5000));

      const job = await getJobStatus(job_id);
      console.log(`[${attempts}] Status: ${job.status}, Parsed: ${job.counts?.parsed || 0}, Error: ${job.error || "none"}`);

      if (job.status === "done") {
        console.log("✅ Job completed successfully!");
        process.exit(0);
      }

      if (job.status === "failed") {
        console.log("❌ Job failed!");
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
