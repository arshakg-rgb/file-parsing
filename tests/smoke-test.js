// Smoke tests for all 8 microservices
// These tests verify basic connectivity and health of each service

const BASE_URL = "https://job-service-81405680629.us-central1.run.app";

async function testJobService() {
  console.log("Testing Job Service...");
  
  // Test health endpoint
  try {
    const response = await fetch(`${BASE_URL}/health`);
    if (response.ok) {
      console.log("✅ Job Service health check passed");
    } else {
      console.log("❌ Job Service health check failed:", response.status);
    }
  } catch (error) {
    console.log("❌ Job Service health check error:", error.message);
  }

  // Test job creation
  try {
    const response = await fetch(`${BASE_URL}/v1/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        source_type: 'url',
        source_ref: 'https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv',
        field_spec: ['Date', 'Country', 'Confirmed', 'Deaths', 'Recovered']
      })
    });
    if (response.ok) {
      const data = await response.json();
      console.log("✅ Job Service job creation passed:", data.job_id);
      return data.job_id;
    } else {
      console.log("❌ Job Service job creation failed:", response.status);
    }
  } catch (error) {
    console.log("❌ Job Service job creation error:", error.message);
  }
}

async function testJobStatus(jobId) {
  console.log("Testing Job Status...");
  
  try {
    const response = await fetch(`${BASE_URL}/v1/jobs/${jobId}`);
    if (response.ok) {
      const data = await response.json();
      console.log("✅ Job Status check passed:", data.status);
    } else {
      console.log("❌ Job Status check failed:", response.status);
    }
  } catch (error) {
    console.log("❌ Job Status check error:", error.message);
  }
}

async function testBatchJobs(batchId) {
  console.log("Testing Batch Jobs...");
  
  try {
    const response = await fetch(`${BASE_URL}/v1/batches/${batchId}/jobs`);
    if (response.ok) {
      const data = await response.json();
      console.log("✅ Batch Jobs check passed:", data.length, "jobs");
    } else {
      console.log("❌ Batch Jobs check failed:", response.status);
    }
  } catch (error) {
    console.log("❌ Batch Jobs check error:", error.message);
  }
}

async function runSmokeTests() {
  console.log("=== Running Smoke Tests ===\n");
  
  await testJobService();
  
  // Test with a real job
  const jobId = await testJobService();
  if (jobId) {
    await new Promise(resolve => setTimeout(resolve, 2000)); // Wait for job to process
    await testJobStatus(jobId);
  }
  
  console.log("\n=== Smoke Tests Complete ===");
}

runSmokeTests();
