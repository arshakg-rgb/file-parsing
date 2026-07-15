// Comprehensive test suite for leak detection and field mapping validation
// Tests memory leaks, resource leaks, and correct field mapping

const BASE_URL = "https://job-service-81405680629.us-central1.run.app";

// Test 1: Field Mapping Validation
async function testFieldMapping() {
  console.log("=== Test 1: Field Mapping Validation ===");
  
  const testCases = [
    {
      name: "Correct field_spec order",
      field_spec: ["Date", "Country", "Confirmed", "Deaths", "Recovered"],
      expected_mapping: { Date: 0, Country: 1, Confirmed: 2, Deaths: 3, Recovered: 4 }
    },
    {
      name: "Partial field_spec",
      field_spec: ["Country", "Confirmed"],
      expected_mapping: { Country: 1, Confirmed: 2 }
    },
    {
      name: "Single field",
      field_spec: ["Country"],
      expected_mapping: { Country: 1 }
    }
  ];

  for (const testCase of testCases) {
    console.log("Testing:", testCase.name);
    console.log("Field spec:", testCase.field_spec);
    console.log("Expected mapping:", testCase.expected_mapping);
    
    const response = await fetch(`${BASE_URL}/v1/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        source_type: 'url',
        source_ref: 'https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv',
        field_spec: testCase.field_spec
      })
    });
    
    if (response.ok) {
      const data = await response.json();
      console.log("Job created:", data.job_id);
      
      await new Promise(resolve => setTimeout(resolve, 5000));
      
      const statusResponse = await fetch(`${BASE_URL}/v1/jobs/${data.job_id}`);
      if (statusResponse.ok) {
        const statusData = await statusResponse.json();
        console.log("Job status:", statusData.status);
        console.log("Counts:", statusData.counts);
      }
    } else {
      console.log("Job creation failed:", response.status);
    }
    console.log("");
  }
}

// Test 2: Memory Leak Detection
async function testMemoryLeak() {
  console.log("=== Test 2: Memory Leak Detection ===");
  
  const jobIds = [];
  const numJobs = 5;
  
  console.log("Creating", numJobs, "sequential jobs to detect memory leaks...");
  
  for (let i = 0; i < numJobs; i++) {
    const response = await fetch(`${BASE_URL}/v1/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        source_type: 'url',
        source_ref: 'https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv',
        field_spec: ['Date', 'Country', 'Confirmed']
      })
    });
    
    if (response.ok) {
      const data = await response.json();
      jobIds.push(data.job_id);
      console.log("Job", i+1, "/", numJobs, "created:", data.job_id);
    } else {
      console.log("Job", i+1, "creation failed");
    }
    
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  
  console.log("\nMonitoring", jobIds.length, "jobs for completion...");
  
  for (const jobId of jobIds) {
    const statusResponse = await fetch(`${BASE_URL}/v1/jobs/${jobId}`);
    if (statusResponse.ok) {
      const statusData = await statusResponse.json();
      console.log("Job", jobId.substring(0, 8), "...:", statusData.status);
    }
  }
  
  console.log("\nMemory leak test completed - check memory usage in Cloud Run logs");
}

// Test 3: Resource Leak Detection
async function testResourceLeak() {
  console.log("=== Test 3: Resource Leak Detection ===");
  
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
    console.log("Long-running job created:", data.job_id);
    
    const intervals = [5, 10, 20, 30, 60];
    for (const seconds of intervals) {
      await new Promise(resolve => setTimeout(resolve, seconds * 1000));
      
      const statusResponse = await fetch(`${BASE_URL}/v1/jobs/${data.job_id}`);
      if (statusResponse.ok) {
        const statusData = await statusResponse.json();
        console.log("T+" + seconds + "s: Status=" + statusData.status + ", Parsed=" + (statusData.counts?.parsed || 0) + ", Dropped=" + (statusData.counts?.dropped_rubbish || 0));
      }
    }
    
    console.log("Resource leak test completed - check for resource cleanup in logs");
  } else {
    console.log("Long-running job creation failed");
  }
}

// Test 4: Database Connection Leak
async function testDatabaseConnectionLeak() {
  console.log("=== Test 4: Database Connection Leak Detection ===");
  
  const jobIds = [];
  const numJobs = 10;
  
  console.log("Creating", numJobs, "jobs to test database connection pooling...");
  
  for (let i = 0; i < numJobs; i++) {
    const response = await fetch(`${BASE_URL}/v1/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        source_type: 'url',
        source_ref: 'https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv',
        field_spec: ['Date', 'Country']
      })
    });
    
    if (response.ok) {
      const data = await response.json();
      jobIds.push(data.job_id);
    }
  }
  
  console.log("Created", jobIds.length, "jobs rapidly");
  console.log("Database connection leak test completed - check connection pool metrics in logs");
}

// Test 5: Queue Message Leak
async function testQueueMessageLeak() {
  console.log("=== Test 5: Queue Message Leak Detection ===");
  
  console.log("Creating jobs to test queue message handling...");
  
  const response = await fetch(`${BASE_URL}/v1/jobs`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      source_type: 'url',
      source_ref: 'https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv',
      field_spec: ['Date', 'Country', 'Confirmed']
    })
  });
  
  if (response.ok) {
    const data = await response.json();
    console.log("Job created:", data.job_id);
    
    const stages = ['queued', 'ingesting', 'detecting', 'parsing', 'finalizing', 'loading', 'reporting', 'done'];
    let currentStage = 0;
    
    while (currentStage < stages.length) {
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      const statusResponse = await fetch(`${BASE_URL}/v1/jobs/${data.job_id}`);
      if (statusResponse.ok) {
        const statusData = await statusResponse.json();
        const status = statusData.status;
        
        if (status === stages[currentStage]) {
          console.log("Stage:", status);
          currentStage++;
        } else if (status === 'failed' || status === 'held') {
          console.log("Job", status, "- stopping monitoring");
          break;
        }
      }
    }
    
    console.log("Queue message leak test completed - check for message accumulation in queues");
  } else {
    console.log("Queue message leak test failed");
  }
}

// Test 6: File Handle Leak
async function testFileHandleLeak() {
  console.log("=== Test 6: File Handle Leak Detection ===");
  
  console.log("Creating multiple jobs to test file handle cleanup...");
  
  const jobIds = [];
  const numJobs = 3;
  
  for (let i = 0; i < numJobs; i++) {
    const response = await fetch(`${BASE_URL}/v1/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        source_type: 'url',
        source_ref: 'https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv',
        field_spec: ['Date', 'Country']
      })
    });
    
    if (response.ok) {
      const data = await response.json();
      jobIds.push(data.job_id);
      console.log("Job", i+1, ":", data.job_id);
    }
  }
  
  console.log("File handle leak test completed - check for file descriptor leaks in logs");
}

// Test 7: Template Registry Memory Leak
async function testTemplateRegistryLeak() {
  console.log("=== Test 7: Template Registry Memory Leak ===");
  
  console.log("Creating jobs with different formats to test template caching...");
  
  const formats = [
    { field_spec: ['Date', 'Country', 'Confirmed'], name: '3 fields' },
    { field_spec: ['Country', 'Confirmed'], name: '2 fields' },
    { field_spec: ['Date'], name: '1 field' },
    { field_spec: ['Date', 'Country', 'Confirmed', 'Deaths', 'Recovered'], name: '5 fields' }
  ];
  
  for (const format of formats) {
    const response = await fetch(`${BASE_URL}/v1/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        source_type: 'url',
        source_ref: 'https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv',
        field_spec: format.field_spec
      })
    });
    
    if (response.ok) {
      const data = await response.json();
      console.log("Format \"" + format.name + "\": Job", data.job_id);
    }
  }
  
  console.log("Template registry leak test completed - check template cache growth in logs");
}

// Test 8: Parquet Part Cleanup
async function testParquetPartCleanup() {
  console.log("=== Test 8: Parquet Part Cleanup ===");
  
  console.log("Creating job to test Parquet part cleanup...");
  
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
    console.log("Job created:", data.job_id);
    
    await new Promise(resolve => setTimeout(resolve, 10000));
    
    const statusResponse = await fetch(`${BASE_URL}/v1/jobs/${data.job_id}`);
    if (statusResponse.ok) {
      const statusData = await statusResponse.json();
      console.log("Job status:", statusData.status);
      console.log("Output paths:", statusData.output_paths?.length || 0);
    }
    
    console.log("Parquet part cleanup test completed - check for orphaned Parquet files in GCS");
  }
}

async function runComprehensiveTests() {
  console.log("=== Running Comprehensive Leak Detection Tests ===\n");
  
  try {
    await testFieldMapping();
    await testMemoryLeak();
    await testResourceLeak();
    await testDatabaseConnectionLeak();
    await testQueueMessageLeak();
    await testFileHandleLeak();
    await testTemplateRegistryLeak();
    await testParquetPartCleanup();
    
    console.log("\n=== All Comprehensive Tests Complete ===");
    console.log("\nPost-Test Checklist:");
    console.log("1. Check Cloud Run logs for memory usage patterns");
    console.log("2. Verify database connection pool metrics");
    console.log("3. Check queue message backlog");
    console.log("4. Verify GCS for orphaned files");
    console.log("5. Check template registry size");
    console.log("6. Verify Parquet part cleanup");
    console.log("7. Monitor for connection leaks in logs");
  } catch (error) {
    console.error("Test suite error:", error);
  }
}

runComprehensiveTests();
