import { randomUUID } from "crypto";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const ENDPOINT = "http://localhost:4566";
const REGION = "us-east-1";
const CREDENTIALS = { accessKeyId: "test", secretAccessKey: "test" };
const DATA_BUCKET = "datalead-osint";

const s3 = new S3Client({ endpoint: ENDPOINT, region: REGION, credentials: CREDENTIALS, forcePathStyle: true });

async function generateLargeCsv(rows: number): Promise<string> 
{
  const lines: string[] = ["id,name,email,created_at"];
  for (let i = 1; i <= rows; i++) 
{
    lines.push(`${i},User${i},user${i}@example.com,2024-01-${String(i).padStart(2, "0")}`);
  }
  return lines.join("\n");
}

async function uploadLargeFile(rows: number): Promise<string> 
{
  const csv = await generateLargeCsv(rows);
  const key = `test/perf-${rows}rows-${randomUUID()}.csv`;
  await s3.send(new PutObjectCommand({ Bucket: DATA_BUCKET, Key: key, Body: csv }));
  console.log(`Uploaded ${rows} row test file: s3:
  return key;
}

async function testBackfillPerformance(rows: number): Promise<void> 
{
  console.log(`\n=== Testing backfill performance with ${rows} rows ===`);
  const startTime = Date.now();
  
  const s3Key = await uploadLargeFile(rows);
  
  const { _gcsClientgcsClient_gcsClient, readFull, parseGcsUrl } = await import("../shared/gcsUtils.js");
  const [bucket, key] = parseGcsUrl(`s3:
  const source = await readFull(bucket, key);
  
  const offsets: number[] = [];
  for (let i = 0; i < Math.min(rows, 1000); i++) 
{
    const offset = Math.floor(Math.random() * source.length);
    offsets.push(offset);
  }
  offsets.sort((a, b) => a - b);
  
  const lineMap = new Map<number, number>();
  let sourcePos = 0;
  let nextOffsetIndex = 0;
  let newlineCount = 0;
  
  while (sourcePos < source.length && nextOffsetIndex < offsets.length) 
{
    while (nextOffsetIndex < offsets.length && offsets[nextOffsetIndex] <= sourcePos) 
{
      lineMap.set(offsets[nextOffsetIndex], newlineCount + 1);
      nextOffsetIndex++;
    }
    if (source[sourcePos] === 0x0a) 
{
      newlineCount++;
    }
    sourcePos++;
  }
  
  const elapsed = Date.now() - startTime;
  console.log(`Backfill completed in ${elapsed}ms`);
  console.log(`Source file size: ${source.length} bytes`);
  console.log(`Offsets processed: ${offsets.length}`);
  console.log(`Lines found: ${newlineCount}`);
  console.log(`Performance: ${(source.length / (elapsed / 1000) / 1024 / 1024).toFixed(2)} MB/s`);
  
  await s3.send(new PutObjectCommand({ Bucket: DATA_BUCKET, Key: s3Key, Body: "" }));
}

async function runPerformanceTests(): Promise<void> 
{
  console.log("Starting finalize backfill performance tests...");
  
  const testSizes = [1000, 10000, 100000, 1000000];
  
  for (const size of testSizes) 
{
    try 
{
      await testBackfillPerformance(size);
    }
 catch (err) 
{
      console.error(`Performance test failed for ${size} rows:`, err);
    }
  }
  
  console.log("\n=== Performance tests completed ===");
}

runPerformanceTests();
