import { readFile } from "fs/promises";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

/**
 * The e n d p o i n t
 */
const ENDPOINT = "http://localhost:4566";
/**
 * The r e g i o n
 */
const REGION = "us-east-1";
/**
 * The b u c k e t
 */
const BUCKET = "file-parsing-pipeline-data";
/**
 * The k e y
 */
const KEY = "test/sample.csv";

/**
 * Main entry point of the application
 */
async function main() {
  const filePath = process.argv[2] || "test/sample.csv";
  const body = await readFile(filePath);
  const s3 = new S3Client({
    endpoint: ENDPOINT,
    region: REGION,
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
    forcePathStyle: true,
  });
  await s3.send(new PutObjectCommand({ Bucket: BUCKET, Key: KEY, Body: body }));
  console.log(`Uploaded s3://${BUCKET}/${KEY}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
