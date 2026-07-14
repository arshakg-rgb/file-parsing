import { readFile } from "fs/promises";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const ENDPOINT = "http://localhost:4566";
const REGION = "us-east-1";
const BUCKET = "file-parsing-pipeline-data";
const KEY = "test/sample.csv";

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
