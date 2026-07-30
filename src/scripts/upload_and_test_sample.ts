/**
 * Upload a sample LinkedIn-like NDJSON file to GCS and run it through LineClassifierServiceImpl locally.
 *
 * Usage:
 *   npx tsx src/scripts/upload_and_test_sample.ts
 *
 * Environment:
 *   GCS_SAMPLE_BUCKET - destination bucket (defaults to datalead-osint)
 *   GOOGLE_APPLICATION_CREDENTIALS - path to service-account key (required for upload)
 */

import fs from "node:fs/promises";
import path from "node:path";
import { Storage } from "@google-cloud/storage";
import { LineClassifier } from "../services/stream-parser/LineClassifierServiceImpl.js";

const SAMPLE_FILE = path.join(__dirname, "../../samples/test-linkedin.ndjson");
const BUCKET = process.env.GCS_SAMPLE_BUCKET || "datalead-osint";
const GCS_KEY = `samples/test-linkedin-${Date.now()}.ndjson`;

const FIELD_SPEC = ["email", "name", "phone", "address"];

async function classifyLocal() {
  console.log("\n=== Local LineClassifierServiceImpl test ===");
  const raw = await fs.readFile(SAMPLE_FILE, "utf8");
  const lines = raw.split("\n").filter((l) => l.trim() !== "");

  const classifier = new LineClassifier("sample-job", FIELD_SPEC, [], [], null);
  let parsed = 0, uncertain = 0, rubbish = 0;

  for (const line of lines) {
    const result = classifier.classify(line, 0, 0);
    if (result.verdict === "parsed") {
      parsed++;
      console.log("  parsed:", JSON.stringify(result.row));
    } else if (result.verdict === "uncertain") {
      uncertain++;
      console.log("  uncertain");
    } else {
      rubbish++;
      console.log("  rubbish");
    }
  }

  console.log(`\nLocal results: parsed=${parsed}, uncertain=${uncertain}, rubbish=${rubbish}`);
}

async function uploadToGcs() {
  if (!process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    console.log("\n⚠️  GOOGLE_APPLICATION_CREDENTIALS not set; skipping GCS upload.");
    console.log(`   To upload, run: gcloud storage cp ${SAMPLE_FILE} gs://${BUCKET}/${GCS_KEY}`);
    return;
  }

  console.log("\n=== GCS upload ===");
  const storage = new Storage();
  await storage.bucket(BUCKET).upload(SAMPLE_FILE, { destination: GCS_KEY });
  const url = `gs://${BUCKET}/${GCS_KEY}`;
  console.log(`Uploaded to ${url}`);
  console.log(`You can now submit this URL to the ingest endpoint to run the full pipeline.`);
}

(async () => {
  await classifyLocal();
  await uploadToGcs();
})();
