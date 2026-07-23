import { Storage } from "@google-cloud/storage";

const storage = new Storage();

async function downloadFile() {
  const bucket = storage.bucket("datalead-osint");
  const file = bucket.file("archive/0fd825b9-824c-4d4e-b261-3182475c48c2/CSV samples/twitter_users_000.csv");
  const [exists] = await file.exists();
  
  if (!exists) {
    console.error("File does not exist");
    return;
  }
  
  await file.download({ destination: "/tmp/twitter_users_000.csv" });
  console.log("Downloaded to /tmp/twitter_users_000.csv");
}

downloadFile().catch(console.error);
