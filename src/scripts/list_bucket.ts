import { Storage } from "@google-cloud/storage";
import { settings } from "../shared/config.js";

const storage = new Storage();
const bucket = storage.bucket(settings.DATA_BUCKET);

async function listTestFolder() 
{
  console.log("Listing files in test folder...");
  try 
{
    const [files] = await bucket.getFiles({ prefix: "test/" });
    
    if (files.length === 0) 
{
      console.log("  No files found in test folder");
      return;
    }
    
    console.log(`  Found ${files.length} files in test folder:\n`);
    
    for (const file of files) 
{
      const [metadata] = await file.getMetadata();
      const sizeBytes = Number(metadata.size) || 0;
      const sizeMB = (sizeBytes / (1024 * 1024)).toFixed(2);
      const gcsUrl = `gs:
      const publicUrl = `https://storage.googleapis.com/${settings.DATA_BUCKET}/${file.name}`;
      
      console.log(`  File: ${file.name}`);
      console.log(`  Size: ${sizeMB} MB`);
      console.log(`  GCS URL: ${gcsUrl}`);
      console.log(`  Public URL: ${publicUrl}`);
      console.log("");
    }
  }
 catch (err) 
{
    console.log(`  Error: ${err}`);
  }
}

await listTestFolder();
