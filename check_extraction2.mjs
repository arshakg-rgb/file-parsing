import { gcsClient } from './src/shared/gcsUtils.ts';
const bucket = gcsClient().bucket('datalead-osint');
const prefix = 'ingested/b25bd0ec-41c0-45dc-9e7a-5792759ac869/entries/';
const [files] = await bucket.getFiles({ prefix });
console.log('Extracted files:', files.length);
for (const f of files) {
  console.log(f.name, f.metadata.size);
}
