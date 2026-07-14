import { gcsClient } from './src/shared/gcsUtils.ts';
import { RARExtractor } from 'unrar-async';
import fs from 'fs/promises';
import os from 'os';
import path from 'path';
const bucket = gcsClient().bucket('datalead-osint');
const key = 'uploads/b25bd0ec-41c0-45dc-9e7a-5792759ac869/source';
const [data] = await bucket.file(key).download();
const tmp = path.join(os.tmpdir(), 'source.rar');
await fs.writeFile(tmp, data);
const extractor = await RARExtractor.fromFile(tmp);
const result = await extractor.extract();
console.log('Files in archive:');
for await (const { fileHeader } of result.files) {
  console.log(fileHeader.name, 'unpSize:', fileHeader.unpSize, 'compressedSize:', fileHeader.packSize);
}
await extractor.close();
await fs.unlink(tmp);
