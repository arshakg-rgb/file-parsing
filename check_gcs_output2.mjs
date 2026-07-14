import { gcsClient } from './src/shared/gcsUtils.ts';
console.log('listing');
const bucket = gcsClient().bucket('datalead-osint');
const [files] = await bucket.getFiles({ prefix: 'outputs/6e74aeca-1cf0-479c-8470-b008a025b4f3/parts' });
console.log('files:', files.length);
for (const f of files.slice(0, 10)) console.log(f.name, f.metadata.size);
