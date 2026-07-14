import { listObjects } from './src/shared/gcsUtils.ts';
const prefix = 'outputs/6e74aeca-1cf0-479c-8470-b008a025b4f3/parts';
const files = await listObjects('datalead-osint', prefix);
console.log('Output parts count:', files.length);
for (const [url, size] of files.slice(0, 10)) {
  console.log(url, size);
}
