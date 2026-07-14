import { gcsClient } from './src/shared/gcsUtils.ts';
const bucket = gcsClient().bucket('datalead-osint');
const file = bucket.file('ingested/b25bd0ec-41c0-45dc-9e7a-5792759ac869/entries/cfd97adb-3288-4735-affe-502398a5e5e0/_ПЕНСИОННЫЙ_ФОНД_РФ_-_type2.csv');
const [data] = await file.download();
const text = data.toString('utf-8');
const lines = text.split('\n');
for (let i = 599998; i < Math.min(600003, lines.length); i++) {
  console.log(`Line ${i}: length ${lines[i].length} : ${lines[i].slice(0, 300)}`);
}
