import { gcsClient } from './src/shared/gcsUtils.ts';
const bucket = gcsClient().bucket('datalead-osint');
const file = bucket.file('ingested/b25bd0ec-41c0-45dc-9e7a-5792759ac869/entries/cfd97adb-3288-4735-affe-502398a5e5e0/_ПЕНСИОННЫЙ_ФОНД_РФ_-_type2.csv');
console.log('starting download');
const [data] = await file.download();
console.log('downloaded', data.length);
