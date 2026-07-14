import { pool } from './src/shared/db.js';
const result = await pool.query('SELECT job_id, source_ref, status, counts, size FROM parse_jobs WHERE job_id IN ($1,$2,$3,$4) ORDER BY size', ['b25bd0ec-41c0-45dc-9e7a-5792759ac869','e1c87699-8dc1-4459-ab0f-10cc112922d8','e3e79ba8-e423-4d4f-af31-b1131b4529a9','6e74aeca-1cf0-479c-8470-b008a025b4f3']);
for (const r of result.rows) console.log(JSON.stringify(r));
