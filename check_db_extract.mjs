import { pool } from './src/shared/db.js';
const parent = 'b25bd0ec-41c0-45dc-9e7a-5792759ac869';
const result = await pool.query('SELECT job_id, batch_id, source_ref, s3_url, size, status, counts, timings FROM parse_jobs WHERE parent_job_id = $1 OR job_id = $1 ORDER BY size', [parent]);
console.log('Rows:', result.rows.length);
for (const r of result.rows) {
  console.log({ job_id: r.job_id, source_ref: r.source_ref, size: r.size, status: r.status, s3_url: r.s3_url ? r.s3_url.split('/').pop() : null });
}
