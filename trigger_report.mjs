import { sendRaw } from './src/shared/queueUtils.js';
import { settings } from './src/shared/config.js';
import { pool } from './src/shared/db.js';
const jobId = process.argv[2] || '6e74aeca-1cf0-479c-8470-b008a025b4f3';
const result = await pool.query('SELECT status, counts, output_paths, timings FROM parse_jobs WHERE job_id = $1', [jobId]);
const row = result.rows[0];
const timings = row.timings || {};
await sendRaw(settings.REPORT_QUEUE_URL, {
  job_id: jobId,
  status: row.status,
  counts: row.counts,
  output_paths: row.output_paths,
  rubbish_log_path: timings._rubbish_log_path ?? null,
  dlq_count: timings._dlq_count ?? 0,
});
console.log('Report message sent for', jobId);
