import { pool } from './src/shared/db.js';
const jobId = '6e74aeca-1cf0-479c-8470-b008a025b4f3';
const result = await pool.query('UPDATE parse_jobs SET status = $1, updated_at = NOW() WHERE job_id = $2 RETURNING job_id, status', ['done', jobId]);
console.log('updated', result.rows[0]);
