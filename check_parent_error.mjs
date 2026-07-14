import { pool } from './src/shared/db.js';
const result = await pool.query('SELECT job_id, status, error, counts FROM parse_jobs WHERE job_id = $1', ['b25bd0ec-41c0-45dc-9e7a-5792759ac869']);
console.log(JSON.stringify(result.rows[0], null, 2));
