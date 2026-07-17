import { Pool } from 'pg';
import { settings } from '../shared/config.js';
import { createLogger } from '../shared/logger.js';

const logger = createLogger('purge-data');

export async function purgeDatabase(pool: Pool): Promise<void> {
  logger.info('Starting database purge...');
  
  // Delete all jobs - this will cascade to output_parts, rubbish_log, dead_letters
  const result = await pool.query('DELETE FROM parse_jobs');
  logger.info(`Deleted ${result.rowCount} jobs from parse_jobs`);
  
  // Verify cascade worked
  const outputCount = await pool.query('SELECT COUNT(*) FROM output_parts');
  const rubbishCount = await pool.query('SELECT COUNT(*) FROM rubbish_log');
  const dlqCount = await pool.query('SELECT COUNT(*) FROM dead_letters');
  
  logger.info(`Remaining records - output_parts: ${outputCount.rows[0].count}, rubbish_log: ${rubbishCount.rows[0].count}, dead_letters: ${dlqCount.rows[0].count}`);
  
  logger.info('Database purge complete');
}

async function main() {
  // Parse DATABASE_URL to get connection parameters
  const dbUrl = new URL(settings.DATABASE_URL);
  const pool = new Pool({
    host: dbUrl.hostname,
    port: parseInt(dbUrl.port || '5432'),
    database: dbUrl.pathname.slice(1), // Remove leading slash
    user: dbUrl.username,
    password: dbUrl.password,
  });

  try {
    await purgeDatabase(pool);
  } finally {
    await pool.end();
  }
}

main().catch(console.error);
