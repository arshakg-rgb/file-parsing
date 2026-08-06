import { BigQueryManager } from "@config/db/BigQueryManager.js";
import { settings } from "@shared/Settings.js";
import { createLogger } from "@utils/logger/Log.js";

/**
 * Logger instance for the module
 */
const logger = createLogger(module);

/**
 * Performs the purge database operation.
 */
const TABLES = [
  "parse_jobs",
  "output_parts",
  "rubbish_log",
  "dead_letters",
  "pending_archive_entries",
  "parsed_records",
];

export async function purgeDatabase(): Promise<void> {
  logger.info("Starting database purge...");

  const bq = BigQueryManager.getInstance();
  await bq.initialize();

  for (const table of TABLES) {
    const fullTable = `\`${settings.BIGQUERY_PROJECT_ID}.${settings.BIGQUERY_DATASET}.${table}\``;
    const deleted = await bq.execute(`DELETE FROM ${fullTable} WHERE TRUE`);
    logger.info(`Deleted ${deleted} rows from ${table}`);
  }

  logger.info("Database purge complete");
}

/**
 * Main entry point of the application
 */
async function main() {
  await purgeDatabase();
}

main().catch(console.error);
