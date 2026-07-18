import MySqlManager from "../config/db/MySqlManager.js";
import { createLogger } from "../utils/logger/logger.js";

const logger = createLogger("purge-data");

export async function purgeDatabase(): Promise<void> {
  logger.info("Starting database purge...");

  const dbManager = MySqlManager.getInstance();
  await dbManager.initialize();
  const { ParseJob, OutputPart, RubbishLog, DeadLetter, PendingArchiveEntry, ParsedRecord } = dbManager.models;

  // The schema does not define foreign keys, so purge in dependency order
  const deletedParseJobs = await ParseJob.destroy({ where: {}, truncate: false });
  logger.info(`Deleted ${deletedParseJobs} jobs from parse_jobs`);

  const [outputCount, rubbishCount, dlqCount, pendingCount, parsedCount] = await Promise.all([
    OutputPart.count(),
    RubbishLog.count(),
    DeadLetter.count(),
    PendingArchiveEntry.count(),
    ParsedRecord.count(),
  ]);

  logger.info(
    `Remaining records - output_parts: ${outputCount}, rubbish_log: ${rubbishCount}, dead_letters: ${dlqCount}, pending_archive_entries: ${pendingCount}, parsed_records: ${parsedCount}`
  );

  logger.info("Database purge complete");
}

async function main() {
  try {
    await purgeDatabase();
  } finally {
    await MySqlManager.getInstance().shutdown();
  }
}

main().catch(console.error);
