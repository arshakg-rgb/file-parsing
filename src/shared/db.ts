// Re-export from MySqlManager for backward compatibility
import MySqlManager from "../config/db/MySqlManager.js";

const dbManager = MySqlManager.getInstance();
const repos = dbManager.repositories;

// Re-export pool for backward compatibility
export const pool = dbManager.pool;

export const models = dbManager.models;
export const repositories = repos;

// Legacy row shape aliases for minimal downstream churn
export type ParseJobRow = any;
export type OutputPartRow = any;
export type DeadLetterRow = any;
export type PendingArchiveEntryRow = any;

// Re-export functions as wrappers around the repository layer
export async function waitForDb(): Promise<void> {
  await dbManager.initialize();
}

export async function getJob(jobId: string): Promise<ParseJobRow | undefined> {
  return repos.jobs.findById(jobId);
}

export async function getBatchJobs(batchId: string): Promise<ParseJobRow[]> {
  return repos.jobs.findByBatchId(batchId);
}

export async function getJobParts(jobId: string): Promise<OutputPartRow[]> {
  return repos.outputParts.findByJob(jobId);
}

export async function createPendingArchiveEntry(
  jobId: string,
  entryName: string,
  entrySize: number
): Promise<void> {
  await repos.pendingArchiveEntries.create({ id: crypto.randomUUID(), job_id: jobId, entry_name: entryName, entry_size: entrySize, status: "pending" });
}

export async function markPendingEntryProcessing(
  jobId: string,
  entryName: string
): Promise<void> {
  const entry = await dbManager.models.PendingArchiveEntry.findOne({ where: { job_id: jobId, entry_name: entryName } });
  if (entry) await repos.pendingArchiveEntries.markStatus(entry.id, "processing");
}

export async function markPendingEntryCompleted(
  jobId: string,
  entryName: string
): Promise<void> {
  const entry = await dbManager.models.PendingArchiveEntry.findOne({ where: { job_id: jobId, entry_name: entryName } });
  if (entry) await repos.pendingArchiveEntries.markStatus(entry.id, "completed");
}

export async function markPendingEntryFailed(
  jobId: string,
  entryName: string,
  error: string
): Promise<void> {
  const entry = await dbManager.models.PendingArchiveEntry.findOne({ where: { job_id: jobId, entry_name: entryName } });
  if (entry) await repos.pendingArchiveEntries.markStatus(entry.id, "failed", error);
}

export async function getPendingEntries(jobId: string): Promise<PendingArchiveEntryRow[]> {
  return repos.pendingArchiveEntries.findByJob(jobId);
}

export async function getPendingEntryCount(jobId: string): Promise<{ pending: number; completed: number; failed: number }> {
  return repos.pendingArchiveEntries.getCountByJob(jobId);
}

export async function getPendingEntryTotalSize(jobId: string): Promise<number> {
  return repos.pendingArchiveEntries.getTotalSize(jobId);
}

export async function createTables(): Promise<void> {
  await dbManager.sequelize.sync({ force: false });
}

// Export the manager class for direct use
export { default as DatabaseManager } from "../config/db/MySqlManager.js";
