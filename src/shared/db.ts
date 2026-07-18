import MySqlManager, {
  ParseJobRow,
  OutputPartRow,
  DeadLetterRow,
  PendingArchiveEntryRow
} from "../config/db/MySqlManager.js";

const dbManager = MySqlManager.getInstance();

export const pool = dbManager.pool;

export type { ParseJobRow, OutputPartRow, DeadLetterRow, PendingArchiveEntryRow };

export async function waitForDb(): Promise<void> 
{
  console.warn("waitForDb is deprecated. Use MySqlManager.getInstance().initialize() instead.");
}

export async function getJob(jobId: string): Promise<ParseJobRow | undefined> 
{
  return dbManager.getJob(jobId);
}

export async function getBatchJobs(batchId: string): Promise<ParseJobRow[]> 
{
  return dbManager.getBatchJobs(batchId);
}

export async function getJobParts(jobId: string): Promise<OutputPartRow[]> 
{
  return dbManager.getJobParts(jobId);
}

export async function createPendingArchiveEntry(
  jobId: string,
  entryName: string,
  entrySize: number
): Promise<void> 
{
  return dbManager.createPendingArchiveEntry(jobId, entryName, entrySize);
}

export async function markPendingEntryProcessing(
  jobId: string,
  entryName: string
): Promise<void> 
{
  return dbManager.markPendingEntryProcessing(jobId, entryName);
}

export async function markPendingEntryCompleted(
  jobId: string,
  entryName: string
): Promise<void> 
{
  return dbManager.markPendingEntryCompleted(jobId, entryName);
}

export async function markPendingEntryFailed(
  jobId: string,
  entryName: string,
  error: string
): Promise<void> 
{
  return dbManager.markPendingEntryFailed(jobId, entryName, error);
}

export async function getPendingEntries(jobId: string): Promise<PendingArchiveEntryRow[]> 
{
  return dbManager.getPendingEntries(jobId);
}

export async function getPendingEntryCount(jobId: string): Promise<{ pending: number; completed: number; failed: number }> 
{
  return dbManager.getPendingEntryCount(jobId);
}

export async function getPendingEntryTotalSize(jobId: string): Promise<number> 
{
  return dbManager.getPendingEntryTotalSize(jobId);
}

export async function createTables(): Promise<void> 
{
  return dbManager.createTables();
}

export { default as DatabaseManager } from "../config/db/MySqlManager.js";
