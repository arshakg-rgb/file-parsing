import { getJob, repositories, type ParseJobRow, type DeadLetterRow } from "@shared/DatabaseManager.js";
import { DeadLetter } from "./DeadLetter.js";

/**
 * FinalizeRepository is responsible for finalize repository operations.
 */
export class FinalizeRepository {
    /**
   * Gets job
   * @param jobId - The job identifier
   * @returns A promise that resolves to the result
   */
  async getJob(jobId: string): Promise<ParseJobRow | null> {
    return getJob(jobId);
  }

    /**
   * Gets dead letters
   * @param jobId - The job identifier
   * @returns A promise that resolves to the list
   */
  async getDeadLetters(jobId: string): Promise<DeadLetter[]> {
    const rows = await repositories.deadLetters.findByJob(jobId);
    return rows.map((row: DeadLetterRow) => DeadLetter.fromRow(row));
  }

    /**
   * Updates dead letter line no
   * @param dlqId - The dlq id
   * @param lineNo - The line no
   */
  async updateDeadLetterLineNo(dlqId: string, lineNo: number): Promise<void> {
    await repositories.deadLetters.updateLineNo(dlqId, lineNo);
  }
}
