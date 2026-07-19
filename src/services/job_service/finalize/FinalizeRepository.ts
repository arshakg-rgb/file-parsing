import { getJob, repositories, type ParseJobRow, type DeadLetterRow } from "../../../shared/DatabaseManager.js";
import { DeadLetter } from "./DeadLetter.js";

export class FinalizeRepository {
  async getJob(jobId: string): Promise<ParseJobRow | null> {
    return getJob(jobId);
  }

  async getDeadLetters(jobId: string): Promise<DeadLetter[]> {
    const rows = await repositories.deadLetters.findByJob(jobId);
    return rows.map((row: DeadLetterRow) => DeadLetter.fromRow(row));
  }

  async updateDeadLetterLineNo(dlqId: string, lineNo: number): Promise<void> {
    await repositories.deadLetters.updateLineNo(dlqId, lineNo);
  }
}
