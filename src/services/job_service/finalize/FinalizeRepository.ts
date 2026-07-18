import { getJob, repositories, type ParseJobRow } from "../../../shared/db.js";
import { DeadLetter } from "./DeadLetter.js";

export class FinalizeRepository {
  async getJob(jobId: string): Promise<ParseJobRow | undefined> {
    return getJob(jobId);
  }

  async getDeadLetters(jobId: string): Promise<DeadLetter[]> {
    const rows = await repositories.deadLetters.findByJob(jobId);
    return rows.map((row) => DeadLetter.fromRow(row as any));
  }

  async updateDeadLetterLineNo(dlqId: string, lineNo: number): Promise<void> {
    await repositories.deadLetters.updateLineNo(dlqId, lineNo);
  }
}
