import { getJob, pool, type ParseJobRow, type DeadLetterRow } from "../../../shared/db.js";
import { DeadLetter } from "./DeadLetter.js";

export class FinalizeRepository {
  async getJob(jobId: string): Promise<ParseJobRow | undefined> {
    return getJob(jobId);
  }

  async getDeadLetters(jobId: string): Promise<DeadLetter[]> {
    const result = await pool.query<DeadLetterRow>(
      "SELECT dlq_id, job_id, byte_offset, line_no FROM dead_letters WHERE job_id = $1",
      [jobId]
    );
    return result.rows.map(DeadLetter.fromRow);
  }

  async updateDeadLetterLineNo(dlqId: string, lineNo: number): Promise<void> {
    await pool.query(
      "UPDATE dead_letters SET line_no = $1, updated_at = NOW() WHERE dlq_id = $2",
      [lineNo, dlqId]
    );
  }
}
