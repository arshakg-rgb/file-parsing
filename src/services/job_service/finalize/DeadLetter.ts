import type { DeadLetterRow } from "../../../shared/DatabaseManager.js";

export class DeadLetter {
  constructor(
    public readonly dlqId: string,
    public readonly jobId: string,
    public readonly byteOffset: number,
    public lineNo: number | null
  ) {}

  static fromRow(row: DeadLetterRow): DeadLetter {
    return new DeadLetter(row.dlq_id, row.job_id, Number(row.byte_offset), row.line_no ?? null);
  }
}
