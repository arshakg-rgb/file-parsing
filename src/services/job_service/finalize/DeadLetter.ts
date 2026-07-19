import type { DeadLetterRow } from "@shared/DatabaseManager.js";

/**
 * DeadLetter is responsible for dead letter operations.
 */
export class DeadLetter {
    /**
   * Constructs a new DeadLetter instance.
   * @param dlqId - The dlq id
   * @param jobId - The job identifier
   * @param byteOffset - The byte offset
   * @param lineNo - The line no
   */
  constructor(
    public readonly dlqId: string,
    public readonly jobId: string,
    public readonly byteOffset: number,
    public lineNo: number | null
  ) {}

    /**
   * Performs the from row operation.
   * @param row - The row
   * @returns The dead letter result
   */
  static fromRow(row: DeadLetterRow): DeadLetter {
    return new DeadLetter(row.dlq_id, row.job_id, Number(row.byte_offset), row.line_no ?? null);
  }
}
