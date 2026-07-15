import { pool } from "./db.js";
import { createLogger } from "./logger.js";
import crypto from "crypto";

const logger = createLogger("trace-system");

export interface TraceRecord {
  s3_url: string;
  byte_offset: number;
  byte_length: number;
  record_index: number;
  line_no: number;
  job_id: string;
  part_id: string;
  template_id: string;
  template_version: number;
  checksum: string;
  parsed_at: Date;
}

export class TraceSystem {
  /**
   * Create atomic trace record for a parsed line
   */
  async createTrace(trace: TraceRecord): Promise<void> {
    try {
      await pool.query(
        `INSERT INTO parsed_records 
         (_job_id, _byte_offset, _byte_length, _record_index, _line_no, _template_id, _template_version, _checksum, _parsed_at, _part_id, fields)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
        [
          trace.job_id,
          trace.byte_offset,
          trace.byte_length,
          trace.record_index,
          trace.line_no,
          trace.template_id,
          trace.template_version,
          trace.checksum,
          trace.parsed_at,
          trace.part_id,
          JSON.stringify({ s3_url: trace.s3_url }) // Store metadata in fields
        ]
      );
      
      logger.debug("trace_created", { 
        job_id: trace.job_id, 
        byte_offset: trace.byte_offset,
        line_no: trace.line_no 
      });
    } catch (error) {
      logger.error("trace_creation_error", { 
        job_id: trace.job_id, 
        byte_offset: trace.byte_offset, 
        error: String(error) 
      });
      throw error;
    }
  }

  /**
   * Create trace for dropped rubbish line
   */
  async logRubbishDrop(
    jobId: string,
    byteOffset: number,
    lineNo: number,
    rawBytes: string,
    matchedTemplateId: string
  ): Promise<void> {
    try {
      await pool.query(
        `INSERT INTO rubbish_log (job_id, byte_offset, line_no, raw_bytes, matched_template_id)
         VALUES ($1, $2, $3, $4, $5)`,
        [jobId, byteOffset, lineNo, rawBytes, matchedTemplateId]
      );
      
      logger.debug("rubbish_logged", { 
        job_id: jobId, 
        byte_offset: byteOffset,
        line_no: lineNo,
        template_id: matchedTemplateId 
      });
    } catch (error) {
      logger.error("rubbish_log_error", { 
        job_id: jobId, 
        byte_offset: byteOffset, 
        error: String(error) 
      });
      throw error;
    }
  }

  /**
   * Get trace records for a job
   */
  async getJobTraces(jobId: string): Promise<TraceRecord[]> {
    const result = await pool.query(
      `SELECT * FROM parsed_records WHERE _job_id = $1 ORDER BY _byte_offset`,
      [jobId]
    );
    
    return result.rows.map(row => ({
      s3_url: JSON.parse(row.fields).s3_url,
      byte_offset: row._byte_offset,
      byte_length: row._byte_length,
      record_index: row._record_index,
      line_no: row._line_no,
      job_id: row._job_id,
      part_id: row._part_id,
      template_id: row._template_id,
      template_version: row._template_version,
      checksum: row._checksum,
      parsed_at: row._parsed_at,
    }));
  }

  /**
   * Get rubbish log for a job
   */
  async getJobRubbishLog(jobId: string): Promise<any[]> {
    const result = await pool.query(
      `SELECT * FROM rubbish_log WHERE job_id = $1 ORDER BY byte_offset`,
      [jobId]
    );
    
    return result.rows;
  }

  /**
   * Generate checksum for line
   */
  static generateChecksum(line: string): string {
    return crypto.createHash("sha256").update(line).digest("hex");
  }

  /**
   * Verify line identity (job_id, byte_offset) for idempotency
   */
  async lineExists(jobId: string, byteOffset: number): Promise<boolean> {
    const result = await pool.query(
      "SELECT 1 FROM parsed_records WHERE _job_id = $1 AND _byte_offset = $2",
      [jobId, byteOffset]
    );
    
    return result.rows.length > 0;
  }

  /**
   * Get line count by fate for a job
   */
  async getLineFateCounts(jobId: string): Promise<{
    parsed: number;
    dropped: number;
    failed: number;
  }> {
    const parsedResult = await pool.query(
      "SELECT COUNT(*) as count FROM parsed_records WHERE _job_id = $1",
      [jobId]
    );
    
    const droppedResult = await pool.query(
      "SELECT COUNT(*) as count FROM rubbish_log WHERE job_id = $1",
      [jobId]
    );
    
    const failedResult = await pool.query(
      "SELECT COUNT(*) as count FROM dead_letters WHERE job_id = $1",
      [jobId]
    );
    
    return {
      parsed: parseInt(parsedResult.rows[0].count),
      dropped: parseInt(droppedResult.rows[0].count),
      failed: parseInt(failedResult.rows[0].count),
    };
  }
}
