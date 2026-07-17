import Config from "../config/system-config/Config.js";
import ServiceManager from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import MySqlManager from "../config/db/MySqlManager.js";
import { createLogger } from "./logger.js";
import crypto from "crypto";

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
  row_data?: Record<string, any>;
}

class TraceSystemService extends ServiceManager {
  protected static instance: TraceSystemService;
  private logger: any;
  private dbManager: MySqlManager;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate TraceSystemService directly. Use getInstance()");
    }
    super(enforce);
    
    this.logger = createLogger("trace-system");
    this.dbManager = MySqlManager.getInstance();
  }

  public static getInstance(): TraceSystemService {
    if (!ServiceManager.instance) {
      ServiceManager.instance = new TraceSystemService(Enforce);
    }
    return ServiceManager.instance as TraceSystemService;
  }

  public async createTrace(trace: TraceRecord): Promise<void> {
    try {
      await this.dbManager.pool.query(
        `INSERT INTO parsed_records 
         (_job_id, _byte_offset, _byte_length, _record_index, _line_no, _template_id, _template_version, _checksum, _parsed_at, _part_id, fields)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT ("_job_id", "_byte_offset") DO NOTHING`,
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
          JSON.stringify({
            s3_url: trace.s3_url,
            ...trace.row_data
          })
        ]
      );
      
      this.logger.debug("trace_created", { 
        job_id: trace.job_id, 
        byte_offset: trace.byte_offset,
        line_no: trace.line_no 
      });
    } catch (error) {
      this.logger.error("trace_creation_error", { 
        job_id: trace.job_id, 
        byte_offset: trace.byte_offset, 
        error: String(error) 
      });
      throw error;
    }
  }

  public async logRubbishDrop(
    jobId: string,
    byteOffset: number,
    lineNo: number,
    rawBytes: string,
    matchedTemplateId: string
  ): Promise<void> {
    try {
      await this.dbManager.pool.query(
        `INSERT INTO rubbish_log (job_id, byte_offset, line_no, raw_bytes, matched_template_id, logged_at)
         VALUES ($1, $2, $3, $4, $5, NOW())`,
        [jobId, byteOffset, lineNo, rawBytes, matchedTemplateId]
      );
      
      this.logger.debug("rubbish_logged", { 
        job_id: jobId, 
        byte_offset: byteOffset,
        line_no: lineNo,
        template_id: matchedTemplateId 
      });
    } catch (error) {
      this.logger.error("rubbish_log_error", { 
        job_id: jobId, 
        byte_offset: byteOffset, 
        error: String(error) 
      });
      throw error;
    }
  }

  public async getJobTraces(jobId: string): Promise<TraceRecord[]> {
    const result = await this.dbManager.pool.query(
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

  public async getJobRubbishLog(jobId: string): Promise<any[]> {
    const result = await this.dbManager.pool.query(
      `SELECT * FROM rubbish_log WHERE job_id = $1 ORDER BY byte_offset`,
      [jobId]
    );
    
    return result.rows;
  }

  static generateChecksum(line: string): string {
    return crypto.createHash("sha256").update(line).digest("hex");
  }

  public async lineExists(jobId: string, byteOffset: number): Promise<boolean> {
    const result = await this.dbManager.pool.query(
      "SELECT 1 FROM parsed_records WHERE _job_id = $1 AND _byte_offset = $2",
      [jobId, byteOffset]
    );
    
    return result.rows.length > 0;
  }

  public async getLineFateCounts(jobId: string): Promise<{
    parsed: number;
    dropped: number;
    failed: number;
  }> {
    const parsedResult = await this.dbManager.pool.query(
      "SELECT COUNT(*) as count FROM parsed_records WHERE _job_id = $1",
      [jobId]
    );
    
    const droppedResult = await this.dbManager.pool.query(
      "SELECT COUNT(*) as count FROM rubbish_log WHERE job_id = $1",
      [jobId]
    );
    
    const failedResult = await this.dbManager.pool.query(
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

function Enforce(): void {}

export default TraceSystemService;

const traceSystemService = TraceSystemService.getInstance();

export class TraceSystem {
  async createTrace(trace: TraceRecord): Promise<void> {
    return traceSystemService.createTrace(trace);
  }

  async logRubbishDrop(
    jobId: string,
    byteOffset: number,
    lineNo: number,
    rawBytes: string,
    matchedTemplateId: string
  ): Promise<void> {
    return traceSystemService.logRubbishDrop(jobId, byteOffset, lineNo, rawBytes, matchedTemplateId);
  }

  async getJobTraces(jobId: string): Promise<TraceRecord[]> {
    return traceSystemService.getJobTraces(jobId);
  }

  async getJobRubbishLog(jobId: string): Promise<any[]> {
    return traceSystemService.getJobRubbishLog(jobId);
  }

  static generateChecksum(line: string): string {
    return TraceSystemService.generateChecksum(line);
  }

  async lineExists(jobId: string, byteOffset: number): Promise<boolean> {
    return traceSystemService.lineExists(jobId, byteOffset);
  }

  async getLineFateCounts(jobId: string): Promise<{
    parsed: number;
    dropped: number;
    failed: number;
  }> {
    return traceSystemService.getLineFateCounts(jobId);
  }
}
