import Config from "../config/system-config/Config.js";
import ServiceManager, { Enforce } from "../config/ServiceManager.js";
import { InstantiationError } from "../errors/InstantiationError.js";
import MySqlManager from "../config/db/MySqlManager.js";
import { createLogger, Logger } from "../utils/logger/logger.js";
import type { ParsedRecordAttributes } from "../config/db/models/ParsedRecord.js";
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
  row_data?: Record<string, unknown>;
}

class TraceSystemService extends ServiceManager {
  protected static instance: TraceSystemService;
  private logger: Logger;
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
    if (!TraceSystemService.instance) {
      TraceSystemService.instance = new TraceSystemService(Enforce);
    }
    return TraceSystemService.instance;
  }

  public async createTrace(trace: TraceRecord): Promise<void> {
    try {
      await this.dbManager.repositories.parsedRecords.create({
        _job_id: trace.job_id,
        _byte_offset: trace.byte_offset,
        _byte_length: trace.byte_length,
        _record_index: trace.record_index,
        _line_no: trace.line_no,
        _template_id: trace.template_id,
        _template_version: trace.template_version,
        _checksum: trace.checksum,
        _parsed_at: trace.parsed_at,
        _part_id: trace.part_id,
        fields: { s3_url: trace.s3_url, ...trace.row_data },
      });
      
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
      await this.dbManager.repositories.rubbishLogs.create({
        job_id: jobId,
        byte_offset: byteOffset,
        line_no: lineNo,
        raw_bytes: rawBytes,
        matched_template_id: matchedTemplateId,
      });
      
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
    const rows = await this.dbManager.repositories.parsedRecords.findByJob(jobId);

    return rows.map((row: ParsedRecordAttributes) => ({
      s3_url: (row.fields.s3_url as string | undefined) ?? "",
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

  public async getJobRubbishLog(jobId: string): Promise<unknown[]> {
    return this.dbManager.repositories.rubbishLogs.findByJob(jobId);
  }

  static generateChecksum(line: string): string {
    return crypto.createHash("sha256").update(line).digest("hex");
  }

  public async lineExists(jobId: string, byteOffset: number): Promise<boolean> {
    return this.dbManager.repositories.parsedRecords.exists(jobId, byteOffset);
  }

  public async getLineFateCounts(jobId: string): Promise<{
    parsed: number;
    dropped: number;
    failed: number;
  }> {
    const [parsed, dropped, failed] = await Promise.all([
      this.dbManager.repositories.parsedRecords.countByJob(jobId),
      this.dbManager.repositories.rubbishLogs.countByJob(jobId),
      this.dbManager.repositories.deadLetters.countByJob(jobId),
    ]);
    
    return { parsed, dropped, failed };
  }
}


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

  async getJobRubbishLog(jobId: string): Promise<unknown[]> {
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
