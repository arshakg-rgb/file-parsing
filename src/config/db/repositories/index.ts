import { Op, Sequelize } from "sequelize";
import { TemplateRepository } from "./TemplateRepository.js";
import { SchemaMigrationRepository } from "./SchemaMigrationRepository.js";
import { JobLogRepository } from "./JobLogRepository.js";
import { OutputPartRepository } from "./OutputPartRepository.js";
import { PendingArchiveEntryRepository } from "./PendingArchiveEntryRepository.js";
import { JobRepository } from "./JobRepository.js";
import { BigQueryManager } from "../BigQueryManager.js";
import type {DatabaseModels, IParseJob} from "@config/db/models/index.js";
import type {
  ParseJobAttributes,
  ParseJobCreationAttributes,
} from "@config/db/models/ParseJob.js";
import type { DeadLetterAttributes, DeadLetterCreationAttributes } from "@config/db/models/DeadLetter.js";
import type { OutputPartAttributes, OutputPartCreationAttributes } from "@config/db/models/OutputPart.js";
import type { PendingArchiveEntryAttributes, PendingArchiveEntryCreationAttributes } from "@config/db/models/PendingArchiveEntry.js";
import type { ParsedRecordAttributes, ParsedRecordCreationAttributes } from "@config/db/models/ParsedRecord.js";
import type { RubbishLogAttributes, RubbishLogCreationAttributes } from "@config/db/models/RubbishLog.js";
import type { TemplateAttributes, TemplateCreationAttributes } from "@config/db/models/Template.js";
import type { SchemaMigrationAttributes, SchemaMigrationCreationAttributes } from "@config/db/models/SchemaMigration.js";
import type { JobLogAttributes, JobLogCreationAttributes } from "@config/db/models/JobLog.js";
import type { JobCounts } from "@shared/models/job.js";
import {RecordTemplate, RubbishTemplate} from "@shared/io/ITemplateRegistryService";

/**
 * DeadLetterRepository is responsible for dead letter repository operations.
 */
export class DeadLetterRepository {
    /**
   * Constructs a new DeadLetterRepository instance.
   * @param models - The models
   */
  constructor(private models: DatabaseModels) {}

    /**
   * Gets the dead letter.
   */
  private get DeadLetter() {
    return this.models.DeadLetter;
  }

    /**
   * Creates the operation
   * @param data - The data to process
   * @param options - The options object
   * @returns A promise that resolves to the result
   */
  async create(data: DeadLetterCreationAttributes, options?: { conflictOn?: "job_id_line_no" | "dlq_id" }): Promise<DeadLetterAttributes | null> {
    if (options?.conflictOn === "job_id_line_no") {
      const existing = await this.DeadLetter.findOne({
        where: { job_id: data.job_id, line_no: data.line_no },
      });
      if (existing) return null;
    }
    try {
      const row = await this.DeadLetter.create(data);
      return row.get({ plain: true }) as DeadLetterAttributes;
    } catch (err: unknown) {
      if ((err as { name?: string }).name === "SequelizeUniqueConstraintError") return null;
      throw err;
    }
  }

    /**
   * Performs the bulk create operation.
   * @param rows - The rows
   */
  async bulkCreate(rows: DeadLetterCreationAttributes[]): Promise<void> {
    const bqRows = rows.map((r) => ({
      ...r,
      created_at: new Date(),
      updated_at: new Date(),
    })) as Record<string, unknown>[];

    await BigQueryManager.getInstance().insert("dead_letters", bqRows);
  }

    /**
   * Finds by id
   * @param dlqId - The dlq id
   * @returns A promise that resolves to the result
   */
  async findById(dlqId: string): Promise<DeadLetterAttributes | null> {
    const row = await this.DeadLetter.findByPk(dlqId, { raw: true });
    return (row as DeadLetterAttributes) || null;
  }

    /**
   * Finds by job and status
   * @param jobId - The job identifier
   * @param status - The status
   * @returns A promise that resolves to the list
   */
  async findByJobAndStatus(jobId: string, status: string): Promise<DeadLetterAttributes[]> {
    return (await this.DeadLetter.findAll({
      where: { job_id: jobId, status },
      order: [["byte_offset", "ASC"]],
      raw: true,
    })) as DeadLetterAttributes[];
  }

    /**
   * Finds by job
   * @param jobId - The job identifier
   * @returns A promise that resolves to the list
   */
  async findByJob(jobId: string): Promise<DeadLetterAttributes[]> {
    return (await this.DeadLetter.findAll({
      where: { job_id: jobId },
      order: [["byte_offset", "ASC"]],
      raw: true,
    })) as DeadLetterAttributes[];
  }

    /**
   * Performs the increment attempts operation.
   * @param dlqId - The dlq id
   * @param status - The status
   */
  async incrementAttempts(dlqId: string, status?: string): Promise<void> {
    const row = await this.DeadLetter.findByPk(dlqId);
    if (!row) return;
    row.attempts = (row.attempts || 0) + 1;
    if (status) row.status = status;
    row.updated_at = new Date();
    await row.save();
  }

    /**
   * Updates status
   * @param dlqId - The dlq id
   * @param status - The status
   * @param options - The options object
   */
  async updateStatus(dlqId: string, status: string, options?: { attempts?: number }): Promise<void> {
    const row = await this.DeadLetter.findByPk(dlqId);
    if (!row) return;
    row.status = status;
    if (options?.attempts !== undefined) row.attempts = options.attempts;
    row.updated_at = new Date();
    await row.save();
  }

    /**
   * Updates line no
   * @param dlqId - The dlq id
   * @param lineNo - The line no
   */
  async updateLineNo(dlqId: string, lineNo: number): Promise<void> {
    const row = await this.DeadLetter.findByPk(dlqId);
    if (!row) return;
    row.line_no = lineNo;
    row.updated_at = new Date();
    await row.save();
  }

    /**
   * Performs the count by job operation.
   * @param jobId - The job identifier
   * @returns A promise that resolves to the result
   */
  async countByJob(jobId: string): Promise<number> {
    return this.DeadLetter.count({ where: { job_id: jobId } });
  }

    /**
   * Summarizes failed (dead-lettered) lines for a job: total count, the exact
   * source line numbers involved (capped), and a breakdown by failure class.
   * @param jobId - The job identifier
   * @param lineNumbersLimit - Max number of individual line numbers to return
   * @returns A promise that resolves to the summary
   */
  async getSummaryByJob(jobId: string, lineNumbersLimit = 500): Promise<{
    count: number;
    line_numbers: number[];
    line_numbers_truncated: boolean;
    by_class: Record<string, number>;
  }> {
    const count: number = await this.DeadLetter.count({ where: { job_id: jobId } });

    const lineRows = (await this.DeadLetter.findAll({
      where: { job_id: jobId },
      attributes: ["line_no"],
      order: [["line_no", "ASC"]],
      limit: lineNumbersLimit,
      raw: true,
    })) as unknown as { line_no: number }[];

    const classRows = (await this.DeadLetter.findAll({
      where: { job_id: jobId },
      attributes: ["failure_class", [Sequelize.fn("COUNT", Sequelize.col("dlq_id")), "count"]],
      group: ["failure_class"],
      raw: true,
    })) as unknown as { failure_class: string; count: string }[];

    const by_class: Record<string, number> = {};
    for (const row of classRows) {
      by_class[row.failure_class] = Number(row.count);
    }

    return {
      count,
      line_numbers: lineRows.map((r) => Number(r.line_no)),
      line_numbers_truncated: count > lineNumbersLimit,
      by_class,
    };
  }
}

/**
 * ParsedRecordRepository is responsible for parsed record repository operations.
 */
export class ParsedRecordRepository {
    /**
   * Constructs a new ParsedRecordRepository instance.
   * @param models - The models
   */
  constructor(private models: DatabaseModels) {}

    /**
   * Gets the parsed record.
   */
  private get ParsedRecord() {
    return this.models.ParsedRecord;
  }

    /**
   * Creates the operation
   * @param data - The data to process
   * @returns A promise that resolves to the result
   */
  async create(data: ParsedRecordCreationAttributes): Promise<ParsedRecordAttributes | null> {
    try {
      const row = await this.ParsedRecord.create(data);
      return row.get({ plain: true }) as ParsedRecordAttributes;
    } catch (err: unknown) {
      if ((err as { name?: string }).name === "SequelizeUniqueConstraintError") return null;
      throw err;
    }
  }

    /**
   * Performs the bulk create operation.
   * @param rows - The rows
   * @param ignoreDuplicates - The ignore duplicates
   */
  async bulkCreate(rows: ParsedRecordCreationAttributes[], _ignoreDuplicates = true): Promise<void> {
    const bqRows = rows.map((r, i) => ({
      ...r,
      id: (r as { id?: number }).id ?? Date.now() + i,
      _parsed_at: r._parsed_at,
      fields: JSON.stringify(r.fields),
    })) as Record<string, unknown>[];

    await BigQueryManager.getInstance().insert("parsed_records", bqRows);
  }

    /**
   * Finds by job
   * @param jobId - The job identifier
   * @returns A promise that resolves to the list
   */
  async findByJob(jobId: string): Promise<ParsedRecordAttributes[]> {
    return (await this.ParsedRecord.findAll({
      where: { _job_id: jobId },
      order: [["_byte_offset", "ASC"]],
      raw: true,
    })) as ParsedRecordAttributes[];
  }

    /**
   * Performs the exists operation.
   * @param jobId - The job identifier
   * @param byteOffset - The byte offset
   * @returns True if the operation succeeds, false otherwise
   */
  async exists(jobId: string, byteOffset: number): Promise<boolean> {
    const count = await this.ParsedRecord.count({
      where: { _job_id: jobId, _byte_offset: byteOffset },
    });
    return count > 0;
  }

    /**
   * Performs the count by job operation.
   * @param jobId - The job identifier
   * @returns A promise that resolves to the result
   */
  async countByJob(jobId: string): Promise<number> {
    return this.ParsedRecord.count({ where: { _job_id: jobId } });
  }

    /**
   * Returns how many rows each record template parsed for a job.
   * @param jobId - The job identifier
   * @returns A promise that resolves to the per-template counts
   */
  async getTemplateUsageCounts(jobId: string): Promise<{ template_id: string; count: number }[]> {
    const rows = (await this.ParsedRecord.findAll({
      where: { _job_id: jobId },
      attributes: ["_template_id", [Sequelize.fn("COUNT", Sequelize.col("_record_index")), "count"]],
      group: ["_template_id"],
      raw: true,
    })) as unknown as { _template_id: string; count: string }[];

    return rows.map((r) => ({ template_id: r._template_id, count: Number(r.count) }));
  }
}

/**
 * RubbishLogRepository is responsible for rubbish log repository operations.
 */
export class RubbishLogRepository {
    /**
   * Constructs a new RubbishLogRepository instance.
   * @param models - The models
   */
  constructor(private models: DatabaseModels) {}

    /**
   * Gets the rubbish log.
   */
  private get RubbishLog() {
    return this.models.RubbishLog;
  }

    /**
   * Creates the operation
   * @param data - The data to process
   * @returns A promise that resolves to the result
   */
  async create(data: RubbishLogCreationAttributes): Promise<RubbishLogAttributes> {
    const row = await this.RubbishLog.create(data);
    return row.get({ plain: true }) as RubbishLogAttributes;
  }

    /**
   * Performs the bulk create operation.
   * @param rows - The rows
   */
  async bulkCreate(rows: RubbishLogCreationAttributes[]): Promise<void> {
    const bqRows = rows.map((r, i) => ({
      ...r,
      id: (r as { id?: number }).id ?? Date.now() + i,
      logged_at: (r as { logged_at?: Date }).logged_at ?? new Date(),
    })) as Record<string, unknown>[];

    await BigQueryManager.getInstance().insert("rubbish_log", bqRows);
  }

    /**
   * Finds by job
   * @param jobId - The job identifier
   * @returns A promise that resolves to the list
   */
  async findByJob(jobId: string): Promise<RubbishLogAttributes[]> {
    return (await this.RubbishLog.findAll({
      where: { job_id: jobId },
      order: [["byte_offset", "ASC"]],
      raw: true,
    })) as RubbishLogAttributes[];
  }

    /**
   * Performs the count by job operation.
   * @param jobId - The job identifier
   * @returns A promise that resolves to the result
   */
  async countByJob(jobId: string): Promise<number> {
    return this.RubbishLog.count({ where: { job_id: jobId } });
  }

    /**
   * Summarizes dropped (rubbish) lines for a job: total count, the exact
   * source line numbers involved (capped), and a breakdown by the rubbish
   * template that matched.
   * @param jobId - The job identifier
   * @param lineNumbersLimit - Max number of individual line numbers to return
   * @returns A promise that resolves to the summary
   */
  async getSummaryByJob(jobId: string, lineNumbersLimit = 500): Promise<{
    count: number;
    line_numbers: number[];
    line_numbers_truncated: boolean;
    by_template: Record<string, number>;
  }> {
    const count: number = await this.RubbishLog.count({ where: { job_id: jobId } });

    const lineRows = (await this.RubbishLog.findAll({
      where: { job_id: jobId },
      attributes: ["line_no"],
      order: [["line_no", "ASC"]],
      limit: lineNumbersLimit,
      raw: true,
    })) as unknown as { line_no: number }[];

    const templateRows = (await this.RubbishLog.findAll({
      where: { job_id: jobId },
      attributes: ["matched_template_id", [Sequelize.fn("COUNT", Sequelize.col("id")), "count"]],
      group: ["matched_template_id"],
      raw: true,
    })) as unknown as { matched_template_id: string; count: string }[];

    const by_template: Record<string, number> = {};
    for (const row of templateRows) {
      by_template[row.matched_template_id] = Number(row.count);
    }

    return {
      count,
      line_numbers: lineRows.map((r) => Number(r.line_no)),
      line_numbers_truncated: count > lineNumbersLimit,
      by_template,
    };
  }
}


/**
 * Repositories is responsible for repositories operations.
 */
export class Repositories {
    /**
   * Jobs
   */
  readonly jobs: JobRepository;
    /**
   * Dead Letters
   */
  readonly deadLetters: DeadLetterRepository;
    /**
   * Output Parts
   */
  readonly outputParts: OutputPartRepository;
    /**
   * Pending Archive Entries
   */
  readonly pendingArchiveEntries: PendingArchiveEntryRepository;
    /**
   * Parsed Records
   */
  readonly parsedRecords: ParsedRecordRepository;
    /**
   * Rubbish Logs
   */
  readonly rubbishLogs: RubbishLogRepository;
    /**
   * Templates
   */
  readonly templates: TemplateRepository;
    /**
   * Schema Migrations
   */
  readonly schemaMigrations: SchemaMigrationRepository;
    /**
   * Job Logs
   */
  readonly jobLogs: JobLogRepository;

    /**
   * Constructs a new Repositories instance.
   * @param models - The models
   */
  constructor(public models: DatabaseModels) {
    this.jobs = new JobRepository(models);
    this.deadLetters = new DeadLetterRepository(models);
    this.outputParts = new OutputPartRepository(models);
    this.pendingArchiveEntries = new PendingArchiveEntryRepository(models);
    this.parsedRecords = new ParsedRecordRepository(models);
    this.rubbishLogs = new RubbishLogRepository(models);
    this.templates = new TemplateRepository(models);
    this.schemaMigrations = new SchemaMigrationRepository(models);
    this.jobLogs = new JobLogRepository(models);
  }
}
