import { Op, Sequelize } from "sequelize";
import type { DatabaseModels } from "@config/db/models/index.js";
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
import type { JobCounts } from "@shared/models/job.js";
import type { RecordTemplate, RubbishTemplate } from "@shared/TemplateRegistryService.js";

/**
 * JobRepository is responsible for job repository operations.
 */
export class JobRepository {
    /**
   * Constructs a new JobRepository instance.
   * @param models - The models
   */
  constructor(private models: DatabaseModels) {}

    /**
   * Gets the parse job.
   */
  private get ParseJob() {
    return this.models.ParseJob;
  }

    /**
   * Finds by id
   * @param jobId - The job identifier
   * @param options - The options object
   * @returns A promise that resolves to the result
   */
  async findById(jobId: string, options?: { attributes?: (keyof ParseJobAttributes)[] }): Promise<ParseJobAttributes | null> {
    const row = await this.ParseJob.findByPk(jobId, {
      raw: true,
      attributes: options?.attributes as string[],
    });
    return (row as ParseJobAttributes) || null;
  }

    /**
   * Finds by batch id
   * @param batchId - The batch identifier
   * @returns A promise that resolves to the list
   */
  async findByBatchId(batchId: string): Promise<ParseJobAttributes[]> {
    return (await this.ParseJob.findAll({
      where: { batch_id: batchId },
      raw: true,
    })) as ParseJobAttributes[];
  }

    /**
   * Creates the operation
   * @param data - The data to process
   * @returns A promise that resolves to the result
   */
  async create(data: ParseJobCreationAttributes): Promise<ParseJobAttributes> {
    const row = await this.ParseJob.create(data, { raw: true });
    return row.get({ plain: true }) as ParseJobAttributes;
  }

    /**
   * Updates fields
   * @param jobId - The job identifier
   * @param fields - The fields
   */
  async updateFields(jobId: string, fields: Partial<ParseJobAttributes>): Promise<void> {
    const payload = { ...fields, updated_at: new Date() };
    await this.ParseJob.update(payload, { where: { job_id: jobId } });
  }

    /**
   * Gets status
   * @param jobId - The job identifier
   * @returns A promise that resolves to the result
   */
  async getStatus(jobId: string): Promise<string | undefined> {
    const row = await this.findById(jobId, { attributes: ["status"] });
    return row?.status;
  }

    /**
   * Gets field spec
   * @param jobId - The job identifier
   * @returns A promise that resolves to the list
   */
  async getFieldSpec(jobId: string): Promise<string[]> {
    const row = await this.findById(jobId, { attributes: ["field_spec"] });
    return (row?.field_spec as string[]) || [];
  }

    /**
   * Updates s3 url
   * @param jobId - The job identifier
   * @param s3Url - The s3 url
   * @param size - The size value
   */
  async updateS3Url(jobId: string, s3Url: string, size: number): Promise<void> {
    await this.updateFields(jobId, { s3_url: s3Url, size });
  }

    /**
   * Marks failed
   * @param jobId - The job identifier
   * @param reason - The reason
   */
  async markFailed(jobId: string, reason: string): Promise<void> {
    const job = await this.ParseJob.findByPk(jobId);
    if (!job) return;
    const timings = { ...(job.timings || {}), failed_at: new Date().toISOString() };
    job.status = "failed";
    job.error = reason;
    job.timings = timings;
    job.updated_at = new Date();
    await job.save();
  }

    /**
   * Holds the operation
   * @param jobId - The job identifier
   * @param reason - The reason
   */
  async hold(jobId: string, reason?: string): Promise<void> {
    const existing = await this.findById(jobId, { attributes: ["error"] });
    await this.updateFields(jobId, { status: "held", error: reason || existing?.error });
  }

    /**
   * Finds stuck jobs
   * @param thresholdMinutes - The threshold minutes
   * @returns A promise that resolves to the list
   */
  async findStuckJobs(thresholdMinutes: number): Promise<ParseJobAttributes[]> {
    const terminalStatuses = ["done", "failed", "partial", "held"];
    return (await this.ParseJob.findAll({
      where: {
        status: { [Op.notIn]: terminalStatuses },
        updated_at: { [Op.lt]: Sequelize.literal(`NOW() - INTERVAL '${thresholdMinutes} minutes'`) },
      },
      attributes: ["job_id", "status", "timings", "error", "created_at"],
      raw: true,
    })) as ParseJobAttributes[];
  }

    /**
   * Finds stuck ingesting
   * @param hours - The hours
   * @returns A promise that resolves to the list
   */
  async findStuckIngesting(hours = 2): Promise<ParseJobAttributes[]> {
    return (await this.ParseJob.findAll({
      where: {
        status: "ingesting",
        updated_at: { [Op.lt]: Sequelize.literal(`NOW() - INTERVAL '${hours} hours'`) },
      },
      attributes: ["job_id", "status", "created_at", "updated_at"],
      raw: true,
    })) as ParseJobAttributes[];
  }

    /**
   * Gets batch stats
   * @param batchId - The batch identifier
   * @returns A promise that resolves to the result
   */
  async getBatchStats(batchId: string): Promise<{
    totalJobs: number;
    passedJobs: number;
    heldJobs: number;
    failedJobs: number;
  }> {
    const [totalJobs, passedJobs, heldJobs, failedJobs] = await Promise.all([
      this.ParseJob.count({ where: { batch_id: batchId } }),
      this.ParseJob.count({ where: { batch_id: batchId, status: "done" } }),
      this.ParseJob.count({ where: { batch_id: batchId, status: "held" } }),
      this.ParseJob.count({ where: { batch_id: batchId, status: "failed" } }),
    ]);
    return { totalJobs, passedJobs, heldJobs, failedJobs };
  }

    /**
   * Gets counts
   * @param jobId - The job identifier
   * @returns A promise that resolves to the result
   */
  async getCounts(jobId: string): Promise<JobCounts> {
    const row = await this.findById(jobId, { attributes: ["counts"] });
    return row?.counts || { parsed: 0, dropped_rubbish: 0, dlq_count: 0, failed_by_class: {} };
  }
}

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
    const row = await this.DeadLetter.create(data);
    return row.get({ plain: true }) as DeadLetterAttributes;
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
}

/**
 * OutputPartRepository is responsible for output part repository operations.
 */
export class OutputPartRepository {
    /**
   * Constructs a new OutputPartRepository instance.
   * @param models - The models
   */
  constructor(private models: DatabaseModels) {}

    /**
   * Gets the output part.
   */
  private get OutputPart() {
    return this.models.OutputPart;
  }

    /**
   * Finds by job
   * @param jobId - The job identifier
   * @returns A promise that resolves to the list
   */
  async findByJob(jobId: string): Promise<OutputPartAttributes[]> {
    return (await this.OutputPart.findAll({
      where: { job_id: jobId },
      raw: true,
    })) as OutputPartAttributes[];
  }

    /**
   * Creates the operation
   * @param data - The data to process
   * @returns A promise that resolves to the result
   */
  async create(data: OutputPartCreationAttributes): Promise<OutputPartAttributes | null> {
    const [row] = await this.OutputPart.findOrCreate({
      where: { part_id: data.part_id },
      defaults: data,
    });
    return row.get({ plain: true }) as OutputPartAttributes;
  }
}

/**
 * PendingArchiveEntryRepository is responsible for pending archive entry repository operations.
 */
export class PendingArchiveEntryRepository {
    /**
   * Constructs a new PendingArchiveEntryRepository instance.
   * @param models - The models
   */
  constructor(private models: DatabaseModels) {}

    /**
   * Gets the entry.
   */
  private get Entry() {
    return this.models.PendingArchiveEntry;
  }

    /**
   * Creates the operation
   * @param data - The data to process
   * @returns A promise that resolves to the result
   */
  async create(data: PendingArchiveEntryCreationAttributes): Promise<PendingArchiveEntryAttributes> {
    const row = await this.Entry.create(data);
    return row.get({ plain: true }) as PendingArchiveEntryAttributes;
  }

    /**
   * Finds by id
   * @param id - The unique identifier
   * @returns A promise that resolves to the result
   */
  async findById(id: string): Promise<PendingArchiveEntryAttributes | null> {
    const row = await this.Entry.findByPk(id, { raw: true });
    return (row as PendingArchiveEntryAttributes) || null;
  }

    /**
   * Marks status
   * @param id - The unique identifier
   * @param status - The status
   * @param error - The error that occurred
   */
  async markStatus(id: string, status: string, error?: string): Promise<void> {
    const row = await this.Entry.findByPk(id);
    if (!row) return;
    row.status = status;
    if (error !== undefined) row.error = error;
    row.updated_at = new Date();
    await row.save();
  }

    /**
   * Finds by job
   * @param jobId - The job identifier
   * @returns A promise that resolves to the list
   */
  async findByJob(jobId: string): Promise<PendingArchiveEntryAttributes[]> {
    return (await this.Entry.findAll({ where: { job_id: jobId }, raw: true })) as PendingArchiveEntryAttributes[];
  }

    /**
   * Finds stale entries
   * @param jobId - The job identifier
   * @param hours - The hours
   * @param statuses - The statuses
   * @returns A promise that resolves to the list
   */
  async findStaleEntries(
    jobId: string,
    hours = 3,
    statuses = ["pending", "processing"]
  ): Promise<PendingArchiveEntryAttributes[]> {
    return (await this.Entry.findAll({
      where: {
        job_id: jobId,
        status: { [Op.in]: statuses },
        updated_at: { [Op.lt]: Sequelize.literal(`NOW() - INTERVAL '${hours} hours'`) },
      },
      attributes: ["id", "entry_name", "created_at"],
      raw: true,
    })) as PendingArchiveEntryAttributes[];
  }

    /**
   * Gets count by job
   * @param jobId - The job identifier
   * @returns A promise that resolves to the result
   */
  async getCountByJob(jobId: string): Promise<{ pending: number; completed: number; failed: number }> {
    const [pending, completed, failed] = await Promise.all([
      this.Entry.count({ where: { job_id: jobId, status: "pending" } }),
      this.Entry.count({ where: { job_id: jobId, status: "completed" } }),
      this.Entry.count({ where: { job_id: jobId, status: "failed" } }),
    ]);
    return { pending, completed, failed };
  }

    /**
   * Gets total size
   * @param jobId - The job identifier
   * @returns A promise that resolves to the result
   */
  async getTotalSize(jobId: string): Promise<number> {
    const value = await this.Entry.sum("entry_size", {
      where: { job_id: jobId, status: { [Op.in]: ["completed", "processing"] } },
    });
    return Number(value) || 0;
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
  async bulkCreate(rows: ParsedRecordCreationAttributes[], ignoreDuplicates = true): Promise<void> {
    await this.ParsedRecord.bulkCreate(rows, { ignoreDuplicates });
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
}

/**
 * TemplateRepository is responsible for template repository operations.
 */
export class TemplateRepository {
    /**
   * Constructs a new TemplateRepository instance.
   * @param models - The models
   */
  constructor(private models: DatabaseModels) {}

    /**
   * Gets the template.
   */
  private get Template() {
    return this.models.Template;
  }

    /**
   * Finds by kind
   * @param kind - The kind
   * @returns A promise that resolves to the list
   */
  async findByKind(kind: string): Promise<TemplateAttributes[]> {
    return (await this.Template.findAll({ where: { kind }, raw: true })) as TemplateAttributes[];
  }

    /**
   * Finds by fingerprint
   * @param fingerprint - The fingerprint
   * @returns A promise that resolves to the result
   */
  async findByFingerprint(fingerprint: string): Promise<TemplateAttributes | null> {
    const row = await this.Template.findOne({ where: { fingerprint }, raw: true });
    return (row as TemplateAttributes) || null;
  }

    /**
   * Saves template
   * @param template - The template
   * @param kind - The kind
   */
  async saveTemplate(template: RecordTemplate | RubbishTemplate, kind: "record" | "rubbish"): Promise<void> {
    const existing = await this.findByFingerprint(template.fingerprint);
    const base = {
      fingerprint: template.fingerprint,
      version: (existing?.version || template.version || 0) + 1,
      kind,
      source: template.source,
    };

    if (kind === "record") {
      const rt = template as RecordTemplate;
      const data = {
        ...base,
        template_id: existing ? existing.template_id : rt.template_id,
        field_map: rt.field_map,
        structure: rt.structure,
        length_hint: rt.length_hint,
        signature: null,
        confidence: null,
      } as TemplateCreationAttributes;
      if (existing) {
        await this.Template.update(data, { where: { fingerprint: template.fingerprint } });
      } else {
        await this.Template.create(data);
      }
    } else {
      const rt = template as RubbishTemplate;
      const data = {
        ...base,
        template_id: existing ? existing.template_id : rt.template_id,
        field_map: null,
        structure: null,
        length_hint: null,
        signature: rt.signature,
        confidence: rt.confidence,
      } as TemplateCreationAttributes;
      if (existing) {
        await this.Template.update(data, { where: { fingerprint: template.fingerprint } });
      } else {
        await this.Template.create(data);
      }
    }
  }
}

/**
 * SchemaMigrationRepository is responsible for schema migration repository operations.
 */
export class SchemaMigrationRepository {
    /**
   * Constructs a new SchemaMigrationRepository instance.
   * @param models - The models
   */
  constructor(private models: DatabaseModels) {}

    /**
   * Gets the migration.
   */
  private get Migration() {
    return this.models.SchemaMigration;
  }

    /**
   * Ensures table
   */
  async ensureTable(): Promise<void> {
    await this.Migration.sync({ force: false });
  }

    /**
   * Gets applied versions
   * @returns A promise that resolves to the list
   */
  async getAppliedVersions(): Promise<number[]> {
    const rows = (await this.Migration.findAll({
      order: [["version", "ASC"]],
      raw: true,
    })) as SchemaMigrationAttributes[];
    return rows.map((r) => r.version);
  }

    /**
   * Adds version
   * @param version - The version
   * @param description - The description
   */
  async addVersion(version: number, description?: string): Promise<void> {
    await this.Migration.create({ version, description } as SchemaMigrationCreationAttributes);
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
  }
}
