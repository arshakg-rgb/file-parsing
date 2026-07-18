import { Op, Sequelize } from "sequelize";
import type { DatabaseModels } from "../models/index.js";
import type {
  ParseJobAttributes,
  ParseJobCreationAttributes,
} from "../models/ParseJob.js";
import type { DeadLetterAttributes, DeadLetterCreationAttributes } from "../models/DeadLetter.js";
import type { OutputPartAttributes, OutputPartCreationAttributes } from "../models/OutputPart.js";
import type { PendingArchiveEntryAttributes, PendingArchiveEntryCreationAttributes } from "../models/PendingArchiveEntry.js";
import type { ParsedRecordAttributes, ParsedRecordCreationAttributes } from "../models/ParsedRecord.js";
import type { RubbishLogAttributes, RubbishLogCreationAttributes } from "../models/RubbishLog.js";
import type { TemplateAttributes, TemplateCreationAttributes } from "../models/Template.js";
import type { SchemaMigrationAttributes, SchemaMigrationCreationAttributes } from "../models/SchemaMigration.js";
import type { RecordTemplate, RubbishTemplate } from "../../../shared/templateRegistry.js";

export class JobRepository {
  constructor(private models: DatabaseModels) {}

  private get ParseJob() {
    return this.models.ParseJob;
  }

  async findById(jobId: string, options?: { attributes?: (keyof ParseJobAttributes)[] }): Promise<ParseJobAttributes | null> {
    const row = await this.ParseJob.findByPk(jobId, {
      raw: true,
      attributes: options?.attributes as any,
    });
    return (row as ParseJobAttributes) || null;
  }

  async findByBatchId(batchId: string): Promise<ParseJobAttributes[]> {
    return (await this.ParseJob.findAll({
      where: { batch_id: batchId },
      raw: true,
    })) as ParseJobAttributes[];
  }

  async create(data: ParseJobCreationAttributes): Promise<ParseJobAttributes> {
    const row = await this.ParseJob.create(data as any, { raw: true });
    return row.get({ plain: true }) as ParseJobAttributes;
  }

  async updateFields(jobId: string, fields: Partial<ParseJobAttributes>): Promise<void> {
    const payload = { ...fields, updated_at: new Date() };
    await this.ParseJob.update(payload, { where: { job_id: jobId } });
  }

  async getStatus(jobId: string): Promise<string | undefined> {
    const row = await this.findById(jobId, { attributes: ["status"] });
    return row?.status;
  }

  async getFieldSpec(jobId: string): Promise<string[]> {
    const row = await this.findById(jobId, { attributes: ["field_spec"] });
    return (row?.field_spec as string[]) || [];
  }

  async updateS3Url(jobId: string, s3Url: string, size: number): Promise<void> {
    await this.updateFields(jobId, { s3_url: s3Url, size });
  }

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

  async hold(jobId: string, reason?: string): Promise<void> {
    const existing = await this.findById(jobId, { attributes: ["error"] });
    await this.updateFields(jobId, { status: "held", error: reason || existing?.error });
  }

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

  async getCounts(jobId: string): Promise<any> {
    const row = await this.findById(jobId, { attributes: ["counts"] });
    return row?.counts || { parsed: 0, dropped_rubbish: 0, failed: 0 };
  }
}

export class DeadLetterRepository {
  constructor(private models: DatabaseModels) {}

  private get DeadLetter() {
    return this.models.DeadLetter;
  }

  async create(data: DeadLetterCreationAttributes, options?: { conflictOn?: "job_id_line_no" | "dlq_id" }): Promise<DeadLetterAttributes | null> {
    if (options?.conflictOn === "job_id_line_no") {
      const existing = await this.DeadLetter.findOne({
        where: { job_id: data.job_id, line_no: data.line_no },
      });
      if (existing) return null;
    }
    const row = await this.DeadLetter.create(data as any);
    return row.get({ plain: true }) as DeadLetterAttributes;
  }

  async findById(dlqId: string): Promise<DeadLetterAttributes | null> {
    const row = await this.DeadLetter.findByPk(dlqId, { raw: true });
    return (row as DeadLetterAttributes) || null;
  }

  async findByJobAndStatus(jobId: string, status: string): Promise<DeadLetterAttributes[]> {
    return (await this.DeadLetter.findAll({
      where: { job_id: jobId, status },
      order: [["byte_offset", "ASC"]],
      raw: true,
    })) as DeadLetterAttributes[];
  }

  async findByJob(jobId: string): Promise<DeadLetterAttributes[]> {
    return (await this.DeadLetter.findAll({
      where: { job_id: jobId },
      order: [["byte_offset", "ASC"]],
      raw: true,
    })) as DeadLetterAttributes[];
  }

  async incrementAttempts(dlqId: string, status?: string): Promise<void> {
    const row = await this.DeadLetter.findByPk(dlqId);
    if (!row) return;
    row.attempts = (row.attempts || 0) + 1;
    if (status) row.status = status;
    row.updated_at = new Date();
    await row.save();
  }

  async updateStatus(dlqId: string, status: string, options?: { attempts?: number }): Promise<void> {
    const row = await this.DeadLetter.findByPk(dlqId);
    if (!row) return;
    row.status = status;
    if (options?.attempts !== undefined) row.attempts = options.attempts;
    row.updated_at = new Date();
    await row.save();
  }

  async updateLineNo(dlqId: string, lineNo: number): Promise<void> {
    const row = await this.DeadLetter.findByPk(dlqId);
    if (!row) return;
    row.line_no = lineNo;
    row.updated_at = new Date();
    await row.save();
  }

  async countByJob(jobId: string): Promise<number> {
    return this.DeadLetter.count({ where: { job_id: jobId } });
  }
}

export class OutputPartRepository {
  constructor(private models: DatabaseModels) {}

  private get OutputPart() {
    return this.models.OutputPart;
  }

  async findByJob(jobId: string): Promise<OutputPartAttributes[]> {
    return (await this.OutputPart.findAll({
      where: { job_id: jobId },
      raw: true,
    })) as OutputPartAttributes[];
  }

  async create(data: OutputPartCreationAttributes): Promise<OutputPartAttributes | null> {
    const [row] = await this.OutputPart.findOrCreate({
      where: { part_id: data.part_id },
      defaults: data as any,
    });
    return row.get({ plain: true }) as OutputPartAttributes;
  }
}

export class PendingArchiveEntryRepository {
  constructor(private models: DatabaseModels) {}

  private get Entry() {
    return this.models.PendingArchiveEntry;
  }

  async create(data: PendingArchiveEntryCreationAttributes): Promise<PendingArchiveEntryAttributes> {
    const row = await this.Entry.create(data as any);
    return row.get({ plain: true }) as PendingArchiveEntryAttributes;
  }

  async findById(id: string): Promise<PendingArchiveEntryAttributes | null> {
    const row = await this.Entry.findByPk(id, { raw: true });
    return (row as PendingArchiveEntryAttributes) || null;
  }

  async markStatus(id: string, status: string, error?: string): Promise<void> {
    const row = await this.Entry.findByPk(id);
    if (!row) return;
    row.status = status;
    if (error !== undefined) row.error = error;
    row.updated_at = new Date();
    await row.save();
  }

  async findByJob(jobId: string): Promise<PendingArchiveEntryAttributes[]> {
    return (await this.Entry.findAll({ where: { job_id: jobId }, raw: true })) as PendingArchiveEntryAttributes[];
  }

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

  async getCountByJob(jobId: string): Promise<{ pending: number; completed: number; failed: number }> {
    const [pending, completed, failed] = await Promise.all([
      this.Entry.count({ where: { job_id: jobId, status: "pending" } }),
      this.Entry.count({ where: { job_id: jobId, status: "completed" } }),
      this.Entry.count({ where: { job_id: jobId, status: "failed" } }),
    ]);
    return { pending, completed, failed };
  }

  async getTotalSize(jobId: string): Promise<number> {
    const value = await this.Entry.sum("entry_size", {
      where: { job_id: jobId, status: { [Op.in]: ["completed", "processing"] } },
    });
    return Number(value) || 0;
  }
}

export class ParsedRecordRepository {
  constructor(private models: DatabaseModels) {}

  private get ParsedRecord() {
    return this.models.ParsedRecord;
  }

  async create(data: ParsedRecordCreationAttributes): Promise<ParsedRecordAttributes | null> {
    try {
      const row = await this.ParsedRecord.create(data as any);
      return row.get({ plain: true }) as ParsedRecordAttributes;
    } catch (err: any) {
      if (err.name === "SequelizeUniqueConstraintError") return null;
      throw err;
    }
  }

  async bulkCreate(rows: ParsedRecordCreationAttributes[], ignoreDuplicates = true): Promise<void> {
    await this.ParsedRecord.bulkCreate(rows as any, { ignoreDuplicates });
  }

  async findByJob(jobId: string): Promise<ParsedRecordAttributes[]> {
    return (await this.ParsedRecord.findAll({
      where: { _job_id: jobId },
      order: [["_byte_offset", "ASC"]],
      raw: true,
    })) as ParsedRecordAttributes[];
  }

  async exists(jobId: string, byteOffset: number): Promise<boolean> {
    const count = await this.ParsedRecord.count({
      where: { _job_id: jobId, _byte_offset: byteOffset },
    });
    return count > 0;
  }

  async countByJob(jobId: string): Promise<number> {
    return this.ParsedRecord.count({ where: { _job_id: jobId } });
  }
}

export class RubbishLogRepository {
  constructor(private models: DatabaseModels) {}

  private get RubbishLog() {
    return this.models.RubbishLog;
  }

  async create(data: RubbishLogCreationAttributes): Promise<RubbishLogAttributes> {
    const row = await this.RubbishLog.create(data as any);
    return row.get({ plain: true }) as RubbishLogAttributes;
  }

  async findByJob(jobId: string): Promise<RubbishLogAttributes[]> {
    return (await this.RubbishLog.findAll({
      where: { job_id: jobId },
      order: [["byte_offset", "ASC"]],
      raw: true,
    })) as RubbishLogAttributes[];
  }

  async countByJob(jobId: string): Promise<number> {
    return this.RubbishLog.count({ where: { job_id: jobId } });
  }
}

export class TemplateRepository {
  constructor(private models: DatabaseModels) {}

  private get Template() {
    return this.models.Template;
  }

  async findByKind(kind: string): Promise<TemplateAttributes[]> {
    return (await this.Template.findAll({ where: { kind }, raw: true })) as TemplateAttributes[];
  }

  async findByFingerprint(fingerprint: string): Promise<TemplateAttributes | null> {
    const row = await this.Template.findOne({ where: { fingerprint }, raw: true });
    return (row as TemplateAttributes) || null;
  }

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
        await this.Template.create(data as any);
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
        await this.Template.create(data as any);
      }
    }
  }
}

export class SchemaMigrationRepository {
  constructor(private models: DatabaseModels) {}

  private get Migration() {
    return this.models.SchemaMigration;
  }

  async ensureTable(): Promise<void> {
    await this.Migration.sync({ force: false });
  }

  async getAppliedVersions(): Promise<number[]> {
    const rows = (await this.Migration.findAll({
      order: [["version", "ASC"]],
      raw: true,
    })) as SchemaMigrationAttributes[];
    return rows.map((r) => r.version);
  }

  async addVersion(version: number, description?: string): Promise<void> {
    await this.Migration.create({ version, description } as SchemaMigrationCreationAttributes);
  }
}

export class Repositories {
  readonly jobs: JobRepository;
  readonly deadLetters: DeadLetterRepository;
  readonly outputParts: OutputPartRepository;
  readonly pendingArchiveEntries: PendingArchiveEntryRepository;
  readonly parsedRecords: ParsedRecordRepository;
  readonly rubbishLogs: RubbishLogRepository;
  readonly templates: TemplateRepository;
  readonly schemaMigrations: SchemaMigrationRepository;

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
