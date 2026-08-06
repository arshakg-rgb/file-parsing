import { Op, Sequelize } from "sequelize";
import { DeadLetterRepository } from "./DeadLetterRepository.js";
import { ParsedRecordRepository } from "./ParsedRecordRepository.js";
import { RubbishLogRepository } from "./RubbishLogRepository.js";
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
