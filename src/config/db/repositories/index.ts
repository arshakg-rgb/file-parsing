import { DeadLetterRepository } from "./DeadLetterRepository.js";
import { ParsedRecordRepository } from "./ParsedRecordRepository.js";
import { RubbishLogRepository } from "./RubbishLogRepository.js";
import { TemplateRepository } from "./TemplateRepository.js";
import { SchemaMigrationRepository } from "./SchemaMigrationRepository.js";
import { JobLogRepository } from "./JobLogRepository.js";
import { OutputPartRepository } from "./OutputPartRepository.js";
import { PendingArchiveEntryRepository } from "./PendingArchiveEntryRepository.js";
import { JobRepository } from "./JobRepository.js";



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
   */
  constructor() {
    this.jobs = new JobRepository();
    this.deadLetters = new DeadLetterRepository();
    this.outputParts = new OutputPartRepository();
    this.pendingArchiveEntries = new PendingArchiveEntryRepository();
    this.parsedRecords = new ParsedRecordRepository();
    this.rubbishLogs = new RubbishLogRepository();
    this.templates = new TemplateRepository();
    this.schemaMigrations = new SchemaMigrationRepository();
    this.jobLogs = new JobLogRepository();
  }
}
