import ParseJob, {
  type IParseJob,
  type ParseJobAttributes,
  type ParseJobCreationAttributes,
} from "./ParseJob.js";
import DeadLetter, {
  type IDeadLetter,
  type DeadLetterAttributes,
  type DeadLetterCreationAttributes,
} from "./DeadLetter.js";
import OutputPart, {
  type IOutputPart,
  type OutputPartAttributes,
  type OutputPartCreationAttributes,
} from "./OutputPart.js";
import PendingArchiveEntry, {
  type IPendingArchiveEntry,
  type PendingArchiveEntryAttributes,
  type PendingArchiveEntryCreationAttributes,
} from "./PendingArchiveEntry.js";
import ParsedRecord, {
  type IParsedRecord,
  type ParsedRecordAttributes,
  type ParsedRecordCreationAttributes,
} from "./ParsedRecord.js";
import RubbishLog, {
  type IRubbishLog,
  type RubbishLogAttributes,
  type RubbishLogCreationAttributes,
} from "./RubbishLog.js";
import Template, {
  type ITemplate,
  type TemplateAttributes,
  type TemplateCreationAttributes,
} from "./Template.js";
import SchemaMigration, {
  type ISchemaMigration,
  type SchemaMigrationAttributes,
  type SchemaMigrationCreationAttributes,
} from "./SchemaMigration.js";

export {
  ParseJob,
  DeadLetter,
  OutputPart,
  PendingArchiveEntry,
  ParsedRecord,
  RubbishLog,
  Template,
  SchemaMigration,
};

export type {
  IParseJob,
  ParseJobAttributes,
  ParseJobCreationAttributes,
  IDeadLetter,
  DeadLetterAttributes,
  DeadLetterCreationAttributes,
  IOutputPart,
  OutputPartAttributes,
  OutputPartCreationAttributes,
  IPendingArchiveEntry,
  PendingArchiveEntryAttributes,
  PendingArchiveEntryCreationAttributes,
  IParsedRecord,
  ParsedRecordAttributes,
  ParsedRecordCreationAttributes,
  IRubbishLog,
  RubbishLogAttributes,
  RubbishLogCreationAttributes,
  ITemplate,
  TemplateAttributes,
  TemplateCreationAttributes,
  ISchemaMigration,
  SchemaMigrationAttributes,
  SchemaMigrationCreationAttributes,
};

export interface DatabaseModels {
  ParseJob: typeof ParseJob;
  DeadLetter: typeof DeadLetter;
  OutputPart: typeof OutputPart;
  PendingArchiveEntry: typeof PendingArchiveEntry;
  ParsedRecord: typeof ParsedRecord;
  RubbishLog: typeof RubbishLog;
  Template: typeof Template;
  SchemaMigration: typeof SchemaMigration;
}
