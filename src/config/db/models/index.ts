import type { Sequelize } from "sequelize";
import { initParseJobModel, ParseJob } from "./ParseJob.js";
import { initDeadLetterModel, DeadLetter } from "./DeadLetter.js";
import { initOutputPartModel, OutputPart } from "./OutputPart.js";
import { initPendingArchiveEntryModel, PendingArchiveEntry } from "./PendingArchiveEntry.js";
import { initParsedRecordModel, ParsedRecord } from "./ParsedRecord.js";
import { initRubbishLogModel, RubbishLog } from "./RubbishLog.js";
import { initTemplateModel, Template } from "./Template.js";
import { initSchemaMigrationModel, SchemaMigration } from "./SchemaMigration.js";

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

export function initModels(sequelize: Sequelize): DatabaseModels {
  return {
    ParseJob: initParseJobModel(sequelize),
    DeadLetter: initDeadLetterModel(sequelize),
    OutputPart: initOutputPartModel(sequelize),
    PendingArchiveEntry: initPendingArchiveEntryModel(sequelize),
    ParsedRecord: initParsedRecordModel(sequelize),
    RubbishLog: initRubbishLogModel(sequelize),
    Template: initTemplateModel(sequelize),
    SchemaMigration: initSchemaMigrationModel(sequelize),
  };
}
