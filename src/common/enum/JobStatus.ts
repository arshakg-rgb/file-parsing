export enum JobStatus {
  CREATED = "Created",
  INGESTING = "Ingesting",
  NEEDS_PASSWORD = "Needs Password",
  DETECTING = "Detecting",
  PARSING = "Parsing",
  MERGING_OUTPUT = "Merging Output",
  SAVING_TO_DATABASE = "Saving to Database",
  REPORTING = "Reporting",
  COMPLETED = "Completed",
  PARTIAL = "Partial",
  ON_HOLD = "On Hold",
  FAILED = "Failed",
}
