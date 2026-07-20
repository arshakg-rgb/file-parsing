export enum JobStatus {
  QUEUED = "queued",
  INGESTING = "ingesting",
  AWAITING_PASSWORD = "awaiting_password",
  DETECTING = "detecting",
  PARSING = "parsing",
  FINALIZING = "finalizing",
  LOADING = "loading",
  REPORTING = "reporting",
  DONE = "done",
  PARTIAL = "partial",
  HELD = "held",
  FAILED = "failed",
}
