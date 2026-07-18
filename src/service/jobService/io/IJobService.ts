export interface JobEvent {
  jobId: string;
  eventType: string;
  timestamp: string;
  data?: unknown;
}

export interface JobServiceConfig {
  queueUrl: string;
  maxMessages: number;
  waitTime: number;
}

export interface IJobService {
  initialize(): Promise<void>;
  startEventConsumer(): Promise<void>;
  handleEvent(event: JobEvent): Promise<void>;
  shutdown(): Promise<void>;
}
