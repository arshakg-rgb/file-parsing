export interface LogContext {
  job_id?: string;
  [key: string]: any;
}

export class Logger {
  private service: string;

  constructor(service: string) {
    this.service = service;
  }

  private format(level: string, message: string, context: LogContext = {}): string {
    const logEntry = {
      timestamp: new Date().toISOString(),
      level,
      service: this.service,
      message,
      ...context,
    };
    return JSON.stringify(logEntry);
  }

  info(message: string, context: LogContext = {}): void {
    console.log(this.format("info", message, context));
  }

  warn(message: string, context: LogContext = {}): void {
    console.warn(this.format("warn", message, context));
  }

  error(message: string, context: LogContext = {}, error?: Error): void {
    const entry = this.format("error", message, {
      ...context,
      ...(error ? { error: error.message, stack: error.stack } : {}),
    });
    console.error(entry);
  }

  debug(message: string, context: LogContext = {}): void {
    if (process.env.LOG_LEVEL === "debug") {
      console.log(this.format("debug", message, context));
    }
  }
}

export function createLogger(service: string): Logger {
  return new Logger(service);
}
