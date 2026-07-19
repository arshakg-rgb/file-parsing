import Config from "@config/system-config/Config.js";

/**
 * The config
 */
const config = Config.getInstance();

export interface LogContext {
  job_id?: string;
  [key: string]: unknown;
}

/**
 * Logger is responsible for logger operations.
 */
export class Logger {
    /**
   * Service
   * @private
   */
  private service: string;
    /**
   * Loki Enabled
   * @private
   */
  private lokiEnabled: boolean;

    /**
   * Constructs a new Logger instance.
   * @param service - The service
   */
  constructor(service: string) {
    this.service = service;
    this.lokiEnabled = !!(config.settings.LOKI_HOST && config.settings.LOKI_USERNAME && config.settings.LOKI_PASSWORD);
  }

    /**
   * Formats the operation
   * @param level - The level
   * @param message - The message
   * @param context - The context object
   * @returns The string result
   */
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

    /**
   * Sends to loki
   * @param level - The level
   * @param message - The message
   * @param context - The context object
   */
  private async sendToLoki(level: string, message: string, context: LogContext = {}): Promise<void> {
    if (!this.lokiEnabled) {
      return;
    }

    try {
      // Loki expects nanosecond timestamps
      const timestamp = Date.now() * 1000000; // Convert to nanoseconds
      
      const logEntry = {
        streams: [
          {
            stream: {
              service: this.service,
              level,
              ...(context.job_id ? { job_id: context.job_id } : {}),
            },
            values: [
              [
                String(timestamp),
                this.format(level, message, context),
              ],
            ],
          },
        ],
      };

      const response = await fetch(`${config.settings.LOKI_HOST}/loki/api/v1/push`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Basic ${btoa(`${config.settings.LOKI_USERNAME}:${config.settings.LOKI_PASSWORD}`)}`,
        },
        body: JSON.stringify(logEntry),
        signal: AbortSignal.timeout(10000), // 10 second timeout
      });

      if (!response.ok) {
        console.error("loki_send_failed", { status: response.status, statusText: response.statusText });
      }
    } catch (error) {
      // Silently fail Loki errors to avoid disrupting application
      console.error("loki_send_error", { error: String(error) });
    }
  }

    /**
   * Logs information about the operation
   * @param message - The message
   * @param context - The context object
   */
  info(message: string, context: LogContext = {}): void {
    const formatted = this.format("info", message, context);
    console.log(formatted);
    this.sendToLoki("info", message, context).catch(() => {});
  }

    /**
   * Warns about the operation
   * @param message - The message
   * @param context - The context object
   */
  warn(message: string, context: LogContext = {}): void {
    const formatted = this.format("warn", message, context);
    console.warn(formatted);
    this.sendToLoki("warn", message, context).catch(() => {});
  }

    /**
   * Logs an error for the operation
   * @param message - The message
   * @param context - The context object
   * @param error - The error that occurred
   */
  error(message: string, context: LogContext = {}, error?: Error): void {
    const entry = this.format("error", message, {
      ...context,
      ...(error ? { error: error.message, stack: error.stack } : {}),
    });
    console.error(entry);
    this.sendToLoki("error", message, {
      ...context,
      ...(error ? { error: error.message, stack: error.stack } : {}),
    }).catch(() => {});
  }

    /**
   * Debugs the operation
   * @param message - The message
   * @param context - The context object
   */
  debug(message: string, context: LogContext = {}): void {
    if (process.env.LOG_LEVEL === "debug") {
      const formatted = this.format("debug", message, context);
      console.log(formatted);
      this.sendToLoki("debug", message, context).catch(() => {});
    }
  }
}

/**
 * Creates logger
 * @param service - The service
 * @returns The logger result
 */
export function createLogger(service: string): Logger {
  return new Logger(service);
}
