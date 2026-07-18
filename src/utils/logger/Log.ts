import Config from "../../config/system-config/Config.js";

const config = Config.getInstance();

export interface LogContext {
  job_id?: string;
  [key: string]: any;
}

export class Logger 
{
  private service: string;
  private lokiEnabled: boolean;

  constructor(service: string) 
{
    this.service = service;
    this.lokiEnabled = !!(config.settings.LOKI_HOST && config.settings.LOKI_USERNAME && config.settings.LOKI_PASSWORD);
  }

  private format(level: string, message: string, context: LogContext = {}): string 
{
    const logEntry = {
      timestamp: new Date().toISOString(),
      level,
      service: this.service,
      message,
      ...context,
    };
    return JSON.stringify(logEntry);
  }

  private async sendToLoki(level: string, message: string, context: LogContext = {}): Promise<void> 
{
    if (!this.lokiEnabled) 
{
      return;
    }

    try 
{
      const timestamp = Date.now() * 1000000;
      
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
        signal: AbortSignal.timeout(10000),
      });

      if (!response.ok) 
{
        console.error("loki_send_failed", { status: response.status, statusText: response.statusText });
      }
    }
 catch (error) 
{
      console.error("loki_send_error", { error: String(error) });
    }
  }

  info(message: string, context: LogContext = {}): void 
{
    const formatted = this.format("info", message, context);
    console.log(formatted);
    this.sendToLoki("info", message, context).catch(() => 
{});
  }

  warn(message: string, context: LogContext = {}): void 
{
    const formatted = this.format("warn", message, context);
    console.warn(formatted);
    this.sendToLoki("warn", message, context).catch(() => 
{});
  }

  error(message: string, context: LogContext = {}, error?: Error): void 
{
    const entry = this.format("error", message, {
      ...context,
      ...(error ? { error: error.message, stack: error.stack } : {}),
    });
    console.error(entry);
    this.sendToLoki("error", message, {
      ...context,
      ...(error ? { error: error.message, stack: error.stack } : {}),
    }).catch(() => 
{});
  }

  debug(message: string, context: LogContext = {}): void 
{
    if (process.env.LOG_LEVEL === "debug") 
{
      const formatted = this.format("debug", message, context);
      console.log(formatted);
      this.sendToLoki("debug", message, context).catch(() => 
{});
    }
  }
}

export function createLogger(service: string): Logger 
{
  return new Logger(service);
}
