import { settings } from "@shared/Settings.js";

export interface LogContext {
  job_id?: string;
  [key: string]: unknown;
}

// ---------------------------------------------------------------------------
// Loki micro-batching transport
// Accumulates log values across all Logger instances and flushes them in a
// single HTTP request every LOKI_FLUSH_INTERVAL_MS (or when the buffer
// reaches LOKI_MAX_BATCH_SIZE entries). This replaces the previous pattern
// of one HTTP fetch per log entry, which caused connection exhaustion and
// 10-second timeout errors under moderate log volume.
// ---------------------------------------------------------------------------
const LOKI_FLUSH_INTERVAL_MS = 500;
const LOKI_SEND_TIMEOUT_MS = 3_000;
const LOKI_MAX_BATCH_SIZE = 200;
const LOKI_MAX_BUFFER_SIZE = 5_000;
const LOKI_CIRCUIT_BREAK_THRESHOLD = 5;
const LOKI_CIRCUIT_COOLDOWN_MS = 30_000;

interface LokiValue {
  stream: Record<string, string>;
  ts: string;          // nanosecond timestamp string
  line: string;        // serialized log entry
}

const lokiBuffer: LokiValue[] = [];
let lokiFlushTimer: ReturnType<typeof setTimeout> | undefined;
let lokiEnabled = false;
let lokiConsecutiveFailures = 0;
let lokiCircuitOpenUntil = 0;

function scheduleLokiFlush(): void {
  if (lokiFlushTimer !== undefined) return;
  lokiFlushTimer = setTimeout(() => {
    lokiFlushTimer = undefined;
    flushLoki().catch(() => {});
  }, LOKI_FLUSH_INTERVAL_MS);
  if (typeof lokiFlushTimer === "object" && (lokiFlushTimer as unknown as { unref?: () => void }).unref) {
    (lokiFlushTimer as unknown as { unref: () => void }).unref(); // don't keep process alive
  }
}

async function flushLoki(): Promise<void> {
  if (!lokiEnabled || lokiBuffer.length === 0) return;

  const now = Date.now();
  if (now < lokiCircuitOpenUntil) {
    // Circuit open: Loki is down. Drop this batch and keep console logging.
    lokiBuffer.splice(0, LOKI_MAX_BATCH_SIZE);
    return;
  }

  const batch = lokiBuffer.splice(0, LOKI_MAX_BATCH_SIZE);

  // Group by (service, level, job_id) to minimize stream cardinality
  const streamMap = new Map<string, { stream: Record<string, string>; values: [string, string][] }>();
  for (const entry of batch) {
    const key = JSON.stringify(entry.stream);
    let s = streamMap.get(key);
    if (!s) {
      s = { stream: entry.stream, values: [] };
      streamMap.set(key, s);
    }
    s.values.push([entry.ts, entry.line]);
  }

  const body = JSON.stringify({ streams: Array.from(streamMap.values()) });
  try {
    const resp = await fetch(`${settings.LOKI_HOST}/loki/api/v1/push`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Basic ${btoa(`${settings.LOKI_USERNAME}:${settings.LOKI_PASSWORD}`)}`,
      },
      body,
      signal: AbortSignal.timeout(LOKI_SEND_TIMEOUT_MS),
    });
    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status}`);
    }
    lokiConsecutiveFailures = 0;
  } catch (err) {
    lokiConsecutiveFailures++;
    console.error("loki_send_error", { error: String(err) });
    if (lokiConsecutiveFailures >= LOKI_CIRCUIT_BREAK_THRESHOLD) {
      lokiCircuitOpenUntil = now + LOKI_CIRCUIT_COOLDOWN_MS;
      console.warn("loki_circuit_open", { cooldown_ms: LOKI_CIRCUIT_COOLDOWN_MS, until: new Date(lokiCircuitOpenUntil).toISOString() });
    }
    // Drop the batch rather than re-queuing — retries cause unbounded buffer growth
  }

  // If the buffer still has entries (was > LOKI_MAX_BATCH_SIZE), schedule another flush
  if (lokiBuffer.length > 0) {
    scheduleLokiFlush();
  }
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
   * Constructs a new Logger instance.
   * @param service - The service
   */
  constructor(service: string) {
    this.service = service;
    // Lazily enable Loki on first Logger construction once settings are loaded
    if (!lokiEnabled && settings.LOKI_HOST && settings.LOKI_USERNAME && settings.LOKI_PASSWORD) {
      lokiEnabled = true;
    }
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

  private enqueueToLoki(level: string, message: string, context: LogContext = {}): void {
    if (!lokiEnabled) return;
    if (lokiBuffer.length >= LOKI_MAX_BUFFER_SIZE) {
      // Backpressure: Loki is down/slow and the backlog would OOM the container.
      // Keep console logging; just drop this entry rather than crashing.
      return;
    }
    lokiBuffer.push({
      stream: {
        service: this.service,
        level,
        ...(context.job_id ? { job_id: String(context.job_id) } : {}),
      },
      ts: String(Date.now() * 1_000_000), // nanoseconds
      line: this.format(level, message, context),
    });
    if (lokiBuffer.length >= LOKI_MAX_BATCH_SIZE) {
      clearTimeout(lokiFlushTimer);
      lokiFlushTimer = undefined;
      flushLoki().catch(() => {});
    } else {
      scheduleLokiFlush();
    }
  }

    /**
   * Logs information about the operation
   * @param message - The message
   * @param context - The context object
   */
  info(message: string, context: LogContext = {}): void {
    console.log(this.format("info", message, context));
    this.enqueueToLoki("info", message, context);
  }

    /**
   * Warns about the operation
   * @param message - The message
   * @param context - The context object
   */
  warn(message: string, context: LogContext = {}): void {
    console.warn(this.format("warn", message, context));
    this.enqueueToLoki("warn", message, context);
  }

    /**
   * Logs an error for the operation
   * @param message - The message
   * @param context - The context object
   * @param error - The error that occurred
   */
  error(message: string, context: LogContext = {}, error?: Error): void {
    const merged = { ...context, ...(error ? { error: error.message, stack: error.stack } : {}) };
    console.error(this.format("error", message, merged));
    this.enqueueToLoki("error", message, merged);
  }

    /**
   * Debugs the operation
   * @param message - The message
   * @param context - The context object
   */
  debug(message: string, context: LogContext = {}): void {
    if (process.env.LOG_LEVEL === "debug") {
      console.log(this.format("debug", message, context));
      this.enqueueToLoki("debug", message, context);
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
