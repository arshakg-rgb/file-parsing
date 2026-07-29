import path from "node:path";
import pino, { type Logger } from "pino";
import dotenv from "dotenv";
import process from "node:process";
import type { LokiOptions } from "pino-loki";
import {Constants} from "@common/io/Constants.js";

dotenv.config();

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

const isProduction: boolean = false;

let transport: any = undefined;
if (isProduction)
{
  transport = pino.transport<LokiOptions>({
    target: "pino-loki",
    options: {
      batching: {
        interval: 5,
      },
      labels: {
        app: requireEnv("APP_NAME"),
        env: process.env.NODE_ENV ?? Constants.ENVIRONMENTS.DEVELOPMENT,
      },
      host: requireEnv("LOKI_HOST"),
      basicAuth: {
        username: requireEnv("LOKI_USERNAME"),
        password: requireEnv("LOKI_PASSWORD"),
      },
    },
  });
}

/**
 * Factory function to create a pino logger with a specified name.
 * @param loggerName - The name of the logger.
 * @returns A pino.Logger instance.
 */

function logFactory(loggerName: string): pino.Logger
{
  return pino({
    name: loggerName,
    formatters: {
      level: (label: string, number: number) => ({ level: number })
    },
    base: undefined,
    timestamp: () => `,"timestamp":"${new Date().toISOString()}"`
  }, isProduction ? transport : undefined);
}

/**
 * Creates a logger instance with a specified name.
 * @param name - The name of the logger or the NodeModule.
 * @returns A pino.Logger instance.
 */

export function createLogger(name?: string | NodeModule): pino.Logger
{
  const loggerName: string =
      typeof name === "string"
          ? name
          : name?.filename
              ? name.filename.split(path.sep).slice(-2).join(path.sep)
              : "default";

  return logFactory(loggerName);
}


const logger: pino.Logger = createLogger(module);

/**
 * Handles uncaught exceptions by logging the error.
 *
 * @param err - The error object.
 */

process.on("uncaughtException", (err: Error) =>
{
  if (err && err.stack)
  {
    logger.error(err, err.message);
  }
  else
  {
    logger.error("uncaughtException, no stack trace available");
  }
});

/**
 * Handles unhandled promise rejections by logging the error.
 *
 * @param err - The error object.
 */

process.on("unhandledRejection", (err: Error) =>
{
  if (err && err.stack)
  {
    logger.error(err, err.message);
  }
  else
  {
    logger.error("unhandledRejection, no stack trace available");
  }
});
