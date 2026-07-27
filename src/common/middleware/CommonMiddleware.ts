import { NextFunction, Request, Response } from "express";
import dotenv from "dotenv";
import {HttpError} from "@errors/HttpError.js";
import {CustomError} from "@errors/CustomError.js";
import { ServerError } from "@errors/ServerError.js";
import {ErrorInfo} from "@errors/io/ErrorInfo.js";
import Config from "@config/system-config/Config.js";

dotenv.config();

/**
 * Middleware to handle 404 errors.
 * Forwards a `HttpError` with a `NOT_FOUND` code and a message indicating the requested path was not found.
 *
 * @param req - The Express impl object.
 * @param res - The Express response object.
 * @param next - The next middleware function in the stack.
 */
export function error404Handler(req: Request, res: Response, next: NextFunction): void
{
  next(new HttpError(HttpError.NOT_FOUND, `page for '${req.path}' not found`));
}

/**
 * Middleware to handle errors and format the error response.
 * Converts known errors to `ServerError` or `HttpError` and formats the response JSON.
 *
 * @param err - The error object.
 * @param req - The Express impl object.
 * @param res - The Express response object.
 */

export function errorPageHandler(err: CustomError, req: Request, res: Response): void
{
  if ("ETIMEDOUT" === err.code)
  {
    err = new ServerError(ServerError.REQUEST_TIMEOUT, "Internal server impl timeout");
  }

  const isProduction: boolean = process.env.NODE_ENV === "production";

  interface ErrorResponseBody
  {
    success?: boolean;
    errors?: ErrorInfo[];
    data?: Record<string, any>;
  }

  const errorResponse: ErrorResponseBody = { success: false };
  const message: string | null = err.message ? err.message.replace(new RegExp("\"", "g"), "'") : null;

  let errorForwarded: string[] = [Config.getInstance().appConfig.name];

  const forwarded: string[] | undefined = (err as unknown as { forwarded?: string[] }).forwarded;

  if (forwarded)
  {
    errorForwarded = errorForwarded.concat(forwarded);
  }

  const errorInfo: ErrorInfo = {
    code: err.code,
    name: err.name,
    forwarded: errorForwarded,
    service: Config.getInstance().appConfig.name,
    message: message,
    fields: err.fields,
    info: err.info
  };

  if (!isProduction && typeof err.stack === "string")
  {
    errorInfo.stack = err.stack.split("\n");
  }
  else if (!isProduction && Array.isArray(err.stack))
  {
    errorInfo.stack = err.stack;
  }

  errorResponse.errors = [errorInfo];

  if (err.data)
  {
    errorResponse.data = err.data;
  }

  const status = (err as unknown as { status?: number }).status;

  res.status(status || 500);
  res.json(errorResponse);
}

/**
 * Middleware to check if the HTTP method is supported.
 * Forwards a `HttpError` with a `NOT_SUPPORTED_METHOD` code if the method is not supported.
 *
 * @param methods - The list of supported HTTP methods.
 * @returns A middleware function that checks the HTTP method.
 */
export function supportedHttpMethods(...methods: string[]): (req: Request, res: Response, next: NextFunction) => void
{
  return (req: Request, _res: Response, next: NextFunction) =>
  {
    if (methods.includes(req.method))
    {
      next();
    }
    else
    {
      next(new HttpError(HttpError.NOT_SUPPORTED_METHOD, `Endpoint does not support http '${req.method}' method.`));
    }
  };
}
