import { Request, Response, NextFunction } from "express";
import { CustomError } from "../../errors/CustomError.js";

export function error404Handler(req: Request, res: Response): void {
  res.status(404).json({
    error: {
      message: "Resource not found",
      code: "NOT_FOUND",
      path: req.path,
      method: req.method,
    },
  });
}

export function errorPageHandler(
  err: Error,
  req: Request,
  res: Response,
  next: NextFunction
): void {
  console.error("Error:", err);

  if (err instanceof CustomError) {
    res.status(err.statusCode).json({
      error: {
        message: err.message,
        code: err.code,
        details: err.details,
      },
    });
  } else {
    res.status(500).json({
      error: {
        message: "Internal server error",
        code: "INTERNAL_ERROR",
      },
    });
  }
}
