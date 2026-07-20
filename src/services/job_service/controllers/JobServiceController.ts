import { Request, Response, NextFunction } from "express";

/**
 * Interface for the Job Service HTTP controller.
 *
 * Each handler is responsible for one route and delegates persistence
 * and queue operations to the underlying services.
 */
export interface JobServiceController {
  createJob(req: Request, res: Response, next: NextFunction): Promise<void>;
  findStuckJobs(req: Request, res: Response, next: NextFunction): Promise<void>;
  getJob(req: Request, res: Response, next: NextFunction): Promise<void>;
  getBatchJobs(req: Request, res: Response, next: NextFunction): Promise<void>;
  providePassword(req: Request, res: Response, next: NextFunction): Promise<void>;
  releaseHold(req: Request, res: Response, next: NextFunction): Promise<void>;
  markFailed(req: Request, res: Response, next: NextFunction): Promise<void>;
  retryJob(req: Request, res: Response, next: NextFunction): Promise<void>;
}
