import { NextFunction, Request, Response } from "express";

/**
 * Base controller interface.
 *
 * Domain controllers may extend this interface and add feature-specific
 * handlers. The success-response helper enforces a uniform response envelope.
 */
export interface Controller {
  createHandler(req: Request, res: Response, next: NextFunction): Promise<void>;
  updateHandler(req: Request, res: Response, next: NextFunction): Promise<void>;
  deleteHandler(req: Request, res: Response, next: NextFunction): Promise<void>;
  fetchByIdHandler(req: Request, res: Response, next: NextFunction): Promise<void>;
  fetchAllHandler(req: Request, res: Response, next: NextFunction): Promise<void>;
  handleSuccessResponse(res: Response, outcome: unknown, pagination?: boolean, status?: number): void;
}
