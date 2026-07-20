import { Request, Response, NextFunction } from "express";
import { InstantiationError } from "@errors/InstantiationError.js";
import { CustomError } from "@errors/CustomError.js";
import { JobServiceController } from "@service/job_service/controllers/JobServiceController.js";
import { JobServiceService } from "@service/job_service/services/JobServiceService.js";
import { JobServiceServiceImpl } from "@service/job_service/services/impl/JobServiceServiceImpl.js";
import {
  ICreateJobRequest,
  IProvidePasswordRequest,
  IMarkFailedRequest,
  IRetryJobRequest,
} from "@service/job_service/io/IJob.js";
import { ServiceResponse } from "@utils/response/ServiceResponse.js";

/**
 * Singleton implementation of the Job Service HTTP controller.
 *
 * Thin controller: extracts request data, delegates to the service,
 * and formats the HTTP response. Errors are forwarded via next(error).
 */
export class JobServiceControllerImpl implements JobServiceController {
  private static instance: JobServiceControllerImpl;
  private readonly service: JobServiceService;

  private constructor(service: JobServiceService, enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate JobServiceControllerImpl directly. Use getInstance()");
    }
    this.service = service;
  }

  public static getInstance(): JobServiceControllerImpl {
    if (!JobServiceControllerImpl.instance) {
      JobServiceControllerImpl.instance = new JobServiceControllerImpl(JobServiceServiceImpl.getInstance(), Enforce);
    }
    return JobServiceControllerImpl.instance;
  }

  public handleSuccessResponse(res: Response, outcome: unknown, pagination: boolean = false, status: number = 200): void {
    const serviceResponse = new ServiceResponse(res).setStatus(status);

    if (pagination) {
      const { data, pages } = outcome as { data: unknown[]; pages: { current: number; total: number; count: number } };
      serviceResponse.setOutcome(data, pages);
    } else {
      serviceResponse.setOutcome(outcome);
    }

    serviceResponse.send();
  }

  public createJob = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const request: ICreateJobRequest = {
        source_type: req.body.source_type,
        source_ref: req.body.source_ref,
        field_spec: req.body.field_spec,
        batch_id: req.body.batch_id,
        column_map: req.body.column_map,
      };
      const result = await this.service.createJob(request);
      this.handleSuccessResponse(res, result, false, 202);
    } catch (err) {
      next(err);
    }
  };

  public findStuckJobs = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const thresholdMinutes = parseInt(req.query.minutes as string) || 15;
      const result = await this.service.findStuckJobs(thresholdMinutes);
      this.handleSuccessResponse(res, result);
    } catch (err) {
      next(err);
    }
  };

  public getJob = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const result = await this.service.getJob(String(req.params.job_id));
      if (!result) {
        next(new CustomError("Job not found", "NOT_FOUND", 404));
        return;
      }
      this.handleSuccessResponse(res, result);
    } catch (err) {
      next(err);
    }
  };

  public getBatchJobs = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const result = await this.service.getBatchJobs(String(req.params.batch_id));
      this.handleSuccessResponse(res, result);
    } catch (err) {
      next(err);
    }
  };

  public providePassword = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const request: IProvidePasswordRequest = { password: req.body.password };
      await this.service.providePassword(String(req.params.job_id), request);
      res.status(204).send();
    } catch (err) {
      next(err);
    }
  };

  public releaseHold = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      await this.service.releaseHold(String(req.params.job_id));
      res.status(204).send();
    } catch (err) {
      next(err);
    }
  };

  public markFailed = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const request: IMarkFailedRequest = { reason: req.body.reason };
      await this.service.markFailed(String(req.params.job_id), request);
      res.status(204).send();
    } catch (err) {
      next(err);
    }
  };

  public retryJob = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const request: IRetryJobRequest = { target_status: req.body.target_status };
      await this.service.retryJob(String(req.params.job_id), request);
      res.status(204).send();
    } catch (err) {
      next(err);
    }
  };
}

function Enforce(): void {}
