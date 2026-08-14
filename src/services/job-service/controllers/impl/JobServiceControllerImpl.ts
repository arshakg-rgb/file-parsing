import { Request, Response, NextFunction } from "express";
import { InstantiationError } from "@errors/InstantiationError.js";
import { ValidationError } from "@errors/ValidationError.js";
import { JobServiceController } from "@service/job-service/controllers/JobServiceController.js";
import { JobService } from "@service/job-service/services/JobService.js";
import { JobServiceImpl  } from "@service/job-service/services/impl/JobServiceImpl.js";
import {
  ICreateJobRequest,
  ICreateJobResponse,
  IJobResponse,
  IJobHeadersResponse,
  IStuckJobsResponse,
  IStatusesResponse,
  IProvidePasswordRequest,
  IMarkFailedRequest,
  IRetryJobRequest, 
  IJobLogEntry, 
  IUploadCsvRequest, 
  IUploadCsvResponse, 
  IUploadAndCreateJobRequest, 
  IDownloadCsvResponse,
} from "@service/job-service/io/IJob.js";
import { ServiceResponse } from "@utils/response/ServiceResponse.js";
import { HttpError } from "@errors/HttpError.js";
import {constants as HttpStatuses} from "http2";
import {IPaginatedResult} from "@utils/pagination/app/io/IPagination";

/**
 * Singleton implementation of the Job Service HTTP controller.
 *
 * Thin controller: extracts request data, delegates to the service,
 * and formats the HTTP response. Errors are forwarded via next(error).
 */
export class JobControllerImpl implements JobServiceController
{
  /**
   * The singleton instance of `JobControllerImpl`.
   * @private
   */

  private static instance: JobControllerImpl;

  /**
   * The JobServiceService instance.
   * @private
   */

  private readonly service: JobService;

  /**
   * Constructs a new JobControllerImpl instance.
   *
   * @param service - The JobService instance to use for region operations.
   * @param enforce - A function to enforce the Singleton pattern.
   * @throws Error if instantiated directly.
   */

  private constructor(service: JobService, enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE,"Cannot instantiate JobControllerImpl directly. Use getInstance()");
    }

    this.service = service;
  }

  /**
   * Returns the singleton instance of JobControllerImpl.
   *
   * @returns The singleton instance of JobControllerImpl.
   */

  public static getInstance(): JobControllerImpl
  {
    if (!JobControllerImpl.instance)
    {
      JobControllerImpl.instance = new JobControllerImpl(JobServiceImpl.getInstance(), Enforce);
    }

    return JobControllerImpl.instance;
  }

  /**
   * @param req - The request object.
   * @param res - The response object.
   * @param next - The next middleware function.
   */

  public createJob: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const request: ICreateJobRequest = {
        source_type: req.body.source_type,
        source_ref: req.body.source_ref,
        field_spec: req.body.field_spec,
        batch_id: req.body.batch_id,
        column_map: req.body.column_map,
        filename: req.body.filename,
      };

      const result: ICreateJobResponse = await this.service.createJob(request);

      this.handleSuccessResponse(res, result, false, 202);
    }
    catch (err) {
      next(err);
    }
  };

  /**
   * @param req - The request object.
   * @param res - The response object.
   * @param next - The next middleware function.
   */

  public uploadAndCreateJob: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      if (!req.file)
      {
        throw new ValidationError(ValidationError.INPUT, "file is required");
      }

      const parseJsonField = (value: unknown): unknown =>
      {
        if (typeof value !== "string") return value;
        try { return JSON.parse(value); } catch { return value; }
      };

      const request: IUploadAndCreateJobRequest = {
        source_buffer: req.file.buffer,
        mimetype: req.file.mimetype,
        filename: req.file.originalname,
        field_spec: parseJsonField(req.body.field_spec),
        column_map: req.body.column_map ? parseJsonField(req.body.column_map) : undefined,
        batch_id: req.body.batch_id,
      };

      const result: ICreateJobResponse = await this.service.uploadAndCreateJob(request);

      this.handleSuccessResponse(res, result, false, 202);
    }
    catch (err) {
      next(err);
    }
  };

  /**
   * @param req - The request object.
   * @param res - The response object.
   * @param next - The next middleware function.
   */

  public findStuckJobs: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const thresholdMinutes: number = parseInt(req.query.minutes as string) || 15;
      const result: IStuckJobsResponse = await this.service.findStuckJobs(thresholdMinutes);

      this.handleSuccessResponse(res, result);
    }
    catch (err)
    {
      next(err);
    }
  };

  /**
   * @param req - The request object.
   * @param res - The response object.
   * @param next - The next middleware function.
   */

  public getJob: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const result: IJobResponse = await this.service.getJob(String(req.params.job_id));

      if (!result)
      {
        next(new HttpError(HttpError.NOT_FOUND, "NOT_FOUND", ));
        return;
      }

      this.handleSuccessResponse(res, result);
    }
    catch (err)
    {
      next(err);
    }
  };

  /**
   * @param req - The request object.
   * @param res - The response object.
   * @param next - The next middleware function.
   */

  public getJobHeaders: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const result: IJobHeadersResponse | null = await this.service.getJobHeaders(String(req.params.job_id));

      if (!result)
      {
        next(new HttpError(HttpError.NOT_FOUND, "NOT_FOUND", ));
        return;
      }

      this.handleSuccessResponse(res, result);
    }
    catch (err)
    {
      next(err);
    }
  };

  /**
   * @param req - The request object.
   * @param res - The response object.
   * @param next - The next middleware function.
   */

  public getBatchJobs: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const limit: number | undefined = req.query.limit ? Number(req.query.limit) : undefined;
      const offset: number | undefined = req.query.offset ? Number(req.query.offset) : undefined;
      const result: IJobResponse[] = await this.service.getBatchJobs(String(req.params.batch_id), limit, offset);
      this.handleSuccessResponse(res, result);
    }
    catch (err)
    {
      next(err);
    }
  };

  public getAllJobs: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const raw = req.query.statuses;
      const statuses: string[] | undefined = typeof raw === "string"
        ? raw.split(",").map((s) => s.trim()).filter(Boolean)
        : Array.isArray(raw)
        ? raw.map(String).map((s) => s.trim()).filter(Boolean)
        : undefined;

      const limit: number | undefined = req.query.limit ? Number(req.query.limit) : undefined;
      const offset: number | undefined = req.query.offset ? Number(req.query.offset) : undefined;

      const result: IJobResponse[] = await this.service.getAllJobs(statuses, limit, offset);
      this.handleSuccessResponse(res, result);
    }
    catch (err)
    {
      next(err);
    }
  };

  public getAllStatuses: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const result: IStatusesResponse = await this.service.getAllStatuses();
      this.handleSuccessResponse(res, result);
    }
    catch (err)
    {
      next(err);
    }
  };

  /**
   * @param req - The request object.
   * @param res - The response object.
   * @param next - The next middleware function.
   */

  public providePassword: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const request: IProvidePasswordRequest = { password: req.body.password };

      await this.service.providePassword(String(req.params.job_id), request);

      this.handleSuccessResponse(res, {}, false, 204);
    }
    catch (err)
    {
      next(err);
    }
  };

  /**
   * @param req - The request object.
   * @param res - The response object.
   * @param next - The next middleware function.
   */

  public releaseHold: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      await this.service.releaseHold(String(req.params.job_id));

      this.handleSuccessResponse(res, {}, false, 204);
    }
    catch (err)
    {
      next(err);
    }
  };

  /**
   * @param req - The request object.
   * @param res - The response object.
   * @param next - The next middleware function.
   */

  public markFailed: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const request: IMarkFailedRequest = { reason: req.body.reason };

      await this.service.markFailed(String(req.params.job_id), request);

      this.handleSuccessResponse(res, {}, false, 204);
    }
    catch (err)
    {
      next(err);
    }
  };

  /**
   * @param req - The request object.
   * @param res - The response object.
   * @param next - The next middleware function.
   */

  public retryJob: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const request: IRetryJobRequest = {
        target_status: req.body.target_status,
        field_spec: req.body.field_spec,
        column_map: req.body.column_map,
      };
      await this.service.retryJob(String(req.params.job_id), request);

      this.handleSuccessResponse(res, {}, false, 204);
    }
    catch (err)
    {
      next(err);
    }
  };

  /**
   * @param req - The request object.
   * @param res - The response object.
   * @param next - The next middleware function.
   */

  public getJobLogs: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const result: IJobLogEntry[] = await this.service.getJobLogs(String(req.params.job_id));

      this.handleSuccessResponse(res, result);
    }
    catch (err)
    {
      next(err);
    }
  };

  /**
   * Handles the service response.
   *
   * @param res - The response object.
   * @param outcome - The outcome of the service operation.
   * @param pagination -Whether to paginate the response.
   * @param status - The HTTP status code to set.
   */

  public uploadCsv: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const request: IUploadCsvRequest = {
        destination_url: req.body.destination_url,
      };

      const result: IUploadCsvResponse = await this.service.uploadCsv(String(req.params.job_id), request);

      this.handleSuccessResponse(res, result);
    }
    catch (err)
    {
      next(err);
    }
  };

  /**
   * @param req - The request object.
   * @param res - The response object.
   * @param next - The next middleware function.
   */

  public downloadCsv: (req: Request, res: Response, next: NextFunction) => Promise<void> = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const result: IDownloadCsvResponse = await this.service.downloadCsv(String(req.params.job_id));

      res.redirect(307, result.download_url);
    }
    catch (err)
    {
      next(err);
    }
  };

  public handleSuccessResponse(res: Response, outcome: {}, pagination: boolean = false, status: number = HttpStatuses.HTTP_STATUS_OK): void
  {
    const serviceResponse: ServiceResponse = new ServiceResponse(res)
        .setStatus(status);

    if (pagination)
    {
      const { data, pages } = outcome as IPaginatedResult<any>;
      serviceResponse.setOutcome(data, pages);
    }
    else
    {
      serviceResponse.setOutcome(outcome);
    }

    serviceResponse.send();
  };
}

function Enforce(): void {}
