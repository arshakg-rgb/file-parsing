import { InstantiationError } from "@errors/InstantiationError.js";
import { CustomRouter } from "@utils/router/CustomRouter.js";
import { JobServiceController } from "@service/job-service/controllers/JobServiceController.js";
import { JobControllerImpl } from "@service/job-service/controllers/impl/JobServiceControllerImpl.js";

/**
 * Router for the Job Service.
 *
 * Follows the Reviro routing convention: singleton class extending
 * CustomRouter, controller injection, route definition in initializeRoutes().
 * No permission middleware is applied.
 */
export class JobServiceRouter extends CustomRouter
{
  private static instance: JobServiceRouter;
  private readonly controller: JobServiceController;

  constructor(controller: JobServiceController, enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE,"Cannot instantiate JobServiceRouter directly. Use getInstance()");
    }

    super();
    this.controller = controller;
    this.initializeRoutes();
  }

  public static getInstance(): JobServiceRouter
  {
    if (!JobServiceRouter.instance)
    {
      JobServiceRouter.instance = new JobServiceRouter(JobControllerImpl.getInstance(), Enforce);
    }

    return JobServiceRouter.instance;
  }

  private initializeRoutes(): void
  {
    this.route("/jobs")
      .post(this.controller.createJob);

    this.route("/jobs/stuck")
      .get(this.controller.findStuckJobs);

    this.route("/jobs/:job_id")
      .get(this.controller.getJob);

    this.route("/batches/:batch_id/jobs")
      .get(this.controller.getBatchJobs);

    this.route("/jobs/:job_id/password")
      .post(this.controller.providePassword);

    this.route("/jobs/:job_id/release-hold")
      .post(this.controller.releaseHold);

    this.route("/jobs/:job_id/fail")
      .post(this.controller.markFailed);

    this.route("/jobs/:job_id/retry")
      .post(this.controller.retryJob);

    this.route("/jobs/:job_id/logs")
      .get(this.controller.getJobLogs);

    this.route("/jobs/:job_id/upload-csv")
      .post(this.controller.uploadCsv);
  }
}

function Enforce(): void {}
