import { InstantiationError } from "@errors/InstantiationError.js";
import { CustomRouter } from "@utils/router/CustomRouter.js";
import { JobServiceController } from "@service/job_service/controllers/JobServiceController.js";
import { JobServiceControllerImpl } from "@service/job_service/controllers/impl/JobServiceControllerImpl.js";

/**
 * Router for the Job Service.
 *
 * Follows the Reviro routing convention: singleton class extending
 * CustomRouter, controller injection, route definition in initializeRoutes().
 * No permission middleware is applied.
 */
export class JobServiceRouter extends CustomRouter {
  private static instance: JobServiceRouter;
  private readonly controller: JobServiceController;

  constructor(controller: JobServiceController, enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate JobServiceRouter directly. Use getInstance()");
    }
    super();
    this.controller = controller;
    this.initializeRoutes();
  }

  public static getInstance(): JobServiceRouter {
    if (!JobServiceRouter.instance) {
      JobServiceRouter.instance = new JobServiceRouter(JobServiceControllerImpl.getInstance(), Enforce);
    }
    return JobServiceRouter.instance;
  }

  private initializeRoutes(): void {
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
  }
}

function Enforce(): void {}
