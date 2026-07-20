import { Request, Response } from "express";
import { InstantiationError } from "@errors/InstantiationError.js";
import { CustomRouter } from "@utils/router/CustomRouter.js";

/**
 * ApiRouter is responsible for api router operations.
 */
class ApiRouter extends CustomRouter {
    /**
   * Singleton instance
   * @private
   */
  private static instance: ApiRouter;

    /**
   * Constructs a new ApiRouter instance.
   */
  private constructor() {
    super();
  }

    /**
   * Gets the single instance of the ApiRouter class.
   * @returns The single instance of the class
   */
  public static getInstance(): ApiRouter {
    if (!ApiRouter.instance) {
      ApiRouter.instance = new ApiRouter();
    }
    return ApiRouter.instance;
  }

  /**
   * Initializes API routes.
   */
  public async initializeRoutes(): Promise<void> {
    this.route("/health")
      .get((_req: Request, res: Response) => {
        res.json({
          status: "healthy",
          timestamp: new Date().toISOString(),
        });
      });
  }
}

export default ApiRouter;
