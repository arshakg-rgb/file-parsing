import { Router, Request, Response } from "express";
import { InstantiationError } from "@errors/InstantiationError.js";

/**
 * ApiRouter is responsible for api router operations.
 */
class ApiRouter {
    /**
   * Singleton instance
   * @private
   */
  private static instance: ApiRouter;
    /**
   * Router
   * @private
   */
  private router: Router;

    /**
   * Constructs a new ApiRouter instance.
   */
  private constructor() {
    this.router = Router();
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
   * Gets router
   * @returns The router result
   */
  public getRouter(): Router {
    return this.router;
  }

  /**
   * Initializes API routes.
   */
  public async initializeRoutes(): Promise<void> {
    // Health check
    this.router.get("/health", (_req: Request, res: Response) => {
      res.json({
        status: "healthy",
        timestamp: new Date().toISOString(),
      });
    });
  }
}

export default ApiRouter;
