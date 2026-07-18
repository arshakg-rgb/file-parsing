import { Request, Response, NextFunction } from "express";

export class DeprecationMiddleware {
  /**
   * Adds deprecation warning headers to API responses
   */
  static deprecationWarning() {
    return (req: Request, res: Response, next: NextFunction): void => {
      // Check if the route is deprecated
      if (req.path.includes("/v1/")) {
        res.setHeader("X-API-Deprecation", "This API version is deprecated. Please use v2.");
      }
      next();
    };
  }
}
