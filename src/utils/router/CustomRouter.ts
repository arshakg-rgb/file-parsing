import { Router, IRoute, RequestHandler } from "express";

/**
 * Thin wrapper around Express Router used throughout the application.
 *
 * Provides a common interface for mounting routers and declaring routes
 * while keeping the underlying Express Router accessible.
 */
export class CustomRouter {
  /**
   * The underlying Express router.
   * @protected
   */
  protected readonly router: Router;

  /**
   * Creates a new CustomRouter with an isolated Express Router.
   */
  constructor() {
    this.router = Router();
  }

  /**
   * Returns the underlying Express router for mounting in the application.
   * @returns The Express Router instance.
   */
  public getRouter(): Router {
    return this.router;
  }

  /**
   * Mounts another CustomRouter under the given path.
   * @param path - The path prefix.
   * @param router - The CustomRouter to mount.
   */
  public use(path: string, router: CustomRouter): void;
  /**
   * Mounts an Express request handler under the given path.
   * @param path - The path prefix.
   * @param handler - The Express request handler.
   */
  public use(path: string, handler: RequestHandler): void;
  public use(path: string, routerOrHandler: CustomRouter | RequestHandler): void {
    if (routerOrHandler instanceof CustomRouter) {
      this.router.use(path, routerOrHandler.getRouter());
    } else {
      this.router.use(path, routerOrHandler);
    }
  }

  /**
   * Returns an Express IRoute for the given path.
   * @param path - The route path.
   * @returns The Express IRoute instance.
   */
  public route(path: string): IRoute {
    return this.router.route(path);
  }
}
