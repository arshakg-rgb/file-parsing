import { Request, Response } from "express";
import { Server as HttpServer } from "node:http";
import * as http from "node:http";
import { createLogger, Logger } from "@utils/logger/Log.js";
import MySqlManager from "@config/db/MySqlManager.js";
import express, { Express } from "express";
import ServiceManager from "@config/ServiceManager.js";
import { Constants } from "@common/io/Constants.js";
import { error404Handler, errorPageHandler } from "@middleware/CommonMiddleware.js";
import bodyParser from "body-parser";
import CorsUtils from "@config/cors/CorsUtils.js";
import ApiRouter from "@routes/ApiRouter.js";
import { DeprecationMiddleware } from "@middleware/DeprecationMiddleware.js";

/**
 * Logger instance for the module
 */
const logger: Logger = createLogger("app");

/**
 * Application entry point class.
 */
export class App {
  /**
   * The Express application instance.
   * @private
   */
  private readonly app: Express;

  /**
   * The HTTP server instance.
   * @private
   */
  private readonly server: HttpServer;

  /**
   * The service managers for the application.
   * @private
   */
  private serviceManagers: ServiceManager[];

    /**
   * Constructs a new App instance.
   * @param serviceManagers - The serviceManagers arguments
   */
  constructor(...serviceManagers: ServiceManager[]) {
    this.app = express();
    this.initializeApp();
    this.server = this.createServer();
    this.serviceManagers = serviceManagers;
  }

  /**
   * Starts the Express server and initializes the managers.
   */
  public async listen(): Promise<void> {
    const port: number = parseInt(process.env.PORT || process.env.APP_PORT || "3000");

    try {
      await this.initializeManagers();
      await ApiRouter.getInstance().initializeRoutes();

      this.server.listen(port, "0.0.0.0", async (): Promise<void> => {
        await MySqlManager.getInstance().sequelize
          .sync({ force: false })
          .then((): void => logger.info("Database & tables created!"))
          .catch((error: Error): void =>
            logger.error(`Failed to sync models: ${error.message}`)
          );

        logger.info(`Server is running on port ${port}`);
      });

      process.on(Constants.SIGINT, this.shutdown.bind(this));
      process.on(Constants.SIGTERM, this.shutdown.bind(this));
    } catch (error) {
      logger.error(`Failed to initialize services or start the server: ${error instanceof Error ? error.message : String(error)}`);
      process.exit(1);
    }
  }

  /**
   * Initializes managers with parallel execution
   */
  private async initializeManagers(): Promise<void> {
    await Promise.all(
      this.serviceManagers.map((manager: ServiceManager): Promise<void> => manager.initialize())
    );
  }

  /**
   * Sets up the middleware for the Express application.
   */
  private setupMiddlewares(): void {
    const requestBodyLimit: string = process.env.REQUEST_BODY_LIMIT || "10mb";

    this.app.use(CorsUtils.setupCors());

    this.app.use(
      bodyParser.json({
        verify: function (req: Request, _res: Response, buf: Buffer): void {
          (req as unknown as { rawBody: Buffer }).rawBody = buf;
        },
        limit: requestBodyLimit,
      })
    );
    this.app.use(bodyParser.urlencoded({ extended: true, limit: requestBodyLimit }));
  }

  /**
   * Initializes the Express application.
   */
  private initializeApp(): void {
    this.setupMiddlewares();
    this.setupRoutes();
  }

  /**
   * Sets up the routes for the Express application.
   */
  private setupRoutes(): void {
    this.app.use(DeprecationMiddleware.deprecationWarning());

    this.app.use(ApiRouter.getInstance().getRouter());

    this.app.use(error404Handler);
    this.app.use(errorPageHandler);
  }

  /**
   * Creates an HTTP server.
   */
  private createServer(): HttpServer {
    return http.createServer(this.app);
  }

  /**
   * Gracefully shuts down the managers and exits the process.
   */
  private async shutdown(): Promise<void> {
    logger.info("Shutting down gracefully");
    await Promise.all(
      this.serviceManagers.map((serviceManager: ServiceManager): Promise<void> => serviceManager.shutdown())
    );
    process.exit(0);
  }

  /**
   * Gets the Express application instance (for testing purposes).
   */
  public getApp(): Express {
    return this.app;
  }
}
