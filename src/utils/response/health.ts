import express from "express";
import {createLogger} from "@utils/logger/Log";
import PrometheusService from "@utils/response/prometheus";
const logger = createLogger(module);

/**
 * HealthService is a static utility class responsible for creating and
 * starting the health check server. All members are static; the class
 * is never instantiated.
 */

class HealthService
{
  /**
   * Creates health check server
   * @returns The express. application result
   */

  public static createHealthCheckServer(): express.Application
  {
    const app = express();

    app.get("/health", (req, res) => {
      res.json({ status: "healthy", timestamp: new Date().toISOString() });
    });

    app.get("/metrics", (req, res) => {
      res.set("Content-Type", "text/plain");
      res.send(PrometheusService.formatPrometheusMetrics());
    });

    app.get("/ready", (req, res) => {
      res.json({ status: "ready", timestamp: new Date().toISOString() });
    });

    return app;
  }

  /**
   * Starts health check server
   * @param port - The port
   */
  public static startHealthCheckServer(port = 3000): void
  {
    const app = HealthService.createHealthCheckServer();
    app.listen(port, () => {
      logger.info(`Health check server listening on port ${port}`);
    });
  }
}

export default HealthService;
