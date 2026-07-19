import express from "express";
import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { metrics } from "./metrics.js";
import { formatPrometheusMetrics } from "./prometheus.js";

/**
 * HealthService is a singleton class responsible for managing the service. It provides methods to initialize and gracefully stop the service.
 */
class HealthService extends ServiceManager {
    /**
   * Singleton instance
   * @private
   */
  protected static instance: HealthService;

    /**
   * Constructs a new HealthService instance.
   * @param enforce - A function to enforce the Singleton pattern
   * @throws Error if instantiated directly
   */
  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate HealthService directly. Use getInstance()");
    }
    super(enforce);
  }

    /**
   * Gets the single instance of the HealthService class.
   * @returns The single instance of the class
   */
  public static getInstance(): HealthService {
    if (!HealthService.instance) {
      HealthService.instance = new HealthService(Enforce);
    }
    return HealthService.instance;
  }

    /**
   * Creates health check server
   * @param port - The port
   * @returns The express. application result
   */
  public createHealthCheckServer(port = 3000): express.Application {
    const app = express();

    app.get("/health", (req, res) => {
      res.json({ status: "healthy", timestamp: new Date().toISOString() });
    });

    app.get("/metrics", (req, res) => {
      res.set("Content-Type", "text/plain");
      res.send(formatPrometheusMetrics());
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
  public startHealthCheckServer(port = 3000): void {
    const app = this.createHealthCheckServer(port);
    app.listen(port, () => {
      console.log(`Health check server listening on port ${port}`);
    });
  }
}


export default HealthService;

/**
 * The health service
 */
const healthService = HealthService.getInstance();

/**
 * Creates health check server
 * @param port - The port
 * @returns The express. application result
 */
export function createHealthCheckServer(port = 3000): express.Application {
  return healthService.createHealthCheckServer(port);
}

/**
 * Starts health check server
 * @param port - The port
 */
export function startHealthCheckServer(port = 3000): void {
  healthService.startHealthCheckServer(port);
}
