import express from "express";
import Config from "@config/system-config/Config.js";
import ServiceManager, { Enforce } from "@config/ServiceManager.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { metrics } from "./metrics.js";
import { formatPrometheusMetrics } from "./prometheus.js";

class HealthService extends ServiceManager {
  protected static instance: HealthService;

  private constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError("Cannot instantiate HealthService directly. Use getInstance()");
    }
    super(enforce);
  }

  public static getInstance(): HealthService {
    if (!HealthService.instance) {
      HealthService.instance = new HealthService(Enforce);
    }
    return HealthService.instance;
  }

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

  public startHealthCheckServer(port = 3000): void {
    const app = this.createHealthCheckServer(port);
    app.listen(port, () => {
      console.log(`Health check server listening on port ${port}`);
    });
  }
}


export default HealthService;

const healthService = HealthService.getInstance();

export function createHealthCheckServer(port = 3000): express.Application {
  return healthService.createHealthCheckServer(port);
}

export function startHealthCheckServer(port = 3000): void {
  healthService.startHealthCheckServer(port);
}
