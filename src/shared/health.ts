import express from "express";
import { metrics } from "../shared/metrics.js";
import { formatPrometheusMetrics } from "../shared/prometheus.js";

export function createHealthCheckServer(port = 3000): express.Application {
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

export function startHealthCheckServer(port = 3000): void {
  const app = createHealthCheckServer(port);
  app.listen(port, () => {
    console.log(`Health check server listening on port ${port}`);
  });
}
