import { createServer } from "node:http";
import { createLogger } from "@utils/logger/Log.js";

const logger = createLogger(module);
const port = Number(process.env.PORT || process.env.HEALTH_CHECK_PORT || "8080");

const server = createServer((_req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ status: "healthy", service: process.env.K_SERVICE || "unknown" }));
});

server.listen(port, "0.0.0.0", () => {
  logger.info({ port }, "[HealthServer] listening");
});

server.on("error", (err) => {
  logger.error({ err }, "[HealthServer] error");
});
