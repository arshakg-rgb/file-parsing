import { createServer } from "node:http";

const port = Number(process.env.PORT || process.env.HEALTH_CHECK_PORT || "8080");

const server = createServer((_req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ status: "healthy", service: process.env.K_SERVICE || "unknown" }));
});

server.listen(port, "0.0.0.0", () => {
  console.log(`[HealthServer] listening on 0.0.0.0:${port}`);
});

server.on("error", (err) => {
  console.error("[HealthServer] error:", err);
});
