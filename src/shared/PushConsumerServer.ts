import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { createLogger } from "@utils/logger/Log.js";

export interface PushConsumerOptions<TPayload>
{
  parse: (body: unknown) => TPayload;
  process: (payload: TPayload) => Promise<void>;
  port?: number;
  healthPath?: string;
}

export function startPushConsumer<TPayload>(options: PushConsumerOptions<TPayload>): void
{
  const logger = createLogger(module);
  const port = options.port ?? Number(process.env.PORT) ?? 8080;

  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    if (req.method === "GET" && (req.url === (options.healthPath ?? "/health") || req.url === "/health"))
    {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "healthy" }));
      return;
    }

    if (req.method !== "POST")
    {
      res.writeHead(405);
      res.end("Method Not Allowed");
      return;
    }

    let body = "";
    req.setEncoding("utf8");
    for await (const chunk of req)
    {
      body += chunk;
    }

    let envelope: unknown;
    try
    {
      envelope = JSON.parse(body);
    }
    catch (err)
    {
      logger.error("push_body_parse_failed", { error: String(err) });
      res.writeHead(400);
      res.end("Invalid JSON");
      return;
    }

    const message = (envelope as { message?: { data?: string } } | undefined)?.message;
    if (!message?.data)
    {
      res.writeHead(400);
      res.end("Missing message data");
      return;
    }

    let payload: TPayload;
    try
    {
      const decoded = Buffer.from(message.data, "base64").toString("utf8");
      const parsed = JSON.parse(decoded);
      payload = options.parse(parsed);
    }
    catch (err)
    {
      logger.error("push_payload_parse_failed", { error: String(err) });
      res.writeHead(400);
      res.end("Invalid payload");
      return;
    }

    try
    {
      await options.process(payload);
      res.writeHead(200);
      res.end("OK");
    }
    catch (err)
    {
      logger.error("push_process_failed", { error: String(err) });
      res.writeHead(500);
      res.end("Failed");
    }
  });

  server.listen(port, () => {
    logger.info("push_consumer_listening", { port });
  });
}
