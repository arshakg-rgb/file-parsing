import { Server as HttpServer } from "node:http";
import { Server as HttpsServer } from "node:https";

export type CustomServer = HttpServer | HttpsServer;

export interface IServer {
  listen(port: number, hostname: string, callback: () => void): void;
  close(callback: () => void): void;
}
