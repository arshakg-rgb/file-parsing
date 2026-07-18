export interface IAppConfig {
  name: string;
  version: string;
  environment: "development" | "staging" | "production";
  port: number;
}
