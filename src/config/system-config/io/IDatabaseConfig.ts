export interface IDatabaseConfig {
  url: string;
  host: string;
  port: number;
  username: string;
  password: string;
  database: string;
  ssl: boolean;
  poolSize: number;
}
