import { IAppConfig } from "./io/IAppConfig.js";
import { IAuthConfig } from "./io/IAuthConfig.js";
import { ICommonConfig } from "./io/ICommonConfig.js";
import { IMysqlConfig } from "./io/IMysqlConfig.js";
import { IRedisConfig } from "./io/IRedisConfig.js";
import { ISocketConfig } from "./io/ISocketConfig.js";

export interface ValidationResult<T> {
  value: T;
  error?: Error;
}

function ok<T>(value: T): ValidationResult<T> {
  return { value };
}

export const validateAppConfig = (data: unknown): ValidationResult<IAppConfig> => ok(data as IAppConfig);
export const validateAuthConfig = (data: unknown): ValidationResult<IAuthConfig> => ok(data as IAuthConfig);
export const validateCommonConfig = (data: unknown): ValidationResult<ICommonConfig> => ok(data as ICommonConfig);
export const validateMysqlConfig = (data: unknown): ValidationResult<IMysqlConfig> => ok(data as IMysqlConfig);
export const validateRedisConfig = (data: unknown): ValidationResult<IRedisConfig> => ok(data as IRedisConfig);
export const validateSocketConfig = (data: unknown): ValidationResult<ISocketConfig> => ok(data as ISocketConfig);
