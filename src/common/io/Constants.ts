export const COMMON_CONSTANTS = {
  DEFAULT_PAGE_SIZE: 10,
  MAX_PAGE_SIZE: 100,
  DEFAULT_TIMEOUT: 30000,
  CACHE_TTL: 3600,
} as const;

export const API_RESPONSE_CODES = {
  SUCCESS: 200,
  CREATED: 201,
  BAD_REQUEST: 400,
  UNAUTHORIZED: 401,
  FORBIDDEN: 403,
  NOT_FOUND: 404,
  INTERNAL_ERROR: 500,
  SERVICE_UNAVAILABLE: 503,
} as const;

export const STATUS_CODES = {
  ACTIVE: "active",
  INACTIVE: "inactive",
  PENDING: "pending",
  COMPLETED: "completed",
  FAILED: "failed",
  CANCELLED: "cancelled",
} as const;

export const Constants = {
  SIGINT: "SIGINT",
  SIGTERM: "SIGTERM",
} as const;
