import { Request, Response, NextFunction } from "express";
import { AuthError } from "@errors/AuthError.js";
import CorsUtils from "@config/cors/CorsUtils.js";

/**
 * Safe HTTP methods that are not state-changing and cannot be CSRFed.
 */
const SAFE_METHODS = new Set(["GET", "HEAD", "OPTIONS"]);

/**
 * CSRF defense-in-depth for cross-site cookie-based auth.
 *
 * When cookies are sent with `SameSite=None`, we cannot rely on the browser
 * to withhold them from cross-origin requests. This middleware validates the
 * `Origin` or `Referer` header on every state-changing request and rejects
 * requests that do not come from an allowed CORS domain.
 *
 * If no allowed CORS domains are configured, the middleware allows all
 * origins (dev convenience). In production, set `CORS_DOMAINS` and
 * `COOKIE_SAMESITE=None` + `COOKIE_SECURE=true` together.
 */
export function originCsrfMiddleware(req: Request, _res: Response, next: NextFunction): void
{
  if (SAFE_METHODS.has(req.method))
  {
    next();
    return;
  }

  const allowed: string[] = CorsUtils.getAllowedDomains();

  // No CORS allow-list configured — dev mode, don't enforce origin checks.
  if (allowed.length === 0)
  {
    next();
    return;
  }

  const isAllowed = (value: string): boolean =>
    allowed.some((pattern) =>
    {
      if (pattern === value) return true;

      try
      {
        return new RegExp(pattern).test(value);
      }
      catch
      {
        return false;
      }
    });

  const origin = req.headers.origin;

  if (origin && isAllowed(origin))
  {
    next();
    return;
  }

  const referer = req.headers.referer;

  if (referer)
  {
    try
    {
      const refererOrigin = new URL(referer).origin;

      if (isAllowed(refererOrigin))
      {
        next();
        return;
      }
    }
    catch
    {
      // Malformed Referer; fall through to rejection.
    }
  }

  next(new AuthError(AuthError.CSRF_ORIGIN_MISMATCH, "Request origin is not allowed."));
}
