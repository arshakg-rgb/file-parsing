import { Request } from "express";
import rateLimit, { RateLimitRequestHandler, ipKeyGenerator } from "express-rate-limit";
import { AuthError } from "@errors/AuthError.js";

/**
 * Builds a rate-limit key that combines the client IP with the (lowercased,
 * trimmed) email from the request body, so a single IP cannot exhaust
 * attempts across many accounts and a single account cannot be hammered
 * from many IPs without also being throttled per-IP.
 * @param req - The Express request object.
 * @returns The composite rate-limit key.
 */

function keyByIpAndEmail(req: Request): string
{
  const email: string = typeof req.body?.email === "string" ? req.body.email.trim().toLowerCase() : "unknown";

  return `${ipKeyGenerator(req.ip ?? "unknown")}:${email}`;
}

/**
 * Rate limiter for `POST /auth/login`. Keyed by IP+email to blunt both
 * credential-stuffing (many emails from one IP) and targeted brute-force
 * (many attempts against one email) attacks.
 */

export const loginRateLimiter: RateLimitRequestHandler = rateLimit({
  windowMs: 15 * 60 * 1000,
  limit: 10,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: keyByIpAndEmail,
  handler: (_req, _res, next) => {
    next(new AuthError(AuthError.TOO_MANY_REQUESTS, "Too many login attempts. Please try again later."));
  },
});

/**
 * Rate limiter for `POST /auth/signup`. Keyed by IP only, since an attacker
 * probing for account creation supplies a different email on every request.
 */

export const signupRateLimiter: RateLimitRequestHandler = rateLimit({
  windowMs: 60 * 60 * 1000,
  limit: 5,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => ipKeyGenerator(req.ip ?? "unknown"),
  handler: (_req, _res, next) => {
    next(new AuthError(AuthError.TOO_MANY_REQUESTS, "Too many signup attempts from this network. Please try again later."));
  },
});

/**
 * Rate limiter for `POST /auth/forgot-password`. Keyed by IP+email, same
 * rationale as {@link loginRateLimiter}: prevents both mass-enumeration
 * from one IP and repeated reset-email spam against one address.
 */

export const forgotPasswordRateLimiter: RateLimitRequestHandler = rateLimit({
  windowMs: 15 * 60 * 1000,
  limit: 5,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: keyByIpAndEmail,
  handler: (_req, _res, next) => {
    next(new AuthError(AuthError.TOO_MANY_REQUESTS, "Too many password reset requests. Please try again later."));
  },
});
