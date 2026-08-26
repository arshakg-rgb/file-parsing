import { NextFunction, Request, Response } from "express";
import { auth } from "firebase-admin";
import pino from "pino";
import { AuthError } from "@errors/AuthError.js";
import { FirebaseAdmin } from "@config/firebase/FirebaseAdmin.js";
import { createLogger } from "@utils/logger/Log.js";

const logger: pino.Logger = createLogger(module);

/**
 * Extracts the bearer token from the `Authorization` header.
 * @param req - The Express request object.
 * @returns The raw token string, or undefined if the header is missing/malformed.
 */

function extractBearerToken(req: Request): string | undefined
{
  const header: string | undefined = req.headers.authorization;

  if (!header || !header.startsWith("Bearer "))
  {
    return undefined;
  }

  const token: string = header.slice("Bearer ".length).trim();

  return token.length > 0 ? token : undefined;
}

/**
 * Maps a firebase-admin verification error to an AuthError with an
 * appropriate code and user-facing message.
 * @param err - The error thrown by `auth().verifyIdToken`.
 * @returns The mapped AuthError.
 */

function mapFirebaseError(err: unknown): AuthError
{
  const code: string | undefined = (err as { code?: string })?.code;

  switch (code)
  {
    case "auth/id-token-expired":
      return new AuthError(AuthError.TOKEN_EXPIRED, "Authentication token has expired. Please log in again.");
    case "auth/id-token-revoked":
      return new AuthError(AuthError.TOKEN_REVOKED, "Your session has been revoked. Please log in again.");
    case "auth/user-disabled":
      return new AuthError(AuthError.USER_DISABLED, "This user account has been disabled.");
    case "auth/argument-error":
    case "auth/invalid-id-token":
      return new AuthError(AuthError.TOKEN_INVALID, "Authentication token is invalid.");
    default:
      return new AuthError(AuthError.TOKEN_INVALID, "Failed to verify authentication token.");
  }
}

/**
 * Express middleware that verifies a Firebase ID token supplied in the
 * `Authorization: Bearer <token>` header. Rejects missing, malformed,
 * expired, revoked, or otherwise invalid tokens with an AuthError, and
 * attaches the decoded token to `req.user` on success.
 * @param req - The Express request object.
 * @param _res - The Express response object.
 * @param next - The next middleware function in the stack.
 */

export async function verifyFirebaseToken(req: Request, _res: Response, next: NextFunction): Promise<void>
{
  const token: string | undefined = extractBearerToken(req);

  if (!token)
  {
    next(new AuthError(AuthError.TOKEN_MISSING, "Missing or malformed Authorization header. Expected 'Bearer <token>'."));
    return;
  }

  try
  {
    const decodedToken: auth.DecodedIdToken = await FirebaseAdmin.getInstance().auth().verifyIdToken(token, true);

    req.user = decodedToken;
    next();
  }
  catch (err)
  {
    logger.warn({ error: err instanceof Error ? err.message : String(err) }, "firebase_token_verification_failed");
    next(mapFirebaseError(err));
  }
}
