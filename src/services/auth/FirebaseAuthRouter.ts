import { NextFunction, Request, Response } from "express";
import joi, { ObjectSchema } from "joi";
import pino from "pino";
import { CustomRouter } from "@utils/router/CustomRouter.js";
import { setupValidator } from "@utils/validator/SetupValidator.js";
import { createLogger } from "@utils/logger/Log.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { AuthError } from "@errors/AuthError.js";
import { ValidationError } from "@errors/ValidationError.js";
import { settings } from "@shared/Settings.js";
import { FirebaseAdmin } from "@config/firebase/FirebaseAdmin.js";
import { FirebaseAuthClient, IFirebaseAuthResult } from "@config/firebase/FirebaseAuthClient.js";
import { verifyFirebaseToken, ID_TOKEN_COOKIE } from "@common/middleware/FirebaseAuthMiddleware.js";
import { loginRateLimiter, signupRateLimiter, forgotPasswordRateLimiter } from "@common/middleware/RateLimitMiddleware.js";

const logger: pino.Logger = createLogger(module);

/**
 * Name of the httpOnly cookie that carries the Firebase refresh token.
 */
const REFRESH_TOKEN_COOKIE: string = "refresh_token";

/**
 * How long the refresh-token cookie stays valid for, in milliseconds.
 * Firebase refresh tokens themselves don't expire on a fixed schedule, but
 * the cookie is capped here to force periodic re-authentication.
 */
const REFRESH_TOKEN_COOKIE_MAX_AGE_MS: number = 30 * 24 * 60 * 60 * 1000;

/**
 * Schema for signup requests. Firebase itself rejects passwords under 6
 * characters, so the same minimum is enforced here to fail fast with a
 * clear message before making the network call.
 */
const signupSchema: ObjectSchema = joi.object().keys({
  email: joi.string().trim().email().required(),
  password: joi.string().min(6).max(4096).required()
});

/**
 * Schema for login requests. Password strength is not enforced here since
 * the account may predate any stricter policy; Firebase validates the
 * credential itself.
 */
const loginSchema: ObjectSchema = joi.object().keys({
  email: joi.string().trim().email().required(),
  password: joi.string().min(1).max(4096).required()
});

/**
 * Schema for refresh-token requests. The token itself is only read from
 * the httpOnly cookie (see {@link REFRESH_TOKEN_COOKIE}), so this schema
 * validates nothing from the body but is kept for symmetry/extensibility.
 */
const refreshSchema: ObjectSchema = joi.object().keys({});

/**
 * Schema for `/auth/forgot-password` requests.
 */
const forgotPasswordSchema: ObjectSchema = joi.object().keys({
  email: joi.string().trim().email().required()
});

/**
 * Schema for `/auth/reset-password` requests.
 */
const resetPasswordSchema: ObjectSchema = joi.object().keys({
  oobCode: joi.string().min(1).required(),
  newPassword: joi.string().min(6).max(4096).required()
});

/**
 * Router exposing this app's own Firebase-authentication endpoints.
 *
 * `signup` / `login` / `refresh` are public and proxy to the Firebase
 * Identity Toolkit REST API (via `FirebaseAuthClient`) using this project's
 * Web API key, so callers only ever talk to this backend. `session` and
 * `logout` require a valid Firebase ID token (via `verifyFirebaseToken`)
 * and use the Admin SDK (via `FirebaseAdmin`).
 */
export class FirebaseAuthRouter extends CustomRouter
{
  /**
   * Singleton instance.
   * @private
   */

  private static instance: FirebaseAuthRouter;

  /**
   * Constructs a new FirebaseAuthRouter instance.
   * @param enforce - A function to enforce the Singleton pattern.
   * @throws {InstantiationError} if instantiated directly instead of via {@link getInstance}
   */

  private constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Cannot instantiate FirebaseAuthRouter directly. Use getInstance()");
    }

    super();
    this.initializeRoutes();
  }

  /**
   * Gets the single instance of the FirebaseAuthRouter class.
   * @returns The single instance of the class.
   */

  public static getInstance(): FirebaseAuthRouter
  {
    if (!FirebaseAuthRouter.instance)
    {
      FirebaseAuthRouter.instance = new FirebaseAuthRouter(Enforce);
    }

    return FirebaseAuthRouter.instance;
  }

  /**
   * Validates a request body against a Joi schema, throwing a
   * {@link ValidationError} listing every invalid/missing field.
   * @param body - The raw request body to validate.
   * @param schema - The Joi schema to validate against.
   * @returns The validated (and stripped/typed) value.
   * @throws {ValidationError} if the body fails validation.
   * @private
   */

  private validateBody<T>(body: unknown, schema: ObjectSchema): T
  {
    const { error, value } = setupValidator(body ?? {}, schema, true, false);

    if (error)
    {
      const fields: string[] = error.details.map((detail) => String(detail.path[0]));
      const message: string = error.details.map((detail) => detail.message).join("; ");

      throw new ValidationError(ValidationError.INPUT, message, fields);
    }

    return value as T;
  }

  /**
   * Sets the `idToken`/`refreshToken` as httpOnly cookies on the response
   * instead of returning them in the JSON body, so they are inaccessible
   * to page JavaScript (mitigates token theft via XSS). `Secure` is only
   * set outside local development so the cookies still work over plain
   * HTTP on localhost.
   * @param res - The Express response object.
   * @param result - The Identity Toolkit auth result to persist as cookies.
   * @private
   */

  private setAuthCookies(res: Response, result: IFirebaseAuthResult): void
  {
    const sameSite: "strict" | "lax" | "none" | undefined = settings.COOKIE_SAMESITE.toLowerCase() as "strict" | "lax" | "none";
    const expiresInMs: number = Number(result.expiresIn) * 1000 || 60 * 60 * 1000;

    const cookieOptions: Record<string, unknown> = {
      httpOnly: true,
      secure: settings.COOKIE_SECURE,
      sameSite,
      maxAge: expiresInMs,
      path: "/"
    };

    if (settings.COOKIE_DOMAIN)
    {
      cookieOptions.domain = settings.COOKIE_DOMAIN;
    }

    res.cookie(ID_TOKEN_COOKIE, result.idToken, cookieOptions as Record<string, unknown> & { maxAge: number } as any);

    const refreshCookieOptions: Record<string, unknown> = {
      httpOnly: true,
      secure: settings.COOKIE_SECURE,
      sameSite,
      maxAge: REFRESH_TOKEN_COOKIE_MAX_AGE_MS,
      path: "/v1/auth"
    };

    if (settings.COOKIE_DOMAIN)
    {
      refreshCookieOptions.domain = settings.COOKIE_DOMAIN;
    }

    res.cookie(REFRESH_TOKEN_COOKIE, result.refreshToken, refreshCookieOptions as Record<string, unknown> & { maxAge: number } as any);
  }

  /**
   * Clears the auth cookies previously set by {@link setAuthCookies}.
   * @param res - The Express response object.
   * @private
   */

  private clearAuthCookies(res: Response): void
  {
    const clearOptions: Record<string, unknown> = { path: "/" };
    if (settings.COOKIE_DOMAIN) { clearOptions.domain = settings.COOKIE_DOMAIN; }
    res.clearCookie(ID_TOKEN_COOKIE, clearOptions as any);

    const refreshClearOptions: Record<string, unknown> = { path: "/v1/auth" };
    if (settings.COOKIE_DOMAIN) { refreshClearOptions.domain = settings.COOKIE_DOMAIN; }
    res.clearCookie(REFRESH_TOKEN_COOKIE, refreshClearOptions as any);
  }

  /**
   * Formats a Firebase Identity Toolkit auth result into this API's
   * response shape. The tokens themselves are never included in the body
   * (see {@link setAuthCookies}); only non-sensitive identity fields are.
   * @param result - The raw Identity Toolkit result.
   * @private
   */

  private toAuthResponse(result: IFirebaseAuthResult)
  {
    return {
      success: true,
      data: {
        uid: result.localId,
        email: result.email,
        idToken: result.idToken,
        refreshToken: result.refreshToken,
        expiresIn: result.expiresIn
      }
    };
  }

  /**
   * Creates a new Firebase user account and returns its ID/refresh tokens.
   * @param req - The Express request object.
   * @param res - The Express response object.
   * @param next - The next middleware function in the stack.
   */

  private signup = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    let email: string | undefined;

    try
    {
      const credentials = this.validateBody<{ email: string; password: string }>(req.body, signupSchema);
      email = credentials.email;

      const result: IFirebaseAuthResult = await FirebaseAuthClient.getInstance().signUp(credentials.email, credentials.password);

      logger.info({ event: "signup_success", uid: result.localId, email, ip: req.ip }, "auth_audit");
      this.setAuthCookies(res, result);
      res.status(201).json(this.toAuthResponse(result));
    }
    catch (err)
    {
      logger.warn({ event: "signup_failure", email, ip: req.ip, error: err instanceof AuthError ? err.code : String(err) }, "auth_audit");
      next(err);
    }
  };

  /**
   * Signs an existing Firebase user in and returns fresh ID/refresh tokens.
   * @param req - The Express request object.
   * @param res - The Express response object.
   * @param next - The next middleware function in the stack.
   */

  private login = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    let email: string | undefined;

    try
    {
      const credentials = this.validateBody<{ email: string; password: string }>(req.body, loginSchema);
      email = credentials.email;

      const result: IFirebaseAuthResult = await FirebaseAuthClient.getInstance().signInWithPassword(credentials.email, credentials.password);

      logger.info({ event: "login_success", uid: result.localId, email, ip: req.ip }, "auth_audit");
      this.setAuthCookies(res, result);
      res.json(this.toAuthResponse(result));
    }
    catch (err)
    {
      logger.warn({ event: "login_failure", email, ip: req.ip, error: err instanceof AuthError ? err.code : String(err) }, "auth_audit");
      next(err);
    }
  };

  /**
   * Exchanges a refresh token for a new ID/refresh token pair, so a client
   * can recover from an expired ID token without re-entering credentials.
   * @param req - The Express request object.
   * @param res - The Express response object.
   * @param next - The next middleware function in the stack.
   */

  private refreshToken = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      this.validateBody<Record<string, never>>(req.body, refreshSchema);

      const refreshToken: unknown = req.cookies?.[REFRESH_TOKEN_COOKIE];

      if (typeof refreshToken !== "string" || !refreshToken)
      {
        throw new AuthError(AuthError.TOKEN_MISSING, "No refresh token cookie present. Please log in again.");
      }

      const result: IFirebaseAuthResult = await FirebaseAuthClient.getInstance().refresh(refreshToken);

      this.setAuthCookies(res, result);
      res.json(this.toAuthResponse(result));
    }
    catch (err)
    {
      next(err);
    }
  };

  /**
   * Sends a password-reset email for the given address. Always responds
   * with a generic success message regardless of whether the email is
   * registered, to avoid leaking account existence (user enumeration).
   * @param req - The Express request object.
   * @param res - The Express response object.
   * @param next - The next middleware function in the stack.
   */

  private forgotPassword = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    let email: string;

    try
    {
      email = this.validateBody<{ email: string }>(req.body, forgotPasswordSchema).email;
    }
    catch (err)
    {
      next(err);
      return;
    }

    try
    {
      await FirebaseAuthClient.getInstance().sendPasswordResetEmail(email);
      logger.info({ event: "password_reset_requested", email, ip: req.ip }, "auth_audit");
    }
    catch (err)
    {
      logger.warn({ event: "password_reset_request_failed", email, ip: req.ip, error: err instanceof AuthError ? err.code : String(err) }, "auth_audit");

      if (!(err instanceof AuthError) || err.code !== AuthError.INVALID_CREDENTIALS)
      {
        next(err);
        return;
      }
    }

    res.json({ success: true, message: "If an account exists for this email, a password reset link has been sent." });
  };

  /**
   * Completes a password reset using the `oobCode` delivered by the
   * reset email.
   * @param req - The Express request object.
   * @param res - The Express response object.
   * @param next - The next middleware function in the stack.
   */

  private resetPassword = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      const { oobCode, newPassword } = this.validateBody<{ oobCode: string; newPassword: string }>(req.body, resetPasswordSchema);
      const { email } = await FirebaseAuthClient.getInstance().resetPassword(oobCode, newPassword);

      logger.info({ event: "password_reset_success", email, ip: req.ip }, "auth_audit");
      res.json({ success: true, message: "Password has been reset successfully. Please log in with your new password." });
    }
    catch (err)
    {
      logger.warn({ event: "password_reset_failure", ip: req.ip, error: err instanceof AuthError ? err.code : String(err) }, "auth_audit");
      next(err);
    }
  };

  /**
   * Returns the authenticated user's decoded token, allowing a client to
   * confirm that its current Firebase session is still valid.
   * @param req - The Express request object.
   * @param res - The Express response object.
   */

  private getSession = (req: Request, res: Response): void =>
  {
    res.json({
      success: true,
      data: { uid: req.user?.uid, email: req.user?.email }
    });
  };

  /**
   * Logs the authenticated user out by revoking all of their Firebase
   * refresh tokens, invalidating every session issued before this call.
   * @param req - The Express request object.
   * @param res - The Express response object.
   * @param next - The next middleware function in the stack.
   */

  private logout = async (req: Request, res: Response, next: NextFunction): Promise<void> =>
  {
    try
    {
      if (!req.user?.uid)
      {
        throw new AuthError(AuthError.NOT_AUTHENTICATED, "No authenticated user found on this request.");
      }

      await FirebaseAdmin.getInstance().auth().revokeRefreshTokens(req.user.uid);
      logger.info({ event: "logout_success", uid: req.user.uid, ip: req.ip }, "auth_audit");

      this.clearAuthCookies(res);
      res.json({ success: true, message: "Logged out successfully" });
    }
    catch (err)
    {
      logger.warn({ event: "logout_failure", uid: req.user?.uid, ip: req.ip, error: err instanceof Error ? err.message : String(err) }, "auth_audit");
      next(err instanceof Error ? err : new AuthError(AuthError.TOKEN_INVALID, "Failed to log out."));
    }
  };

  /**
   * Initializes the routes for this router.
   */

  private initializeRoutes(): void
  {
    this.route("/auth/signup")
      .post(signupRateLimiter, this.signup);

    this.route("/auth/login")
      .post(loginRateLimiter, this.login);

    this.route("/auth/refresh")
      .post(this.refreshToken);

    this.route("/auth/forgot-password")
      .post(forgotPasswordRateLimiter, this.forgotPassword);

    this.route("/auth/reset-password")
      .post(this.resetPassword);

    this.route("/auth/session")
      .get(verifyFirebaseToken, this.getSession);

    this.route("/auth/logout")
      .post(verifyFirebaseToken, this.logout);
  }
}

function Enforce(): void {}

export default FirebaseAuthRouter;
