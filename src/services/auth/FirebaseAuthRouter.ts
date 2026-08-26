import { NextFunction, Request, Response } from "express";
import { CustomRouter } from "@utils/router/CustomRouter.js";
import { InstantiationError } from "@errors/InstantiationError.js";
import { AuthError } from "@errors/AuthError.js";
import { ValidationError } from "@errors/ValidationError.js";
import { FirebaseAdmin } from "@config/firebase/FirebaseAdmin.js";
import { FirebaseAuthClient, IFirebaseAuthResult } from "@config/firebase/FirebaseAuthClient.js";
import { verifyFirebaseToken } from "@common/middleware/FirebaseAuthMiddleware.js";

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
   * Extracts and validates `email`/`password` strings from a request body.
   * @param req - The Express request object.
   * @returns The validated email and password.
   * @throws {ValidationError} if either field is missing or not a string.
   * @private
   */

  private requireCredentials(req: Request): { email: string; password: string }
  {
    const { email, password } = req.body ?? {};

    if (typeof email !== "string" || !email.trim() || typeof password !== "string" || !password)
    {
      throw new ValidationError(ValidationError.INPUT, "'email' and 'password' are required.", ["email", "password"]);
    }

    return { email, password };
  }

  /**
   * Formats a Firebase Identity Toolkit auth result into this API's
   * response shape.
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
    try
    {
      const { email, password } = this.requireCredentials(req);
      const result: IFirebaseAuthResult = await FirebaseAuthClient.getInstance().signUp(email, password);

      res.status(201).json(this.toAuthResponse(result));
    }
    catch (err)
    {
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
    try
    {
      const { email, password } = this.requireCredentials(req);
      const result: IFirebaseAuthResult = await FirebaseAuthClient.getInstance().signInWithPassword(email, password);

      res.json(this.toAuthResponse(result));
    }
    catch (err)
    {
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
      const refreshToken: unknown = req.body?.refreshToken;

      if (typeof refreshToken !== "string" || !refreshToken)
      {
        throw new ValidationError(ValidationError.INPUT, "'refreshToken' is required.", ["refreshToken"]);
      }

      const result: IFirebaseAuthResult = await FirebaseAuthClient.getInstance().refresh(refreshToken);

      res.json(this.toAuthResponse(result));
    }
    catch (err)
    {
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

      res.json({ success: true, message: "Logged out successfully" });
    }
    catch (err)
    {
      next(err instanceof Error ? err : new AuthError(AuthError.TOKEN_INVALID, "Failed to log out."));
    }
  };

  /**
   * Initializes the routes for this router.
   */

  private initializeRoutes(): void
  {
    this.route("/auth/signup")
      .post(this.signup);

    this.route("/auth/login")
      .post(this.login);

    this.route("/auth/refresh")
      .post(this.refreshToken);

    this.route("/auth/session")
      .get(verifyFirebaseToken, this.getSession);

    this.route("/auth/logout")
      .post(verifyFirebaseToken, this.logout);
  }
}

function Enforce(): void {}

export default FirebaseAuthRouter;
