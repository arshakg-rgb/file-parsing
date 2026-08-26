import { InstantiationError } from "@errors/InstantiationError.js";
import { AuthError } from "@errors/AuthError.js";
import { settings } from "@shared/Settings.js";

/**
 * Shape of a successful response from Identity Toolkit's signUp /
 * signInWithPassword / token endpoints.
 */
export interface IFirebaseAuthResult {
  idToken: string;
  refreshToken: string;
  expiresIn: string;
  localId: string;
  email?: string;
}

/**
 * Shape of Identity Toolkit's error envelope.
 * @see https://firebase.google.com/docs/reference/rest/auth
 */
interface IIdentityToolkitErrorResponse {
  error?: {
    code?: number;
    message?: string;
  };
}

/**
 * FirebaseAuthClient is a singleton wrapper around the public Firebase
 * Identity Toolkit REST API (signUp / signInWithPassword / token refresh).
 *
 * This lets this backend own `/auth/signup`, `/auth/login`, and
 * `/auth/refresh` endpoints: the client calls Firebase directly using the
 * project's Web API key, and only ever hands the caller the resulting
 * Firebase ID/refresh tokens. Session verification (`verifyFirebaseToken`)
 * and logout (`revokeRefreshTokens`) still use the Admin SDK via
 * {@link FirebaseAdmin}.
 */
export class FirebaseAuthClient
{
  /**
   * Singleton instance.
   * @private
   */

  private static instance: FirebaseAuthClient;

  /**
   * Base URL for the Identity Toolkit REST API.
   * @private
   */

  private static readonly BASE_URL: string = "https://identitytoolkit.googleapis.com/v1";

  /**
   * Base URL for the Secure Token refresh API.
   * @private
   */

  private static readonly TOKEN_URL: string = "https://securetoken.googleapis.com/v1";

  /**
   * Constructs a new FirebaseAuthClient instance.
   * @param enforce - A function to enforce the Singleton pattern.
   * @throws {InstantiationError} if instantiated directly instead of via {@link getInstance}
   */

  private constructor(enforce: () => void)
  {
    if (enforce !== Enforce)
    {
      throw new InstantiationError(InstantiationError.NOT_INSTANTIABLE, "Cannot instantiate FirebaseAuthClient directly. Use getInstance()");
    }
  }

  /**
   * Gets the single instance of the FirebaseAuthClient class.
   * @returns The single instance of the class.
   */

  public static getInstance(): FirebaseAuthClient
  {
    if (!FirebaseAuthClient.instance)
    {
      FirebaseAuthClient.instance = new FirebaseAuthClient(Enforce);
    }

    return FirebaseAuthClient.instance;
  }

  /**
   * Gets the configured Firebase Web API key, throwing a clear error if unset.
   * @returns The API key.
   * @private
   */

  private getApiKey(): string
  {
    if (!settings.FIREBASE_WEB_API_KEY)
    {
      throw new Error("FIREBASE_WEB_API_KEY is not configured. Set it in the environment to enable signup/login.");
    }

    return settings.FIREBASE_WEB_API_KEY;
  }

  /**
   * Creates a new Firebase user with the given email/password.
   * @param email - The new user's email address.
   * @param password - The new user's password (Firebase requires 6+ characters).
   * @returns The resulting ID/refresh token pair.
   */

  public async signUp(email: string, password: string): Promise<IFirebaseAuthResult>
  {
    return this.request<IFirebaseAuthResult>("accounts:signUp", { email, password, returnSecureToken: true });
  }

  /**
   * Signs an existing Firebase user in with email/password.
   * @param email - The user's email address.
   * @param password - The user's password.
   * @returns The resulting ID/refresh token pair.
   */

  public async signInWithPassword(email: string, password: string): Promise<IFirebaseAuthResult>
  {
    return this.request<IFirebaseAuthResult>("accounts:signInWithPassword", { email, password, returnSecureToken: true });
  }

  /**
   * Requests a password-reset email for the given address via Identity
   * Toolkit's out-of-band (oob) code flow. Resolves silently even if the
   * caller-visible result should be generic (callers should not surface
   * `EMAIL_NOT_FOUND` to avoid leaking which addresses have accounts).
   * @param email - The account email to send the reset link to.
   */

  public async sendPasswordResetEmail(email: string): Promise<void>
  {
    await this.request<{ email: string }>("accounts:sendOobCode", { requestType: "PASSWORD_RESET", email });
  }

  /**
   * Completes a password reset using the `oobCode` from the reset email.
   * @param oobCode - The out-of-band code delivered to the user's email.
   * @param newPassword - The new password to set (Firebase requires 6+ characters).
   * @returns The email address whose password was reset.
   */

  public async resetPassword(oobCode: string, newPassword: string): Promise<{ email: string }>
  {
    return this.request<{ email: string }>("accounts:resetPassword", { oobCode, newPassword });
  }

  /**
   * Exchanges a refresh token for a new ID/refresh token pair, allowing a
   * client to obtain a fresh ID token after the previous one expired
   * without the user having to log in again.
   * @param refreshToken - A previously issued Firebase refresh token.
   * @returns The resulting ID/refresh token pair.
   */

  public async refresh(refreshToken: string): Promise<IFirebaseAuthResult>
  {
    const apiKey: string = this.getApiKey();
    const response: Response = await fetch(`${FirebaseAuthClient.TOKEN_URL}/token?key=${apiKey}`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ grant_type: "refresh_token", refresh_token: refreshToken }).toString(),
    });

    const body = await response.json() as Record<string, string> & IIdentityToolkitErrorResponse;

    if (!response.ok)
    {
      throw this.mapError(body);
    }

    return {
      idToken: body.id_token,
      refreshToken: body.refresh_token,
      expiresIn: body.expires_in,
      localId: body.user_id,
    };
  }

  /**
   * Performs a POST request against an Identity Toolkit `accounts:*` endpoint.
   * @param path - The endpoint path (e.g. `accounts:signUp`).
   * @param body - The JSON request body.
   * @returns The parsed successful response.
   * @private
   */

  private async request<T>(path: string, body: Record<string, unknown>): Promise<T>
  {
    const apiKey: string = this.getApiKey();
    const response: Response = await fetch(`${FirebaseAuthClient.BASE_URL}/${path}?key=${apiKey}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    const responseBody = await response.json() as T & IIdentityToolkitErrorResponse;

    if (!response.ok)
    {
      throw this.mapError(responseBody);
    }

    return responseBody;
  }

  /**
   * Maps an Identity Toolkit error envelope to an {@link AuthError} with an
   * appropriate code and user-facing message.
   * @param body - The parsed error response body.
   * @returns The mapped AuthError.
   * @private
   */

  private mapError(body: IIdentityToolkitErrorResponse): AuthError
  {
    const message: string = body.error?.message || "";

    switch (message)
    {
      case "EMAIL_EXISTS":
        return new AuthError(AuthError.EMAIL_EXISTS, "An account with this email already exists.");
      case "EMAIL_NOT_FOUND":
      case "INVALID_PASSWORD":
      case "INVALID_LOGIN_CREDENTIALS":
        return new AuthError(AuthError.INVALID_CREDENTIALS, "Invalid email or password.");
      case "USER_DISABLED":
        return new AuthError(AuthError.USER_DISABLED, "This user account has been disabled.");
      case "TOO_MANY_ATTEMPTS_TRY_LATER":
        return new AuthError(AuthError.TOO_MANY_REQUESTS, "Too many attempts. Please try again later.");
      case "INVALID_EMAIL":
        return new AuthError(AuthError.INVALID_EMAIL, "The email address is badly formatted.");
      case "MISSING_PASSWORD":
      case "MISSING_EMAIL":
        return new AuthError(AuthError.VALIDATION_ERROR, "Email and password are required.");
      case "INVALID_REFRESH_TOKEN":
      case "TOKEN_EXPIRED":
        return new AuthError(AuthError.TOKEN_INVALID, "Refresh token is invalid or expired. Please log in again.");
      case "INVALID_OOB_CODE":
        return new AuthError(AuthError.OOB_CODE_INVALID, "This password reset link is invalid or has already been used.");
      case "EXPIRED_OOB_CODE":
        return new AuthError(AuthError.OOB_CODE_EXPIRED, "This password reset link has expired. Please request a new one.");
      default:
        if (message.startsWith("WEAK_PASSWORD"))
        {
          return new AuthError(AuthError.WEAK_PASSWORD, "Password should be at least 6 characters.");
        }

        return new AuthError(AuthError.TOKEN_INVALID, message || "Authentication request failed.");
    }
  }
}

function Enforce(): void {}

export default FirebaseAuthClient;
