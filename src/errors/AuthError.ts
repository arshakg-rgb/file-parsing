import { constants as HttpStatuses } from "http2";
import pino from "pino";
import { CustomError } from "@errors/CustomError.js";
import { createLogger } from "@utils/logger/Log.js";

const logger: pino.Logger = createLogger(module);

/**
 * Class representing a Firebase authentication error.
 * Extends the CustomError class.
 */
export class AuthError extends CustomError
{
    /**
     * Error code for a missing or malformed Authorization header.
     */
    public static TOKEN_MISSING: string = "TOKEN_MISSING";

    /**
     * Error code for a token that failed signature/structure verification.
     */
    public static TOKEN_INVALID: string = "TOKEN_INVALID";

    /**
     * Error code for a token that has expired.
     */
    public static TOKEN_EXPIRED: string = "TOKEN_EXPIRED";

    /**
     * Error code for a token whose session has been revoked.
     */
    public static TOKEN_REVOKED: string = "TOKEN_REVOKED";

    /**
     * Error code for a disabled Firebase user account.
     */
    public static USER_DISABLED: string = "USER_DISABLED";

    /**
     * Error code for requests missing an authenticated user context.
     */
    public static NOT_AUTHENTICATED: string = "NOT_AUTHENTICATED";

    /**
     * Error code for signup attempts using an email that is already registered.
     */
    public static EMAIL_EXISTS: string = "EMAIL_EXISTS";

    /**
     * Error code for login attempts with an unknown email or incorrect password.
     */
    public static INVALID_CREDENTIALS: string = "INVALID_CREDENTIALS";

    /**
     * Error code for signup passwords that don't meet Firebase's minimum strength.
     */
    public static WEAK_PASSWORD: string = "WEAK_PASSWORD";

    /**
     * Error code for malformed email addresses.
     */
    public static INVALID_EMAIL: string = "INVALID_EMAIL";

    /**
     * Error code for rate-limited signup/login attempts.
     */
    public static TOO_MANY_REQUESTS: string = "TOO_MANY_REQUESTS";

    /**
     * Error code for missing/invalid request body fields.
     */
    public static VALIDATION_ERROR: string = "VALIDATION_ERROR";

    /**
     * Error code for an expired or already-used password reset (oob) code.
     */
    public static OOB_CODE_EXPIRED: string = "OOB_CODE_EXPIRED";

    /**
     * Error code for an invalid password reset (oob) code.
     */
    public static OOB_CODE_INVALID: string = "OOB_CODE_INVALID";

    /**
     * Constructs a new instance of the AuthError class.
     * @param code - The error code.
     * @param message - The error message.
     * @param fields - Optional array of fields related to the error.
     * @param info - Optional additional information associated with the error.
     * @param data - Optional additional data associated with the error.
     */
    constructor(code: string, message: string, fields?: string[], info?: Record<string, unknown>, data?: Record<string, unknown>)
    {
        super("AuthError");
        this.code = code;
        this.message = message;
        this.status = this.getStatus();
        this.fields = fields;
        this.info = info;
        this.data = data;
    }

    /**
     * Gets the HTTP status code based on the error code.
     * @returns The HTTP status code.
     */
    private getStatus(): number
    {
        const statusMap: Record<string, number> = {
            [AuthError.TOKEN_MISSING]: HttpStatuses.HTTP_STATUS_UNAUTHORIZED,
            [AuthError.TOKEN_INVALID]: HttpStatuses.HTTP_STATUS_UNAUTHORIZED,
            [AuthError.TOKEN_EXPIRED]: HttpStatuses.HTTP_STATUS_UNAUTHORIZED,
            [AuthError.TOKEN_REVOKED]: HttpStatuses.HTTP_STATUS_UNAUTHORIZED,
            [AuthError.USER_DISABLED]: HttpStatuses.HTTP_STATUS_FORBIDDEN,
            [AuthError.NOT_AUTHENTICATED]: HttpStatuses.HTTP_STATUS_UNAUTHORIZED,
            [AuthError.EMAIL_EXISTS]: HttpStatuses.HTTP_STATUS_CONFLICT,
            [AuthError.INVALID_CREDENTIALS]: HttpStatuses.HTTP_STATUS_UNAUTHORIZED,
            [AuthError.WEAK_PASSWORD]: HttpStatuses.HTTP_STATUS_BAD_REQUEST,
            [AuthError.INVALID_EMAIL]: HttpStatuses.HTTP_STATUS_BAD_REQUEST,
            [AuthError.TOO_MANY_REQUESTS]: HttpStatuses.HTTP_STATUS_TOO_MANY_REQUESTS,
            [AuthError.VALIDATION_ERROR]: HttpStatuses.HTTP_STATUS_BAD_REQUEST,
            [AuthError.OOB_CODE_EXPIRED]: HttpStatuses.HTTP_STATUS_BAD_REQUEST,
            [AuthError.OOB_CODE_INVALID]: HttpStatuses.HTTP_STATUS_BAD_REQUEST
        };

        const status: number | undefined = this.code ? statusMap[this.code] : undefined;

        if (!status)
        {
            logger.warn(`Unknown error status code ${this.code}`);

            return HttpStatuses.HTTP_STATUS_UNAUTHORIZED;
        }

        return status;
    }
}
