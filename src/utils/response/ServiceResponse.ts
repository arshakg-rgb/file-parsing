import { IResponsePages } from "@utils/pagination/app/io/IPagination";
import { Response } from "express";
import pino from "pino";
import { createLogger } from "../logger/Log";

const logger: pino.Logger = createLogger(module);

/**
 * Class representing a service response.
 * This class is used to standardize the structure of responses sent by the service.
 */
export class ServiceResponse
{
    private _originalResponse: Response;
    private _status: number = 200;
    private _outcome: unknown = null;
    private _pages: IResponsePages;
    private readonly _success: boolean = true;
    private _errors: unknown[] = [];

    /**
     * Creates an instance of `ServiceResponse`.
     *
     * @param {Response} response - The original Express response object.
     * @param {boolean} [success=true] - Indicates whether the response is successful.
     */
    constructor(response: Response, success: boolean = true)
    {
        this._originalResponse = response;
        this._success = success;
    }

    /**
     * Sets the HTTP status code for the response.
     *
     * @param {number} status - The HTTP status code.
     * @returns {ServiceResponse} The current instance for chaining.
     */
    public setStatus(status: number): ServiceResponse
    {
        this._status = status;

        return this;
    }

    /**
     * Sets the outcome data and optional errors for the response.
     *
     * @param {unknown} data - The outcome data to be sent in the response.
     * @param pages
     * @param {unknown[]} [errors] - Optional array of errors.
     * @returns {ServiceResponse} The current instance for chaining.
     */
    public setOutcome(data: unknown, pages?: IResponsePages, errors?: unknown[]): ServiceResponse
    {
        this._outcome = data;

        if (errors && errors.length > 0)
        {
            this._errors = errors;
        }

        if (pages != null)
        {
            this._pages = pages;
        }

        return this;
    }

    /**
     * Sends the response to the client.
     * Constructs the response payload and sends it using the original Express response object.
     *
     * @returns {ServiceResponse} The current instance for chaining.
     */
    public send(): ServiceResponse
    {
        if (this._originalResponse)
        {
            const responseData: {
                success: boolean;
                data?: unknown;
                pages?: IResponsePages;
                errors?: unknown[];
            } = {
                success: this._success,
                data: this._outcome || undefined,
                errors: this._errors.length ? this._errors : undefined
            };

            if (this._pages)
            {
                responseData.pages = this._pages;
            }

            this._originalResponse.status(this._status).json(responseData);
            this.clear();
        }
        else
        {
            logger.warn("ServiceResponse already closed, response has been sent.");
        }

        return this;
    }

    /**
     * Clears the internal state of the `ServiceResponse` instance.
     * Resets the original response, outcome, and errors.
     */
    private clear(): void
    {
        this._originalResponse = null;
        this._outcome = null;
        this._errors = [];
    }
}
