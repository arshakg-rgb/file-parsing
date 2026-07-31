import { IRetry, RetryResponse } from "@service/retry/io/IRetry.js";
import RetryServiceImpl from "@service/retry/impl/RetryServiceImpl.js";

/**
 * Thin wrapper around RetryServiceImpl, kept for backward compatibility
 * with call sites that instantiate `RetryService` directly instead of
 * going through the singleton. Prefer `RetryServiceImpl.getInstance()`
 * for new code.
 *
 * The underlying implementation can be injected for testing; when omitted
 * it defaults to the singleton instance.
 */

export class RetryService implements IRetry
{
  private static defaultInstance: RetryService;

  private readonly service: RetryServiceImpl;

  constructor(service: RetryServiceImpl = RetryServiceImpl.getInstance())
  {
    this.service = service;
  }

  /**
   * Shared instance used by the static backward-compatibility methods
   * and the bootstrap entrypoint below.
   */

  private static getDefault(): RetryService
  {
    if (!RetryService.defaultInstance)
    {
      RetryService.defaultInstance = new RetryService();
    }

    return RetryService.defaultInstance;
  }

  async processRetry(): Promise<RetryResponse>
  {
    return this.service.processRetry();
  }

  async consumerLoop(): Promise<void>
  {
    return this.service.consumerLoop();
  }

  /**
   * Bootstraps the consumer loop when this module is loaded as the
   * service entrypoint. Guarded via RETRY_SERVICE_AUTOSTART so importing
   * this module elsewhere (tests, or another service that only needs
   * `handleDlqEntry`) doesn't trigger a second, competing consumer loop.
   */

  static bootstrap(): void
  {
    const logger = RetryServiceImpl.getInstance().getLogger();
    const instance: RetryService = RetryService.getDefault();

    instance.consumerLoop().catch((err) =>
    {
      logger.error("retry_consumer_failed", err instanceof Error ? err : new Error(String(err)));
      process.exit(1);
    });

    const shutdown = (signal: NodeJS.Signals) =>
    {
      logger.info("retry_consumer_shutting_down", { signal });
      process.exit(0);
    };

    process.once("SIGTERM", shutdown);
    process.once("SIGINT", shutdown);
  }
}

export { IRetry, RetryRequest, RetryResponse } from "@service/retry/io/IRetry.js";

if (process.env.RETRY_SERVICE_AUTOSTART !== "false")
{
  RetryService.bootstrap();
}

export default RetryService;
