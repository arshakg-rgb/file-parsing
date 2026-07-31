import { ILoad, LoadResponse } from "@service/load/io/ILoad.js";
import LoadServiceImpl from "@service/load/impl/LoadServiceImpl.js";

/**
 * Thin wrapper around LoadServiceImpl, kept for backward compatibility
 * with call sites that instantiate `LoadService` directly instead of
 * going through the singleton. Prefer `LoadServiceImpl.getInstance()`
 * for new code.
 *
 * The underlying implementation can be injected for testing; when omitted
 * it defaults to the singleton instance.
 */

export class LoadServiceHandler implements ILoad
{
  private static defaultInstance: LoadServiceHandler;

  private readonly service: LoadServiceImpl;

  constructor(service: LoadServiceImpl = LoadServiceImpl.getInstance())
  {
    this.service = service;
  }

  /**
   * Shared instance used by the static backward-compatibility methods
   * and the bootstrap entrypoint below.
   */

  private static getDefault(): LoadServiceHandler
  {
    if (!LoadServiceHandler.defaultInstance)
    {
      LoadServiceHandler.defaultInstance = new LoadServiceHandler();
    }

    return LoadServiceHandler.defaultInstance;
  }

  async processLoad(): Promise<LoadResponse>
  {
    return this.service.processLoad();
  }

  async consumerLoop(): Promise<void>
  {
    return this.service.consumerLoop();
  }

  /**
   * Bootstraps the consumer loop when this module is loaded as the
   * service entrypoint. Guarded via LOAD_SERVICE_AUTOSTART so importing
   * this module elsewhere (tests, or another service that only needs
   * `processLoad`) doesn't trigger a second, competing consumer loop.
   */

  static bootstrap(): void
  {
    const logger = LoadServiceImpl.getInstance().getLogger();
    const instance: LoadServiceHandler = LoadServiceHandler.getDefault();

    instance.consumerLoop().catch((err) =>
    {
      logger.error(
          "load_consumer_failed",
          err instanceof Error ? err : new Error(String(err))
      );
      process.exit(1);
    });

    const shutdown = (signal: NodeJS.Signals) =>
    {
      logger.info("load_consumer_shutting_down", { signal });
      process.exit(0);
    };

    process.once("SIGTERM", shutdown);
    process.once("SIGINT", shutdown);
  }
}

export { ILoad, LoadRequest, LoadResponse } from "@service/load/io/ILoad.js";

if (process.env.LOAD_SERVICE_AUTOSTART !== "false")
{
  LoadServiceHandler.bootstrap();
}

export default LoadServiceHandler;
