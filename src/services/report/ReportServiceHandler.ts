import { IReport, ReportResponse } from "@service/report/io/IReport.js";
import ReportServiceImpl from "@service/report/impl/ReportServiceImpl.js";

/**
 * Thin wrapper around ReportServiceImpl, kept for backward compatibility
 * with call sites that instantiate `ReportService` directly instead of
 * going through the singleton. Prefer `ReportServiceImpl.getInstance()`
 * for new code.
 *
 * The underlying implementation can be injected for testing; when omitted
 * it defaults to the singleton instance.
 */

export class ReportService implements IReport
{
  private static defaultInstance: ReportService;

  private readonly service: ReportServiceImpl;

  constructor(service: ReportServiceImpl = ReportServiceImpl.getInstance())
  {
    this.service = service;
  }

  /**
   * Shared instance used by the static backward-compatibility methods
   * and the bootstrap entrypoint below.
   */

  private static getDefault(): ReportService
  {
    if (!ReportService.defaultInstance)
    {
      ReportService.defaultInstance = new ReportService();
    }

    return ReportService.defaultInstance;
  }

  async processReport(): Promise<ReportResponse>
  {
    return this.service.processReport();
  }

  async consumerLoop(): Promise<void>
  {
    return this.service.consumerLoop();
  }

  /**
   * Bootstraps the consumer loop when this module is loaded as the
   * service entrypoint. Guarded via REPORT_SERVICE_AUTOSTART so importing
   * this module elsewhere (tests, or another service that only needs
   * `generateReport`) doesn't trigger a second, competing consumer loop.
   */

  static bootstrap(): void
  {
    const logger = ReportServiceImpl.getInstance().getLogger();
    const instance: ReportService = ReportService.getDefault();

    instance.consumerLoop().catch((err) =>
    {
      logger.error(
          "report_consumer_failed",
          err instanceof Error ? err : new Error(String(err))
      );
      process.exit(1);
    });

    const shutdown = (signal: NodeJS.Signals) =>
    {
      logger.info("report_consumer_shutting_down", { signal });
      process.exit(0);
    };

    process.once("SIGTERM", shutdown);
    process.once("SIGINT", shutdown);
  }
}

export { default as ReportServiceImpl } from "@service/report/impl/ReportServiceImpl.js";
export { IReport, ReportRequest, ReportResponse } from "@service/report/io/IReport.js";

if (process.env.REPORT_SERVICE_AUTOSTART !== "false")
{
  ReportService.bootstrap();
}

export default ReportService;
