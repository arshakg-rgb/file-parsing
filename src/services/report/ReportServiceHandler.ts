import { IReport, ReportRequest, ReportResponse } from "@service/report/io/IReport.js";
import ReportServiceImpl from "@service/report/impl/ReportServiceImpl.js";
import { ReportMessage } from "@shared/models/job.js";

/**
 * Legacy ReportService class - now a thin wrapper around ReportServiceImpl
 * This maintains backward compatibility while using the new service pattern
 */
export class ReportService implements IReport {
    /**
   * Service
   * @private
   */
  private service: ReportServiceImpl;

    /**
   * Constructs a new ReportService instance.
   */
  constructor() {
    this.service = ReportServiceImpl.getInstance();
  }

    /**
   * Processes report
   * @param req - The HTTP request object
   * @returns A promise that resolves to the result
   */
  async processReport(req: ReportRequest): Promise<ReportResponse> {
    return this.service.processReport(req);
  }

    /**
   * Performs the generate report operation.
   * @param msg - The msg
   */
  async generateReport(msg: ReportMessage): Promise<void> {
    return this.service.generateReport(msg);
  }

    /**
   * Performs the consumer loop operation.
   */
  async consumerLoop(): Promise<void> {
    return this.service.consumerLoop();
  }
}

// Re-export the new service for direct use
export { default as ReportServiceImpl } from "@service/report/impl/ReportServiceImpl.js";
export { IReport, ReportRequest, ReportResponse } from "@service/report/io/IReport.js";

// Backward compatibility wrappers
const reportService = new ReportService();

/**
 * Performs the generate report operation.
 * @param msg - The msg
 */
export async function generateReport(msg: ReportMessage): Promise<void> {
  return reportService.generateReport(msg);
}

/**
 * Performs the consumer loop operation.
 */
export async function consumerLoop(): Promise<void> {
  return reportService.consumerLoop();
}

// Auto-start the service when module is loaded
reportService.consumerLoop().catch(err => {
  console.error("report_consumer_failed", { error: String(err) });
  process.exit(1);
});

export default ReportService;
