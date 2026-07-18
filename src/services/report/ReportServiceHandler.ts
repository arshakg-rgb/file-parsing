import { IReport, ReportRequest, ReportResponse } from "./io/IReport.js";
import ReportServiceImpl from "./impl/ReportServiceImpl.js";
import { ReportMessage } from "../../shared/models/job.js";

/**
 * Legacy ReportService class - now a thin wrapper around ReportServiceImpl
 * This maintains backward compatibility while using the new service pattern
 */
export class ReportService implements IReport {
  private service: ReportServiceImpl;

  constructor() {
    this.service = ReportServiceImpl.getInstance();
  }

  async processReport(req: ReportRequest): Promise<ReportResponse> {
    return this.service.processReport(req);
  }

  async generateReport(msg: ReportMessage): Promise<void> {
    return this.service.generateReport(msg);
  }

  async consumerLoop(): Promise<void> {
    return this.service.consumerLoop();
  }
}

// Re-export the new service for direct use
export { default as ReportServiceImpl } from "./impl/ReportServiceImpl.js";
export { IReport, ReportRequest, ReportResponse } from "./io/IReport.js";

// Backward compatibility wrappers
const reportService = new ReportService();

export async function generateReport(msg: ReportMessage): Promise<void> {
  return reportService.generateReport(msg);
}

export async function consumerLoop(): Promise<void> {
  return reportService.consumerLoop();
}

// Auto-start the service when module is loaded
reportService.consumerLoop().catch(err => {
  console.error("report_consumer_failed", { error: String(err) });
  process.exit(1);
});

export default ReportService;
