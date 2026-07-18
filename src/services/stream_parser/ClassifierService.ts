import { IClassifier } from "./io/IClassifier.js";

export interface ClassifierService extends IClassifier {
  reset(jobId: string, fieldSpec: string[], recordTemplates: unknown[], rubbishTemplates: unknown[]): void;
  getHeaderMap(): Record<string, number> | null;
}
