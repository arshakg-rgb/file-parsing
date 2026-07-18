import { FailureClass } from "../../shared/models/job.js";
import { RecordTemplate, RubbishTemplate } from "../../shared/templateRegistry.js";
import ClassifierServiceImpl from "./impl/ClassifierServiceImpl.js";
import { IClassifier, ClassifyResult } from "./io/IClassifier.js";

/**
 * Legacy LineClassifier class - now a thin wrapper around ClassifierServiceImpl
 * This maintains backward compatibility while using the new service pattern
 */
export class LineClassifier implements IClassifier {
  private service: ClassifierServiceImpl;

  constructor(
    jobId: string,
    fieldSpec: string[],
    recordTemplates: RecordTemplate[],
    rubbishTemplates: RubbishTemplate[]
  ) {
    this.service = ClassifierServiceImpl.getInstance();
    this.service.reset(jobId, fieldSpec, recordTemplates, rubbishTemplates);
  }

  classify(line: string, byteOffset: number, byteLength: number): ClassifyResult {
    return this.service.classify(line, byteOffset, byteLength);
  }

  async classifyWithAI(line: string, contextLines: string[]): Promise<ClassifyResult> {
    return this.service.classifyWithAI(line, contextLines);
  }

  async classifyWithTimeout(line: string, contextLines: string[], timeoutMs: number): Promise<ClassifyResult> {
    return this.service.classifyWithTimeout(line, contextLines, timeoutMs);
  }

  /** Get the header map if a header was detected */
  getHeaderMap(): Record<string, number> | null {
    return this.service.getHeaderMap();
  }
}

// Re-export the new service for direct use
export { ClassifierServiceImpl };
export { default as ClassifierService } from "./impl/ClassifierServiceImpl.js";
export { IClassifier, ClassifyRequest, ClassifyResponse, ClassifyResult } from "./io/IClassifier.js";
export { FIELD_ALIASES, DELIMITERS, TEMPLATE_IDS } from "./io/ClassifierConstants.js";
