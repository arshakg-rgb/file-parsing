import { IAiClassifier, ClassifyRequest, ClassifyResponse } from "./io/IAiClassifier.js";
import AiClassifierServiceImpl from "./impl/AiClassifierServiceImpl.js";
import { RecordTemplate } from "../../shared/templateRegistry.js";

/**
 * Legacy AiClassifierService class - now a thin wrapper around AiClassifierServiceImpl
 * This maintains backward compatibility while using the new service pattern
 */
export class AiClassifierService implements IAiClassifier {
  private service: AiClassifierServiceImpl;

  constructor() {
    this.service = AiClassifierServiceImpl.getInstance();
  }

  async classifyAi(req: ClassifyRequest): Promise<ClassifyResponse> {
    return this.service.classifyAi(req);
  }

  async validateTemplate(req: ClassifyRequest, tmpl: RecordTemplate): Promise<boolean> {
    return this.service.validateTemplate(req, tmpl);
  }
}

// Re-export the new service for direct use
export { default as AiClassifierServiceImpl } from "./impl/AiClassifierServiceImpl.js";
export { IAiClassifier, ClassifyRequest, ClassifyResponse } from "./io/IAiClassifier.js";

// Backward compatibility wrappers
const aiService = new AiClassifierService();

export async function classifyAi(req: ClassifyRequest): Promise<ClassifyResponse> {
  return aiService.classifyAi(req);
}

export async function validateTemplate(req: ClassifyRequest, tmpl: RecordTemplate): Promise<boolean> {
  return aiService.validateTemplate(req, tmpl);
}

export default AiClassifierService;
