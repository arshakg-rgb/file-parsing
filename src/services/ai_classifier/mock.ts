import { MockClassifier } from "@service/ai_classifier/mock/MockClassifier.js";
import type {
  MockClassifyRequest,
  MockClassifyResponse,
} from "@service/ai_classifier/io/IMockClassifier.js";

export type { MockClassifyRequest, MockClassifyResponse };
export { MockClassifier };

export function mockFingerprint(line: string): string {
  return MockClassifier.fingerprint(line);
}

export function mockClassify(req: MockClassifyRequest): MockClassifyResponse {
  return MockClassifier.getInstance().classify(req);
}
