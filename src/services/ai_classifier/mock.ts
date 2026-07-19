import { MockClassifier } from "@service/ai_classifier/mock/MockClassifier.js";
import type {
  MockClassifyRequest,
  MockClassifyResponse,
} from "@service/ai_classifier/io/IMockClassifier.js";

export type { MockClassifyRequest, MockClassifyResponse };
export { MockClassifier };

/**
 * Performs the mock fingerprint operation.
 * @param line - The line to process
 * @returns The string result
 */
export function mockFingerprint(line: string): string {
  return MockClassifier.fingerprint(line);
}

/**
 * Performs the mock classify operation.
 * @param req - The HTTP request object
 * @returns The mock classify response result
 */
export function mockClassify(req: MockClassifyRequest): MockClassifyResponse {
  return MockClassifier.getInstance().classify(req);
}
