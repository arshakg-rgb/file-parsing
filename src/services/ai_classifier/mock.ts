import { MockClassifier } from "./mock/MockClassifier.js";
import type {
  MockClassifyRequest,
  MockClassifyResponse,
} from "./io/IMockClassifier.js";

export type { MockClassifyRequest, MockClassifyResponse };
export { MockClassifier };

export function mockFingerprint(line: string): string {
  return MockClassifier.fingerprint(line);
}

export function mockClassify(req: MockClassifyRequest): MockClassifyResponse {
  return MockClassifier.getInstance().classify(req);
}
