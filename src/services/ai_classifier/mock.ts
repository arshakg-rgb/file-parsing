import {
  MockClassifier,
  MockClassifyRequest,
  MockClassifyResponse,
} from "./mock/MockClassifier.js";

export type { MockClassifyRequest, MockClassifyResponse };
export { MockClassifier };

export function mockFingerprint(line: string): string {
  return MockClassifier.fingerprint(line);
}

export function mockClassify(req: MockClassifyRequest): MockClassifyResponse {
  return MockClassifier.getInstance().classify(req);
}
