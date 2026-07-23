import { describe, it, expect, vi } from "vitest";

vi.mock("@config/system-config/Config.js", () => ({
  default: {
    getInstance: () => ({
      getConfig: () => ({
        settings: {
          MAX_QUOTED_NEWLINES: 0,
          MAX_LINE_BYTES: 1024 * 1024,
        },
      }),
    }),
  },
}));

vi.mock("@config/ServiceManager.js", () => ({
  default: class {
    getConfig() {
      return {
        settings: {
          MAX_QUOTED_NEWLINES: 0,
          MAX_LINE_BYTES: 1024 * 1024,
        },
      };
    }
  },
  Enforce: () => {},
}));

vi.mock("@utils/logger/logger.js", () => ({
  createLogger: () => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  }),
  Logger: class {},
}));

vi.mock("@google-cloud/storage", () => ({
  Storage: class {},
}));

import { splitAllLines } from "./GcsUtils.js";

describe("GcsUtils.splitAllLines", () => {
  it("splits a CSV row with an embedded newline into one line when limit is high", () => {
    const header = "ID,name,location,url,description,protected,verified\n";
    const row12 = "12,jack,usa,http://x.com,\"#bitcoin\",0,0\n";
    const row34 = `34,"Ariel Poler","SF Bay Area",http://www.reveri.com,"Entrepreneur, Mentor, Board Member, Investor.
SUP foiler, Wing Foiler, Kite Foiler.",0,1\n`;
    const footer = "999,footer\n";
    const input = header + row12 + row34 + footer;

    const lines = splitAllLines(Buffer.from(input, "utf-8"), "utf-8", 100);
    const texts = lines.map((t) => t[0]);

    expect(texts).toHaveLength(4);
    expect(texts[0]).toBe("ID,name,location,url,description,protected,verified");
    expect(texts[1]).toBe("12,jack,usa,http://x.com,\"#bitcoin\",0,0");
    expect(texts[2]).toContain("Entrepreneur, Mentor, Board Member, Investor.");
    expect(texts[2]).toContain("SUP foiler, Wing Foiler, Kite Foiler.");
    expect(texts[2]).toContain("34,");
    expect(texts[3]).toBe("999,footer");
  });

  it("preserves embedded newlines inside a quoted field when no quoted-newline limit is provided", () => {
    const input = `header,row
34,"A","B","first
second",0\n`;

    const lines = splitAllLines(Buffer.from(input, "utf-8"), "utf-8");
    const texts = lines.map((t) => t[0]);

    expect(texts).toHaveLength(2);
    expect(texts[1]).toContain("34");
    expect(texts[1]).toContain("first");
    expect(texts[1]).toContain("second");
  });

  it("preserves a quoted field with 100+ embedded newlines as a single row", () => {
    const bio = Array.from({ length: 150 }, (_, i) => `line ${i}`).join("\n");
    const header = "ID,name,description,verified\n";
    const row = `12,jack,"${bio}",0\n`;
    const footer = "999,footer,desc,1\n";
    const input = header + row + footer;

    const lines = splitAllLines(Buffer.from(input, "utf-8"), "utf-8", 100);
    const texts = lines.map((t) => t[0]);

    expect(texts).toHaveLength(3);
    expect(texts[1]).toContain("12,jack,");
    expect(texts[1]).toContain("line 0");
    expect(texts[1]).toContain("line 149");
    expect(texts[2]).toBe("999,footer,desc,1");
  });
});
