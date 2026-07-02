import { describe, expect, it } from "vitest";

import * as index from "../src/index.js";

describe("public index", () => {
  it("exports the browser yamux API", () => {
    expect(index.createClient).toBeTypeOf("function");
    expect(index.createServer).toBeTypeOf("function");
    expect(index.Session).toBeTypeOf("function");
    expect(index.Client).toBeTypeOf("function");
    expect(index.Server).toBeTypeOf("function");
    expect(index.Stream).toBeTypeOf("function");
    expect(index.YamuxError).toBeTypeOf("function");
    expect(index.HEADER_SIZE).toBe(12);
  });
});
