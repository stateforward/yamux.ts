import { describe, expect, it } from "vitest";

import { abortedError, timeoutError, YamuxError } from "../src/errors.js";

describe("yamux errors", () => {
  it("constructs typed helper errors", () => {
    expect(abortedError()).toMatchObject({ name: "YamuxError", code: "ABORTED" });
    expect(timeoutError()).toMatchObject({ name: "YamuxError", code: "TIMEOUT" });
  });

  it("stores explicit codes and messages", () => {
    const cause = new Error("root cause");
    const error = new YamuxError("PROTOCOL_ERROR", "bad frame", { cause });

    expect(error.name).toBe("YamuxError");
    expect(error.code).toBe("PROTOCOL_ERROR");
    expect(error.message).toBe("bad frame");
    expect(error.cause).toBe(cause);
  });
});
