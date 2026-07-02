import { describe, expect, it } from "vitest";
import fc from "fast-check";

import { YamuxError, type YamuxErrorCode } from "../src/errors.js";

const errorCodes: YamuxErrorCode[] = [
  "ABORTED",
  "CONNECTION_RESET",
  "DUPLICATE_STREAM",
  "GO_AWAY",
  "INTERNAL_ERROR",
  "INVALID_FRAME",
  "INVALID_STREAM",
  "PROTOCOL_ERROR",
  "RECEIVE_WINDOW_EXCEEDED",
  "SESSION_CLOSED",
  "STREAM_CLOSED",
  "STREAMS_EXHAUSTED",
  "TIMEOUT",
];

describe("yamux error fuzzing", () => {
  it("preserves arbitrary messages for every error code", () => {
    fc.assert(
      fc.property(fc.constantFrom(...errorCodes), fc.string({ maxLength: 200 }), (code, message) => {
        const error = new YamuxError(code, message);

        expect(error.name).toBe("YamuxError");
        expect(error.code).toBe(code);
        expect(error.message).toBe(message);
      }),
      { numRuns: 200 },
    );
  });
});
