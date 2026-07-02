import { describe, expect, it } from "vitest";
import fc from "fast-check";

import * as index from "../src/index.js";

const runtimeExportKeys = [
  "Client",
  "HEADER_SIZE",
  "INITIAL_STREAM_WINDOW",
  "MAX_UINT32",
  "PROTOCOL_VERSION",
  "Server",
  "Session",
  "Stream",
  "YamuxError",
  "YamuxFlag",
  "YamuxFrameType",
  "YamuxGoAwayCode",
  "abortedError",
  "clientStreamIDs",
  "createClient",
  "createServer",
  "decodeHeader",
  "encodeHeader",
  "frameToBytes",
  "hasFlag",
  "isFrameType",
  "serverStreamIDs",
  "timeoutError",
] as const;

describe("public index fuzzing", () => {
  it("exposes sampled public runtime exports", () => {
    fc.assert(
      fc.property(fc.constantFrom(...runtimeExportKeys), (key) => {
        expect(index[key]).not.toBeUndefined();
      }),
      { numRuns: 100 },
    );
  });
});
