import { describe, expect, it } from "vitest";
import fc from "fast-check";

import {
  HEADER_SIZE,
  YamuxFlag,
  YamuxFrameType,
  decodeHeader,
  encodeHeader,
  frameToBytes,
  hasFlag,
} from "../src/protocol.js";

const frameTypeArbitrary = fc.constantFrom(
  YamuxFrameType.Data,
  YamuxFrameType.WindowUpdate,
  YamuxFrameType.Ping,
  YamuxFrameType.GoAway,
);

const flagsArbitrary = fc.integer({ min: 0, max: 0xf });
const uint32Arbitrary = fc.integer({ min: 0, max: 0xffff_ffff });

describe("yamux protocol fuzzing", () => {
  it("round-trips random headers through the big-endian codec", () => {
    fc.assert(
      fc.property(frameTypeArbitrary, flagsArbitrary, uint32Arbitrary, uint32Arbitrary, (type, flags, streamID, length) => {
        const encoded = encodeHeader(type, flags, streamID, length);

        expect(encoded).toHaveLength(HEADER_SIZE);
        expect(decodeHeader(encoded)).toEqual({
          version: 0,
          type,
          flags,
          streamID,
          length,
        });
      }),
      { numRuns: 500 },
    );
  });

  it("preserves data frame payload bytes", () => {
    fc.assert(
      fc.property(
        flagsArbitrary,
        uint32Arbitrary.filter((streamID) => streamID > 0),
        fc.uint8Array({ minLength: 0, maxLength: 512 }),
        (flags, streamID, payload) => {
          const bytes = frameToBytes(YamuxFrameType.Data, flags, streamID, payload.byteLength, payload);
          const header = decodeHeader(bytes.subarray(0, HEADER_SIZE));

          expect(header.length).toBe(payload.byteLength);
          expect(bytes.subarray(HEADER_SIZE)).toEqual(payload);
          expect(hasFlag(flags, YamuxFlag.SYN)).toBe((flags & YamuxFlag.SYN) === YamuxFlag.SYN);
        },
      ),
      { numRuns: 300 },
    );
  });
});
