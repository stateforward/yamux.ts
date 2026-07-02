import { describe, expect, it } from "vitest";
import fc from "fast-check";

import { ByteReader } from "../src/byte-reader.js";
import { chunksReadable, concatBytes } from "./helpers.js";

describe("byte reader fuzzing", () => {
  it("reassembles arbitrary chunk boundaries exactly", async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(fc.uint8Array({ maxLength: 64 }), { maxLength: 50 }), async (chunks) => {
        const expected = concatBytes(chunks);
        await expect(new ByteReader(chunksReadable(chunks)).readExactly(expected.byteLength)).resolves.toEqual(expected);
      }),
      { numRuns: 200 },
    );
  });
});
