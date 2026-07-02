import { describe, expect, it } from "vitest";
import fc from "fast-check";

import { collect, concatBytes, startHarness, startStream } from "./helpers.js";

describe("yamux stream fuzzing", () => {
  it("preserves arbitrary inbound byte chunks through the readable side", async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(fc.uint8Array({ maxLength: 128 }), { maxLength: 32 }), async (chunks) => {
        const session = startHarness();
        const stream = startStream(session);

        for (const chunk of chunks) {
          await stream.receiveData(chunk);
        }
        await stream.receiveFin();

        await expect(collect(stream.readable)).resolves.toEqual(concatBytes(chunks));
      }),
      { numRuns: 100 },
    );
  });
});
