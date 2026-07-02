import { describe, expect, it } from "vitest";
import fc from "fast-check";

import { AsyncQueue, withTimeout } from "../src/async.js";

describe("async helper fuzzing", () => {
  it("preserves FIFO order for arbitrary queued values", async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(fc.integer(), { maxLength: 100 }), async (values) => {
        const queue = new AsyncQueue<number>();
        for (const value of values) {
          queue.push(value);
        }

        await expect(Promise.all(values.map(() => queue.shift()))).resolves.toEqual(values);
        expect(queue.length).toBe(0);
      }),
      { numRuns: 200 },
    );
  });

  it("returns arbitrary resolved values before finite timeouts fire", async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer(), fc.integer({ min: 0, max: 20 }), async (value, timeoutMs) => {
        await expect(withTimeout(Promise.resolve(value), timeoutMs)).resolves.toBe(value);
      }),
      { numRuns: 100 },
    );
  });
});
