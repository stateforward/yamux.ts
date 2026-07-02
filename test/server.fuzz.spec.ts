import { describe, expect, it } from "vitest";
import fc from "fast-check";

import { createClient } from "../src/client.js";
import { createServer } from "../src/server.js";
import { duplexPair } from "./helpers.js";

describe("yamux server fuzzing", () => {
  it("allocates even stream ids for arbitrary server-open counts", async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 5 }), async (count) => {
        const pair = duplexPair();
        const client = createClient(pair.client);
        const server = createServer(pair.server);

        try {
          const accepted = Array.from({ length: count }, () => client.acceptStream({ timeoutMs: 1_000 }));
          const opened = [];

          for (let index = 0; index < count; index += 1) {
            opened.push(await server.openStream({ timeoutMs: 1_000 }));
          }

          await Promise.all(accepted);
          expect(opened.map((stream) => stream.id)).toEqual(
            Array.from({ length: count }, (_, index) => 2 + index * 2),
          );
        } finally {
          await client.close().catch(() => undefined);
          await server.close().catch(() => undefined);
        }
      }),
      { numRuns: 40 },
    );
  });
});
