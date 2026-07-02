import { describe, expect, it } from "vitest";
import fc from "fast-check";

import { createClient } from "../src/client.js";
import { createServer } from "../src/server.js";
import { collect, duplexPair } from "./helpers.js";

describe("yamux session fuzzing", () => {
  it("moves random payloads over browser streams in both directions", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.uint8Array({ minLength: 0, maxLength: 4096 }),
        fc.uint8Array({ minLength: 0, maxLength: 4096 }),
        async (clientPayload, serverPayload) => {
          const pair = duplexPair();
          const client = createClient(pair.client);
          const server = createServer(pair.server);

          try {
            const acceptedByServer = server.acceptStream({ timeoutMs: 1_000 });
            const clientStream = await client.openStream({ timeoutMs: 1_000 });
            const serverInbound = await acceptedByServer;

            const acceptedByClient = client.acceptStream({ timeoutMs: 1_000 });
            const serverStream = await server.openStream({ timeoutMs: 1_000 });
            const clientInbound = await acceptedByClient;

            const clientWriter = clientStream.writable.getWriter();
            const serverWriter = serverStream.writable.getWriter();

            await Promise.all([
              clientWriter.write(clientPayload),
              serverWriter.write(serverPayload),
            ]);
            await Promise.all([
              clientWriter.close(),
              serverWriter.close(),
            ]);

            await expect(collect(serverInbound.readable)).resolves.toEqual(clientPayload);
            await expect(collect(clientInbound.readable)).resolves.toEqual(serverPayload);
          } finally {
            await client.close().catch(() => undefined);
            await server.close().catch(() => undefined);
          }
        },
      ),
      { numRuns: 50 },
    );
  });
});
