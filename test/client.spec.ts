import { describe, expect, it } from "vitest";

import { Client, clientStreamIDs, createClient } from "../src/client.js";
import { createServer } from "../src/server.js";
import { duplexPair, pendingReadable, writableSink } from "./helpers.js";

describe("yamux client", () => {
  it("uses the client role and odd stream id sequence", () => {
    const client = new Client({
      readable: pendingReadable(),
      writable: writableSink(),
    });

    expect(client.role).toBe("client");
    expect(clientStreamIDs).toEqual({ first: 1, step: 2 });
  });

  it("opens a client stream, accepts it on the server, and transfers data", async () => {
    const pair = duplexPair();
    const client = createClient(pair.client);
    const server = createServer(pair.server);

    const accepted = server.acceptStream({ timeoutMs: 1_000 });
    const outbound = await client.openStream({ timeoutMs: 1_000 });
    const inbound = await accepted;

    expect(outbound.id).toBe(1);
    expect(inbound.id).toBe(1);

    const writer = outbound.writable.getWriter();
    await writer.write(new TextEncoder().encode("hello"));
    await writer.close();

    const reader = inbound.readable.getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("hello");
    expect((await reader.read()).done).toBe(true);
    reader.releaseLock();

    await client.close();
    await server.close();
  });
});
