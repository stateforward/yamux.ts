import { describe, expect, it } from "vitest";

import { createClient } from "../src/client.js";
import { Server, createServer, serverStreamIDs } from "../src/server.js";
import { duplexPair, pendingReadable, writableSink } from "./helpers.js";

describe("yamux server", () => {
  it("uses the server role and even stream id sequence", () => {
    const server = new Server({
      readable: pendingReadable(),
      writable: writableSink(),
    });

    expect(server.role).toBe("server");
    expect(serverStreamIDs).toEqual({ first: 2, step: 2 });
  });

  it("opens server streams with the correct id parity", async () => {
    const pair = duplexPair();
    const client = createClient(pair.client);
    const server = createServer(pair.server);

    const acceptedByClient = client.acceptStream({ timeoutMs: 1_000 });
    const serverStream = await server.openStream({ timeoutMs: 1_000 });
    const clientStream = await acceptedByClient;

    expect(serverStream.id).toBe(2);
    expect(clientStream.id).toBe(2);

    await client.close();
    await server.close();
  });
});
