import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { Readable, Writable } from "node:stream";

import { createClient, createServer } from "../dist/index.js";

const conformanceDir = fileURLToPath(new URL(".", import.meta.url));
const timeoutMs = Number.parseInt(process.env.YAMUX_CONFORMANCE_TIMEOUT_MS ?? "10000", 10);

const cases = [
  {
    name: "ts client -> go server echo: empty payload",
    run: () => runTypeScriptClientEchoCase(payload(0, 1)),
  },
  {
    name: "ts client -> go server echo: small payload",
    run: () => runTypeScriptClientEchoCase(payload(1024, 2)),
  },
  {
    name: "ts client -> go server echo: window-sized payload",
    run: () => runTypeScriptClientEchoCase(payload(320 * 1024, 3)),
  },
  {
    name: "go client -> ts server echo: empty payload",
    run: () => runGoClientEchoCase(payload(0, 4)),
  },
  {
    name: "go client -> ts server echo: small payload",
    run: () => runGoClientEchoCase(payload(1024, 5)),
  },
  {
    name: "go client -> ts server echo: window-sized payload",
    run: () => runGoClientEchoCase(payload(320 * 1024, 6)),
  },
  {
    name: "ts client -> go server: many concurrent streams",
    run: () => runTypeScriptManyStreamsCase(16, 4096),
  },
  {
    name: "go client -> ts server: many concurrent streams",
    run: () => runGoManyStreamsCase(16, 4096),
  },
  {
    name: "ts client -> go server: reply after client FIN",
    run: () => runTypeScriptHalfCloseCase(payload(2048, 7)),
  },
  {
    name: "go client -> ts server: reply after client FIN",
    run: () => runGoHalfCloseCase(payload(2048, 8)),
  },
  {
    name: "ts client reset is observed by go server",
    run: () => runTypeScriptResetCase(),
  },
  {
    name: "go reset is observed by ts client",
    run: () => runGoResetCase(),
  },
  {
    name: "go GOAWAY prevents new ts streams",
    run: () => runGoAwayToTypeScriptCase(),
  },
  {
    name: "ts GOAWAY prevents new go streams",
    run: () => runTypeScriptGoAwayCase(),
  },
  {
    name: "go peer close rejects pending ts accept",
    run: () => runGoPeerCloseCase(),
  },
  {
    name: "go malformed frame fails ts session",
    run: () => runMalformedGoFrameCase(),
  },
  {
    name: "ts malformed frame fails go session",
    run: () => runMalformedTypeScriptFrameCase(),
  },
];

let passed = 0;
for (const testCase of cases) {
  await testCase.run();
  passed += 1;
  console.log(`ok ${passed} - ${testCase.name}`);
}

console.log(`conformance passed: ${passed}/${cases.length}`);

async function runTypeScriptClientEchoCase(expected) {
  const native = spawnNative("server-echo");
  const client = createClient(native.transport);

  try {
    await withTimeout(client.ping({ timeoutMs }), "typescript client ping to go server timed out");
    const stream = await withTimeout(client.openStream({ timeoutMs }), "typescript client open timed out");
    const writer = stream.writable.getWriter();
    const received = collect(stream.readable);

    await Promise.all([
      withTimeout(
        (async () => {
          if (expected.byteLength > 0) {
            await writer.write(expected);
          }
          await writer.close();
          writer.releaseLock();
        })(),
        "typescript client write to go server timed out",
      ),
      withTimeout(received, "typescript client read from go server timed out"),
    ]).then(([, actual]) => assertBytesEqual(actual, expected));

    await withTimeout(client.close(), "typescript client close timed out");
    await native.wait();
  } catch (error) {
    native.kill();
    throw withNativeStderr(error, native);
  }
}

async function runGoClientEchoCase(expected) {
  const native = spawnNative("client-roundtrip", {
    YAMUX_PAYLOAD_BASE64: Buffer.from(expected).toString("base64"),
  });
  const server = createServer(native.transport);

  try {
    const stream = await withTimeout(server.acceptStream({ timeoutMs }), "typescript server accept timed out");
    const actual = await withTimeout(readExactly(stream.readable, expected.byteLength), "typescript server read timed out");
    assertBytesEqual(actual, expected);

    const writer = stream.writable.getWriter();
    if (actual.byteLength > 0) {
      await withTimeout(writer.write(actual), "typescript server write timed out");
    }
    await withTimeout(writer.close(), "typescript server stream close timed out");
    writer.releaseLock();

    await server.close().catch(() => undefined);
    await native.wait();
  } catch (error) {
    native.kill();
    throw withNativeStderr(error, native);
  }
}

async function runTypeScriptManyStreamsCase(count, length) {
  const native = spawnNative("server-echo");
  const client = createClient(native.transport);

  try {
    await withTimeout(client.ping({ timeoutMs }), "typescript client ping to go server timed out");
    await Promise.all(Array.from({ length: count }, async (_, index) => {
      const expected = payload(length, index + 20);
      const stream = await withTimeout(client.openStream({ timeoutMs }), `typescript stream ${index} open timed out`);
      await writeStream(stream, expected, `typescript stream ${index} write timed out`);
      const actual = await withTimeout(collect(stream.readable), `typescript stream ${index} read timed out`);
      assertBytesEqual(actual, expected);
    }));

    await withTimeout(client.close(), "typescript client close timed out");
    await native.wait();
  } catch (error) {
    native.kill();
    throw withNativeStderr(error, native);
  }
}

async function runGoManyStreamsCase(count, length) {
  const native = spawnNative("client-many-roundtrip", {
    YAMUX_STREAM_COUNT: String(count),
    YAMUX_PAYLOAD_LENGTH: String(length),
  });
  const server = createServer(native.transport);

  try {
    const handlers = [];
    for (let index = 0; index < count; index += 1) {
      const stream = await withTimeout(server.acceptStream({ timeoutMs }), `typescript server accept ${index} timed out`);
      handlers.push(echoStreamExact(stream, length, `typescript server echo ${index} timed out`));
    }

    await Promise.all(handlers);
    await server.close().catch(() => undefined);
    await native.wait();
  } catch (error) {
    native.kill();
    throw withNativeStderr(error, native);
  }
}

async function runTypeScriptHalfCloseCase(expected) {
  const native = spawnNative("server-reply-after-fin", {
    YAMUX_PAYLOAD_BASE64: Buffer.from(expected).toString("base64"),
  });
  const client = createClient(native.transport);

  try {
    const stream = await withTimeout(client.openStream({ timeoutMs }), "typescript half-close stream open timed out");
    await writeStream(stream, expected, "typescript half-close write timed out");
    const actual = await withTimeout(collect(stream.readable), "typescript half-close read timed out");
    assertBytesEqual(actual, expected);

    await client.close().catch(() => undefined);
    await native.wait();
  } catch (error) {
    native.kill();
    throw withNativeStderr(error, native);
  }
}

async function runGoHalfCloseCase(expected) {
  const native = spawnNative("client-half-close", {
    YAMUX_PAYLOAD_BASE64: Buffer.from(expected).toString("base64"),
  });
  const server = createServer(native.transport);

  try {
    const stream = await withTimeout(server.acceptStream({ timeoutMs }), "typescript half-close accept timed out");
    const actual = await withTimeout(collect(stream.readable), "typescript half-close collect timed out");
    assertBytesEqual(actual, expected);
    await writeStream(stream, actual, "typescript half-close reply timed out");

    await server.close().catch(() => undefined);
    await native.wait();
  } catch (error) {
    native.kill();
    throw withNativeStderr(error, native);
  }
}

async function runTypeScriptResetCase() {
  const native = spawnNative("server-expect-reset");
  const client = createClient(native.transport);

  try {
    const stream = await withTimeout(client.openStream({ timeoutMs }), "typescript reset stream open timed out");
    await withTimeout(stream.reset(new Error("conformance reset")), "typescript reset timed out");
    await native.wait();
    await client.close().catch(() => undefined);
  } catch (error) {
    native.kill();
    throw withNativeStderr(error, native);
  }
}

async function runGoResetCase() {
  const native = spawnNative("server-reset-after-close-timeout", {
    YAMUX_HOLD_MS: "300",
  });
  const client = createClient(native.transport);

  try {
    const stream = await withTimeout(client.openStream({ timeoutMs }), "typescript reset-observer stream open timed out");
    const reader = stream.readable.getReader();
    const result = await withTimeout(reader.read(), "typescript reset-observer FIN read timed out");
    reader.releaseLock();
    assert.equal(result.done, true);
    await delay(150);
    await expectRejects(
      stream.write(new Uint8Array([1])),
      (error) => error?.code === "CONNECTION_RESET",
      "typescript stream write should reject after go reset",
    );

    await client.close().catch(() => undefined);
    await native.wait();
  } catch (error) {
    native.kill();
    throw withNativeStderr(error, native);
  }
}

async function runGoAwayToTypeScriptCase() {
  const native = spawnNative("server-goaway", {
    YAMUX_HOLD_MS: "500",
  });
  const client = createClient(native.transport);

  try {
    await waitForGoAway(client);
    await expectRejects(
      client.openStream({ timeoutMs }),
      (error) => error?.code === "GO_AWAY",
      "typescript open after go goaway should reject",
    );
    await client.close().catch(() => undefined);
    await native.wait();
  } catch (error) {
    native.kill();
    throw withNativeStderr(error, native);
  }
}

async function runTypeScriptGoAwayCase() {
  const native = spawnNative("client-open-after-goaway", {
    YAMUX_HOLD_MS: "100",
  });
  const server = createServer(native.transport);

  try {
    await withTimeout(server.goAway(), "typescript goaway timed out");
    await native.wait();
    await server.close().catch(() => undefined);
  } catch (error) {
    native.kill();
    throw withNativeStderr(error, native);
  }
}

async function runGoPeerCloseCase() {
  const native = spawnNative("server-close-immediately");
  const client = createClient(native.transport);

  try {
    const accepted = client.acceptStream({ timeoutMs });
    await expectRejects(
      accepted,
      (error) => error?.code === "SESSION_CLOSED",
      "pending accept should reject after go peer close",
    );
    await native.wait();
    await client.close().catch(() => undefined);
  } catch (error) {
    native.kill();
    throw withNativeStderr(error, native);
  }
}

async function runMalformedGoFrameCase() {
  const native = spawnNative("malformed-frame");
  const client = createClient(native.transport);

  try {
    await expectRejects(
      client.acceptStream({ timeoutMs }),
      (error) => error?.code === "PROTOCOL_ERROR",
      "malformed go frame should fail the typescript session",
    );
    await native.wait();
    await client.close().catch(() => undefined);
  } catch (error) {
    native.kill();
    throw withNativeStderr(error, native);
  }
}

async function runMalformedTypeScriptFrameCase() {
  const native = spawnNative("server-expect-malformed");

  try {
    native.child.stdin.end(Buffer.from([
      1, 2, 0, 1,
      0, 0, 0, 0,
      0, 0, 0, 0,
    ]));
    await native.wait();
  } catch (error) {
    native.kill();
    throw withNativeStderr(error, native);
  }
}

function spawnNative(mode, env = {}) {
  const child = spawn("go", ["run", "./native", mode], {
    cwd: conformanceDir,
    env: {
      ...process.env,
      ...env,
    },
    stdio: ["pipe", "pipe", "pipe"],
  });

  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });

  child.on("error", (error) => {
    stderr += `${error.stack ?? error.message}\n`;
  });

  return {
    child,
    transport: {
      readable: Readable.toWeb(child.stdout),
      writable: Writable.toWeb(child.stdin),
    },
    get stderr() {
      return stderr;
    },
    kill() {
      if (!child.killed) {
        child.kill("SIGKILL");
      }
    },
    async wait() {
      const [code, signal] = await withTimeout(once(child, "exit"), `native ${mode} did not exit`);
      if (code !== 0) {
        throw new Error(`native ${mode} exited with code ${code ?? signal}`);
      }
    },
  };
}

async function echoStream(stream, label) {
  const actual = await withTimeout(collect(stream.readable), label);
  await writeStream(stream, actual, label);
}

async function echoStreamExact(stream, length, label) {
  const actual = await withTimeout(readExactly(stream.readable, length), label);
  await writeStream(stream, actual, label);
}

async function writeStream(stream, bytes, label) {
  const writer = stream.writable.getWriter();
  try {
    if (bytes.byteLength > 0) {
      await withTimeout(writer.write(bytes), label);
    }
    await withTimeout(writer.close(), label);
  } finally {
    writer.releaseLock();
  }
}

async function collect(stream) {
  const reader = stream.getReader();
  const chunks = [];
  let total = 0;

  for (;;) {
    const result = await reader.read();
    if (result.done) {
      break;
    }
    chunks.push(result.value);
    total += result.value.byteLength;
  }

  reader.releaseLock();
  return concat(chunks, total);
}

async function readExactly(stream, length) {
  if (length === 0) {
    return new Uint8Array(0);
  }

  const reader = stream.getReader();
  const output = new Uint8Array(length);
  let offset = 0;

  try {
    while (offset < length) {
      const result = await reader.read();
      if (result.done) {
        throw new Error(`stream ended after ${offset}/${length} bytes`);
      }

      const available = Math.min(result.value.byteLength, length - offset);
      output.set(result.value.subarray(0, available), offset);
      offset += available;
      if (available !== result.value.byteLength) {
        throw new Error("conformance read received more bytes than expected");
      }
    }
  } finally {
    reader.releaseLock();
  }

  return output;
}

function concat(chunks, total) {
  const output = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

function payload(length, seed) {
  const bytes = new Uint8Array(length);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = (index * 31 + seed * 17) & 0xff;
  }
  return bytes;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForGoAway(session) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      await session.acceptStream({ timeoutMs: 20 });
      throw new Error("acceptStream resolved while waiting for GOAWAY");
    } catch (error) {
      if (error?.code === "GO_AWAY") {
        return;
      }
      if (error?.code !== "TIMEOUT") {
        throw error;
      }
    }
  }
  throw new Error("timed out waiting for GOAWAY");
}

async function expectRejects(promise, predicate, label) {
  try {
    await promise;
  } catch (error) {
    if (!predicate(error)) {
      throw error;
    }
    return;
  }
  throw new Error(label);
}

function assertBytesEqual(actual, expected) {
  assert.deepEqual(Buffer.from(actual), Buffer.from(expected));
}

function withTimeout(promise, label) {
  let timeoutId;
  const timeout = new Promise((_, reject) => {
    timeoutId = setTimeout(() => reject(new Error(label)), timeoutMs);
  });

  return Promise.race([promise, timeout]).finally(() => clearTimeout(timeoutId));
}

function withNativeStderr(error, native) {
  if (!native.stderr.trim()) {
    return error;
  }

  const wrapped = new Error(`${error.message}\nnative stderr:\n${native.stderr.trim()}`);
  wrapped.cause = error;
  return wrapped;
}
