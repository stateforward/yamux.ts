import {
  HEADER_SIZE,
  INITIAL_STREAM_WINDOW,
  YamuxFlag,
  YamuxFrameType,
  createClient,
  createServer,
  decodeHeader,
  encodeHeader,
} from "../src/index.js";

declare global {
  interface Window {
    __yamuxBrowserProbe: Promise<BrowserProbeResult>;
  }
}

type BrowserProbeResult = {
  ok: true;
  userAgent: string;
  transferredBytes: number;
};

window.__yamuxBrowserProbe = runBrowserProbe()
  .then((result) => {
    document.body.dataset.status = "ok";
    return result;
  })
  .catch((error: unknown) => {
    document.body.dataset.status = "error";
    document.body.textContent = error instanceof Error ? error.stack ?? error.message : String(error);
    throw error;
  });

async function runBrowserProbe(): Promise<BrowserProbeResult> {
  const header = encodeHeader(YamuxFrameType.WindowUpdate, YamuxFlag.SYN | YamuxFlag.ACK, 17, 42);
  if (header.byteLength !== HEADER_SIZE) {
    throw new Error(`expected ${HEADER_SIZE} byte header, got ${header.byteLength}`);
  }
  const decoded = decodeHeader(header);
  if (
    decoded.type !== YamuxFrameType.WindowUpdate ||
    decoded.flags !== (YamuxFlag.SYN | YamuxFlag.ACK) ||
    decoded.streamID !== 17 ||
    decoded.length !== 42
  ) {
    throw new Error("header codec failed in browser");
  }

  const pair = duplexPair();
  const client = createClient(pair.client);
  const server = createServer(pair.server);

  try {
    await client.ping({ timeoutMs: 1_000 });

    const accepted = server.acceptStream({ timeoutMs: 1_000 });
    const outbound = await client.openStream({ timeoutMs: 1_000 });
    const inbound = await accepted;

    const payload = new Uint8Array(INITIAL_STREAM_WINDOW + 1024);
    for (let index = 0; index < payload.byteLength; index += 1) {
      payload[index] = (index * 31 + 7) & 0xff;
    }

    const writer = outbound.writable.getWriter();
    const received = collect(inbound.readable);
    await writer.write(payload);
    await writer.close();
    writer.releaseLock();

    assertBytesEqual(await received, payload);

    await client.close();
    await server.close();

    return {
      ok: true,
      userAgent: navigator.userAgent,
      transferredBytes: payload.byteLength,
    };
  } finally {
    await client.close().catch(() => undefined);
    await server.close().catch(() => undefined);
  }
}

function duplexPair(): {
  client: {
    readable: ReadableStream<Uint8Array>;
    writable: WritableStream<Uint8Array>;
  };
  server: {
    readable: ReadableStream<Uint8Array>;
    writable: WritableStream<Uint8Array>;
  };
} {
  const clientToServer = new TransformStream<Uint8Array, Uint8Array>();
  const serverToClient = new TransformStream<Uint8Array, Uint8Array>();

  return {
    client: {
      readable: serverToClient.readable,
      writable: clientToServer.writable,
    },
    server: {
      readable: clientToServer.readable,
      writable: serverToClient.writable,
    },
  };
}

async function collect(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
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
  const output = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

function assertBytesEqual(actual: Uint8Array, expected: Uint8Array): void {
  if (actual.byteLength !== expected.byteLength) {
    throw new Error(`byte length mismatch: ${actual.byteLength} !== ${expected.byteLength}`);
  }
  for (let index = 0; index < expected.byteLength; index += 1) {
    if (actual[index] !== expected[index]) {
      throw new Error(`byte mismatch at ${index}: ${actual[index]} !== ${expected[index]}`);
    }
  }
}
