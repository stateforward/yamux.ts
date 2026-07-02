import { createRequire } from "node:module";
import { performance } from "node:perf_hooks";
import { TransformStream } from "node:stream/web";

import { createClient, createServer } from "../dist/index.js";

const require = createRequire(import.meta.url);
const { Client: YamuxJsClient, Server: YamuxJsServer } = require("yamux-js");
const yamuxJsPackage = require("yamux-js/package.json");

const DEFAULT_SCENARIOS = [
  { name: "1 KiB sequential", streams: 500, payloadBytes: 1024, concurrency: 1 },
  { name: "1 KiB concurrent", streams: 1000, payloadBytes: 1024, concurrency: 32 },
  { name: "64 KiB concurrent", streams: 128, payloadBytes: 64 * 1024, concurrency: 16 },
];

const WARMUPS = readPositiveIntegerEnv("YAMUX_BENCH_WARMUPS", 2);
const RUNS = readPositiveIntegerEnv("YAMUX_BENCH_RUNS", 7);
const SCENARIOS = parseScenarios();

const IMPLEMENTATIONS = [
  {
    name: "@stateforward/yamux.ts",
    run: runStateforward,
  },
  {
    name: `yamux-js@${yamuxJsPackage.version}`,
    run: runYamuxJs,
  },
];

async function main() {
  console.log(`node ${process.version}`);
  console.log(`warmups=${WARMUPS} runs=${RUNS}`);
  console.log("metric: client-opened echo streams over an in-memory transport");
  console.log("throughput counts payload bytes in both directions");
  console.log("");

  const rows = [];
  for (const scenario of SCENARIOS) {
    const scenarioRows = [];
    for (const implementation of IMPLEMENTATIONS) {
      const result = await measure(implementation, scenario);
      scenarioRows.push(result);
      rows.push(result);
    }

    const baseline = scenarioRows.find((row) => row.package === `yamux-js@${yamuxJsPackage.version}`);
    if (baseline) {
      for (const row of scenarioRows) {
        row.vsYamuxJs = baseline.medianMs / row.medianMs;
      }
    }
  }

  printTable(rows);
}

async function measure(implementation, scenario) {
  const payload = makePayload(scenario.payloadBytes);

  for (let index = 0; index < WARMUPS; index += 1) {
    await withTimeout(
      implementation.run(scenario, payload),
      `${implementation.name} ${scenario.name} warmup ${index + 1}`,
    );
  }

  const timings = [];
  for (let index = 0; index < RUNS; index += 1) {
    timings.push(await withTimeout(
      implementation.run(scenario, payload),
      `${implementation.name} ${scenario.name} run ${index + 1}`,
    ));
  }

  const medianMs = median(timings);
  const seconds = medianMs / 1000;
  const bytesRoundTrip = scenario.streams * scenario.payloadBytes * 2;
  return {
    scenario: scenario.name,
    package: implementation.name,
    streams: scenario.streams,
    payload: formatBytes(scenario.payloadBytes),
    concurrency: scenario.concurrency,
    medianMs,
    streamsPerSecond: scenario.streams / seconds,
    mibPerSecond: bytesRoundTrip / seconds / 1024 / 1024,
    vsYamuxJs: 1,
  };
}

async function runStateforward(scenario, payload) {
  const transport = createWebTransportPair();
  const client = createClient({
    readable: transport.client.readable,
    writable: transport.client.writable,
    acceptBacklog: scenario.streams + scenario.concurrency,
  });
  const server = createServer({
    readable: transport.server.readable,
    writable: transport.server.writable,
    acceptBacklog: scenario.streams + scenario.concurrency,
  });
  const stopServer = serveStateforwardEcho(server);

  try {
    const started = performance.now();
    await runConcurrently(scenario.streams, scenario.concurrency, async () => {
      await stateforwardEcho(client, payload);
    });
    return performance.now() - started;
  } finally {
    await client.close().catch(ignoreError);
    await stopServer();
  }
}

async function stateforwardEcho(client, payload) {
  const stream = await client.openStream({ timeoutMs: 30_000 });
  const readTask = expectWebReadable(stream.readable, payload);
  const writer = stream.writable.getWriter();

  try {
    await writer.write(payload);
    await writer.close();
  } finally {
    writer.releaseLock();
  }

  await readTask;
}

function serveStateforwardEcho(server) {
  let stopping = false;
  const task = (async () => {
    while (!stopping) {
      try {
        const stream = await server.acceptStream();
        void stream.readable.pipeTo(stream.writable).catch(ignoreError);
      } catch (error) {
        if (!stopping) {
          throw error;
        }
      }
    }
  })();

  return async () => {
    stopping = true;
    await server.close().catch(ignoreError);
    await task.catch(ignoreError);
  };
}

async function runYamuxJs(scenario, payload) {
  const config = {
    acceptBacklog: scenario.streams + scenario.concurrency,
    connectionWriteTimeout: 30,
    enableKeepAlive: false,
    logger: ignoreError,
    maxStreamWindowSize: 256 * 1024,
  };
  const server = new YamuxJsServer((stream) => {
    stream.on("error", ignoreError);
    stream.on("data", (chunk) => {
      stream.write(chunk);
    });
    stream.on("end", () => {
      stream.close();
    });
  }, config);
  const client = new YamuxJsClient(config);

  server.on("error", ignoreError);
  client.on("error", ignoreError);
  client.pipe(server).pipe(client);

  try {
    const started = performance.now();
    await runConcurrently(scenario.streams, scenario.concurrency, async () => {
      await yamuxJsEcho(client, payload);
    });
    return performance.now() - started;
  } finally {
    client.unpipe(server);
    server.unpipe(client);
    client.close();
    server.close();
    client.destroy();
    server.destroy();
  }
}

async function yamuxJsEcho(client, payload) {
  const stream = client.open();
  const expected = Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength);

  await new Promise((resolve, reject) => {
    const chunks = [];
    let length = 0;
    let settled = false;

    const fail = (error) => {
      if (!settled) {
        settled = true;
        reject(error);
      }
    };
    const finish = () => {
      if (settled) {
        return;
      }
      try {
        expectNodeChunks(chunks, length, expected);
        settled = true;
        resolve();
      } catch (error) {
        fail(error);
      }
    };

    stream.on("data", (chunk) => {
      chunks.push(chunk);
      length += chunk.length;
      if (length >= expected.length) {
        finish();
      }
    });
    stream.once("error", fail);
    stream.write(expected, (error) => {
      if (error) {
        fail(error);
        return;
      }
      stream.close();
    });
  });
}

function createWebTransportPair() {
  const clientToServer = new TransformStream();
  const serverToClient = new TransformStream();
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

async function expectWebReadable(readable, expected) {
  const reader = readable.getReader();
  let offset = 0;

  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      if (offset + value.byteLength > expected.byteLength) {
        throw new Error(`received too many bytes: ${offset + value.byteLength} > ${expected.byteLength}`);
      }
      for (let index = 0; index < value.byteLength; index += 1) {
        if (value[index] !== expected[offset + index]) {
          throw new Error(`payload mismatch at byte ${offset + index}`);
        }
      }
      offset += value.byteLength;
    }
  } finally {
    reader.releaseLock();
  }

  if (offset !== expected.byteLength) {
    throw new Error(`received ${offset} bytes, expected ${expected.byteLength}`);
  }
}

function expectNodeChunks(chunks, length, expected) {
  if (length !== expected.length) {
    throw new Error(`received ${length} bytes, expected ${expected.length}`);
  }

  let offset = 0;
  for (const chunk of chunks) {
    for (let index = 0; index < chunk.length; index += 1) {
      if (chunk[index] !== expected[offset + index]) {
        throw new Error(`payload mismatch at byte ${offset + index}`);
      }
    }
    offset += chunk.length;
  }
}

async function runConcurrently(total, concurrency, operation) {
  let next = 0;
  const workers = Array.from({ length: Math.min(total, concurrency) }, async () => {
    for (;;) {
      const index = next;
      next += 1;
      if (index >= total) {
        return;
      }
      await operation(index);
    }
  });
  await Promise.all(workers);
}

function makePayload(size) {
  const payload = new Uint8Array(size);
  for (let index = 0; index < payload.length; index += 1) {
    payload[index] = (index * 31 + 17) & 0xff;
  }
  return payload;
}

function median(values) {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[middle];
  }
  return (sorted[middle - 1] + sorted[middle]) / 2;
}

async function withTimeout(promise, label) {
  let timeoutId;
  const timeout = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      reject(new Error(`${label} timed out after 30000ms`));
    }, 30_000);
  });

  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timeoutId);
  }
}

function printTable(rows) {
  const table = rows.map((row) => ({
    scenario: row.scenario,
    package: row.package,
    streams: String(row.streams),
    payload: row.payload,
    concurrency: String(row.concurrency),
    "median ms": row.medianMs.toFixed(2),
    "streams/s": row.streamsPerSecond.toFixed(0),
    "MiB/s": row.mibPerSecond.toFixed(1),
    "vs yamux-js": `${row.vsYamuxJs.toFixed(2)}x`,
  }));
  const headers = Object.keys(table[0]);
  const widths = Object.fromEntries(
    headers.map((header) => [
      header,
      Math.max(header.length, ...table.map((row) => row[header].length)),
    ]),
  );

  console.log(headers.map((header) => header.padEnd(widths[header])).join("  "));
  console.log(headers.map((header) => "-".repeat(widths[header])).join("  "));
  for (const row of table) {
    console.log(headers.map((header) => row[header].padEnd(widths[header])).join("  "));
  }
}

function parseScenarios() {
  const single = process.env.YAMUX_BENCH_SCENARIO;
  if (!single) {
    return DEFAULT_SCENARIOS;
  }

  const [name, streams, payloadBytes, concurrency] = single.split(":");
  if (!name || !streams || !payloadBytes || !concurrency) {
    throw new Error("YAMUX_BENCH_SCENARIO must be name:streams:payloadBytes:concurrency");
  }

  return [
    {
      name,
      streams: parsePositiveInteger(streams, "scenario streams"),
      payloadBytes: parsePositiveInteger(payloadBytes, "scenario payloadBytes"),
      concurrency: parsePositiveInteger(concurrency, "scenario concurrency"),
    },
  ];
}

function readPositiveIntegerEnv(name, fallback) {
  const value = process.env[name];
  if (value === undefined) {
    return fallback;
  }
  return parsePositiveInteger(value, name);
}

function parsePositiveInteger(value, name) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function formatBytes(bytes) {
  if (bytes % (1024 * 1024) === 0) {
    return `${bytes / 1024 / 1024} MiB`;
  }
  if (bytes % 1024 === 0) {
    return `${bytes / 1024} KiB`;
  }
  return `${bytes} B`;
}

function ignoreError() {}

await main();
