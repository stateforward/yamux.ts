# @stateforward/yamux.ts

Browser-native Yamux multiplexing over Web Streams.

## Installation

<!-- install command matching package.json#name -->

```sh
npm install @stateforward/yamux.ts
```

## Runtime Dependency

<!-- runtime dependencies from package.json#dependencies -->

- `@stateforward/hsm.ts`

## Development

<!-- npm scripts from package.json -->

- `npm run build`
- `npm run benchmark`
- `npm run conformance`
- `npm test`
- `npm run test:browser`
- `npm run test:coverage`
- `npm run test:fuzz`
- `npm run typecheck`

## Conformance

<!-- native Go conformance runner from conformance/ -->

`npm run conformance` builds this package and runs bidirectional echo, concurrency, half-close, reset, GOAWAY, peer-close, and malformed-frame cases against `github.com/hashicorp/yamux` pinned by `conformance/go.mod`.

## Benchmark

<!-- benchmark command from package.json#scripts.benchmark and comparison target from package.json#devDependencies -->

`npm run benchmark` builds this package and compares client-opened echo streams over an in-memory transport against `yamux-js`.

See [docs/performance.md](docs/performance.md) for profiling notes and optimization history.

## API

<!-- exported public API from src/index.ts -->

- `Session`
- `SessionRole`
- `SessionOptions`
- `WaitOptions`
- `PingOptions`
- `SessionEvent`
- `Client`
- `ClientOptions`
- `clientStreamIDs`
- `ClientEvent`
- `createClient`
- `Server`
- `ServerOptions`
- `serverStreamIDs`
- `ServerEvent`
- `createServer`
- `Stream`
- `StreamOptions`
- `StreamEvent`
- `PROTOCOL_VERSION`
- `HEADER_SIZE`
- `INITIAL_STREAM_WINDOW`
- `MAX_UINT32`
- `YamuxHeader`
- `YamuxFrame`
- `decodeHeader`
- `encodeHeader`
- `hasFlag`
- `frameToBytes`
- `isFrameType`
- `YamuxFrameType`
- `YamuxFlag`
- `YamuxGoAwayCode`
- `YamuxError`
- `YamuxErrorCode`
- `abortedError`
- `timeoutError`

## Example

```ts
import { createClient } from "@stateforward/yamux.ts";

const session = createClient({
  readable: socket.readable,
  writable: socket.writable,
});

const stream = await session.openStream();
const writer = stream.writable.getWriter();

await writer.write(new TextEncoder().encode("hello"));
await writer.close();
```
