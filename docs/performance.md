# Performance Notes

This document records the profiling loop used to optimize `@stateforward/yamux.ts` against `yamux-js@0.2.1`.

## Method

The benchmark uses client-opened echo streams over an in-memory transport. It counts payload bytes in both directions and validates echoed bytes.

```sh
npm run benchmark
```

For isolated profiling of this implementation:

```sh
npm run build
YAMUX_BENCH_IMPLEMENTATION='@stateforward/yamux.ts' \
YAMUX_BENCH_WARMUPS=2 \
YAMUX_BENCH_RUNS=12 \
YAMUX_BENCH_SCENARIO='1 KiB sequential:2500:1024:1' \
node --cpu-prof --cpu-prof-dir=/tmp/yamux-ts-profiles benchmarks/yamux-js.mjs
```

## Optimization Log

All numbers below were collected on Node `v24.11.1`. They are local-machine benchmarks, so use them for direction and regression detection rather than as portable absolute performance claims.

| Iteration | Isolated 1 KiB sequential result | Change | Profile evidence |
| --- | ---: | --- | --- |
| Baseline | 1200 streams in 92.52 ms, 12,971 streams/s | Starting point | Hot frames included per-frame no-op session hsm dispatch, byte copying in `ByteReader`, stream hsm startup, and protocol frame allocation. |
| Remove no-op session dispatches | 73.10 ms, 16,415 streams/s | Kept real session state transitions, removed hot-path no-op frame/open/accept dispatches. | hsm cost dropped but stream startup and byte copying remained hot. |
| Fast contiguous `ByteReader` reads | 67.28 ms, 17,835 streams/s | Returned existing contiguous chunks/subarrays instead of copying every header/payload. | `ByteReader` self-time dropped; stream startup became the largest owned cost. |
| Remove per-stream hsm startup | 38.51 ms, 31,164 streams/s | Kept session/client/server as hsm state machines; made `Stream` direct state because it already owned the authoritative flags. | hsm fell to about 1 ms in the next profile, proving it was no longer a material bottleneck. |
| Write protocol headers directly | 34.20 ms, 35,093 streams/s | Wrote headers into the final frame buffer and avoided DataView/header-copy overhead. | Protocol encoding self-time dropped but remained visible. |
| Avoid async promises for sync frame paths | 32.04 ms, 37,447 streams/s | Made data/window frame handling synchronous unless it actually has to write/reset/open asynchronously. | Remaining hot spots were benchmark byte validation, Web Stream construction, `readLoop`, `ByteReader`, and frame encoding. |
| Tried `Stream.write` single-frame fast path | 59.49 ms for 2500-stream long run versus 59.60 ms before | Reverted. The result was neutral and not worth extra branch complexity. | Improvement was micro-level noise. |

## Current Comparison

Current `npm run benchmark` result:

| Scenario | `@stateforward/yamux.ts` | `yamux-js@0.2.1` | Relative |
| --- | ---: | ---: | ---: |
| 1 KiB sequential | 36,256 streams/s, 70.8 MiB/s | 129,791 streams/s, 253.5 MiB/s | 0.28x |
| 1 KiB concurrent | 38,312 streams/s, 74.8 MiB/s | 155,958 streams/s, 304.6 MiB/s | 0.25x |
| 64 KiB concurrent | 10,616 streams/s, 1327.0 MiB/s | 9,576 streams/s, 1197.1 MiB/s | 1.11x |

## Current Bottlenecks

The final profile no longer supports blaming `@stateforward/hsm.ts`: hsm frames were about 1 ms in a roughly 927 ms profiling run. Remaining costs are dominated by:

- Web Stream construction per logical stream.
- Web Stream read/pull/write scheduling.
- Benchmark-side byte validation in `expectWebReadable`.
- Residual frame encoding and `ByteReader` work.

Further small-stream gains likely require changing the public stream representation or adding a lower-level non-Web-Streams fast path. That would be a larger API/product decision rather than another local micro-optimization.
