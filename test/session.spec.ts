import * as hsm from "@stateforward/hsm.ts";
import { describe, expect, it } from "vitest";

import { Deferred } from "../src/async.js";
import { YamuxError } from "../src/errors.js";
import {
  HEADER_SIZE,
  MAX_UINT32,
  YamuxFlag,
  YamuxFrameType,
  YamuxGoAwayCode,
  frameToBytes,
} from "../src/protocol.js";
import { createClient } from "../src/client.js";
import { createServer } from "../src/server.js";
import { Session } from "../src/session.js";
import {
  FailingGoAwaySession,
  HarnessSession,
  chunksReadable,
  duplexPair,
  emptyReadable,
  pendingReadable,
  startHarness,
  startStream,
  writableSink,
} from "./helpers.js";

describe("yamux session", () => {
  it("validates constructor and public option values", async () => {
    const options = () => ({
      readable: pendingReadable(),
      writable: writableSink(),
    });

    expect(() => new Session({
      ...options(),
      readable: null as unknown as ReadableStream<Uint8Array>,
    })).toThrow(TypeError);
    expect(() => new Session({
      ...options(),
      writable: null as unknown as WritableStream<Uint8Array>,
    })).toThrow(TypeError);
    expect(() => new Session({
      ...options(),
      role: "bad" as unknown as "client",
    })).toThrow(RangeError);
    expect(() => new Session({
      ...options(),
      acceptBacklog: -1,
    })).toThrow(RangeError);
    expect(() => new Session({
      ...options(),
      acceptBacklog: 1.5,
    })).toThrow(RangeError);
    expect(() => new Session({
      ...options(),
      receiveWindow: -1,
    })).toThrow(RangeError);
    expect(() => new Session({
      ...options(),
      maxFrameSize: 0,
    })).toThrow(RangeError);
    expect(() => new Session({
      ...options(),
      maxFrameSize: Number.POSITIVE_INFINITY,
    })).toThrow(RangeError);

    const session = startHarness();
    await expect(session.acceptStream({ timeoutMs: -1 })).rejects.toThrow(RangeError);
    await expect(session.goAway(99 as YamuxGoAwayCode)).rejects.toThrow(RangeError);
  });

  it("responds to pings", async () => {
    const pair = duplexPair();
    const client = createClient(pair.client);
    const server = createServer(pair.server);

    await expect(client.ping({ timeoutMs: 1_000 })).resolves.toBeGreaterThanOrEqual(0);

    await client.close();
    await server.close();
  });

  it("handles accept timeouts and aborted signals", async () => {
    const session = startHarness();
    await expect(session.acceptStream({ timeoutMs: 0 })).rejects.toMatchObject({ code: "TIMEOUT" });

    const controller = new AbortController();
    controller.abort();
    await expect(session.acceptStream({ signal: controller.signal })).rejects.toMatchObject({ code: "ABORTED" });

    const controllerWithTimeout = new AbortController();
    await expect(session.acceptStream({ signal: controllerWithTimeout.signal, timeoutMs: 0 })).rejects.toMatchObject({
      code: "TIMEOUT",
    });

    const abortedWithTimeout = new AbortController();
    abortedWithTimeout.abort();
    await expect(session.exposeShiftIncoming({ signal: abortedWithTimeout.signal, timeoutMs: 100 })).rejects.toMatchObject({
      code: "ABORTED",
    });
  });

  it("covers stream id and ping allocation boundaries", () => {
    const client = startHarness(new HarnessSession("client"));
    const server = startHarness(new HarnessSession("server"));

    expect(client.exposeAllocateStreamID()).toBe(1);
    expect(server.exposeAllocateStreamID()).toBe(2);

    client.setNextStreamID(MAX_UINT32 + 1);
    expect(() => client.exposeAllocateStreamID()).toThrow(YamuxError);

    client.setNextPingValue(MAX_UINT32);
    expect(client.exposeAllocatePingValue()).toBe(MAX_UINT32);
    expect(client.exposeAllocatePingValue()).toBe(1);
  });

  it("rejects invalid public operations on closed or draining sessions", async () => {
    const session = startHarness();

    session.setTransportClosed(true);
    await expect(session.openStream()).rejects.toMatchObject({ code: "SESSION_CLOSED" });
    await expect(session.ping()).rejects.toMatchObject({ code: "SESSION_CLOSED" });
    await expect(session.close()).resolves.toBeUndefined();

    session.setTransportClosed(false);
    session.setGoAwayReceived(true);
    await expect(session.openStream()).rejects.toMatchObject({ code: "GO_AWAY" });
    await expect(session.acceptStream()).rejects.toMatchObject({ code: "GO_AWAY" });

    session.setGoAwayReceived(false);
    session.setGoAwaySent(true);
    await expect(session.openStream()).rejects.toMatchObject({ code: "GO_AWAY" });
    await expect(session.goAway()).resolves.toBeUndefined();
  });

  it("cleans pending pings when sending fails", async () => {
    const session = startHarness();
    session.setWriter(null);

    await expect(session.ping({ value: 12 })).rejects.toMatchObject({ code: "SESSION_CLOSED" });
    expect(session.pendingPingCount()).toBe(0);
  });

  it("validates control frames", async () => {
    const session = startHarness();

    expect(() => session.exposeValidateHeader({
      version: 1,
      type: YamuxFrameType.Ping,
      flags: 0,
      streamID: 0,
      length: 0,
    })).toThrow(YamuxError);
    expect(() => session.exposeValidateHeader({
      version: 0,
      type: YamuxFrameType.GoAway,
      flags: YamuxFlag.SYN,
      streamID: 0,
      length: 0,
    })).toThrow(YamuxError);

    await expect(session.exposeHandlePing({
      version: 0,
      type: YamuxFrameType.Ping,
      flags: YamuxFlag.SYN,
      streamID: 1,
      length: 1,
    })).rejects.toMatchObject({ code: "PROTOCOL_ERROR" });

    await expect(session.exposeHandlePing({
      version: 0,
      type: YamuxFrameType.Ping,
      flags: 0,
      streamID: 0,
      length: 1,
    })).rejects.toMatchObject({ code: "PROTOCOL_ERROR" });

    await expect(session.exposeHandleGoAway({
      version: 0,
      type: YamuxFrameType.GoAway,
      flags: 0,
      streamID: 1,
      length: YamuxGoAwayCode.Normal,
    })).rejects.toMatchObject({ code: "PROTOCOL_ERROR" });

    await expect(session.exposeHandleGoAway({
      version: 0,
      type: YamuxFrameType.GoAway,
      flags: 0,
      streamID: 0,
      length: 99,
    })).rejects.toMatchObject({ code: "PROTOCOL_ERROR" });
  });

  it("handles stream frame edge cases", async () => {
    const session = startHarness(new HarnessSession("server"));
    const existing = startStream(session, 1);
    session.addStream(existing);

    await expect(session.exposeHandleStreamFrame({
      header: { version: 0, type: YamuxFrameType.Data, flags: 0, streamID: 0, length: 0 },
      payload: new Uint8Array(0),
    })).rejects.toMatchObject({ code: "PROTOCOL_ERROR" });

    await expect(session.exposeHandleStreamFrame({
      header: { version: 0, type: YamuxFrameType.WindowUpdate, flags: YamuxFlag.SYN, streamID: 2, length: 0 },
      payload: new Uint8Array(0),
    })).rejects.toMatchObject({ code: "INVALID_STREAM" });

    await session.exposeHandleStreamFrame({
      header: { version: 0, type: YamuxFrameType.WindowUpdate, flags: YamuxFlag.RST, streamID: 1, length: 0 },
      payload: new Uint8Array(0),
    });

    session.addStream(startStream(session, 3));
    await expect(session.exposeHandleStreamFrame({
      header: { version: 0, type: YamuxFrameType.WindowUpdate, flags: YamuxFlag.SYN, streamID: 3, length: 0 },
      payload: new Uint8Array(0),
    })).rejects.toMatchObject({ code: "DUPLICATE_STREAM" });

    session.setGoAwaySent(true);
    await expect(session.exposeHandleStreamFrame({
      header: { version: 0, type: YamuxFrameType.WindowUpdate, flags: YamuxFlag.SYN, streamID: 5, length: 0 },
      payload: new Uint8Array(0),
    })).rejects.toMatchObject({ code: "GO_AWAY" });

    await session.exposeHandleStreamFrame({
      header: { version: 0, type: YamuxFrameType.WindowUpdate, flags: 0, streamID: 99, length: 0 },
      payload: new Uint8Array(0),
    });

    await session.exposeHandleStreamFrame({
      header: { version: 0, type: YamuxFrameType.WindowUpdate, flags: YamuxFlag.RST, streamID: 99, length: 0 },
      payload: new Uint8Array(0),
    });
  });

  it("handles pending ping acknowledgements and failures", async () => {
    const session = startHarness();
    const deferred = new Deferred<number>();
    session.addPendingPing(7, deferred);

    await session.exposeHandlePing({
      version: 0,
      type: YamuxFrameType.Ping,
      flags: YamuxFlag.ACK,
      streamID: 0,
      length: 7,
    });

    await expect(deferred.promise).resolves.toBeGreaterThanOrEqual(0);
    expect(session.pendingPingCount()).toBe(0);
    await session.exposeHandlePing({
      version: 0,
      type: YamuxFrameType.Ping,
      flags: YamuxFlag.ACK,
      streamID: 0,
      length: 99,
    });
    await expect(session.ping({ value: -1 })).rejects.toThrow(RangeError);
  });

  it("cleans up during fail and close helper paths", async () => {
    const session = startHarness(new FailingGoAwaySession());
    const stream = startStream(session);
    session.addStream(stream);
    const deferred = new Deferred<number>();
    session.addPendingPing(1, deferred);

    await session.exposeFail("bad frame");
    await expect(deferred.promise).rejects.toMatchObject({ code: "PROTOCOL_ERROR" });
    expect(session.lastError).toBe("bad frame");

    const alreadyDraining = startHarness();
    alreadyDraining.setGoAwaySent(true);
    await alreadyDraining.exposeFail(new Error("already draining"));
    expect(alreadyDraining.lastError).toBeInstanceOf(Error);

    await session.exposeMarkTransportClosed();
    session.setWriter(null);
    await session.exposeCloseWriter();

    const rejectingClose = startHarness();
    rejectingClose.setWriteChain(Promise.reject(new Error("write failed")));
    rejectingClose.setWriter(new WritableStream<Uint8Array>({
      close() {
        return Promise.reject(new Error("close failed"));
      },
    }).getWriter());
    await rejectingClose.exposeCloseWriter();

    const direct = startHarness();
    Session.onSendGoAway(direct.context(), direct, { ...Session.sendGoAwayEvent, data: YamuxGoAwayCode.InternalError });
    Session.onSendGoAway(direct.context(), direct, Session.sendGoAwayEvent);
    Session.onRemoteGoAway(direct.context(), direct, Session.remoteGoAwayEvent);
    Session.onError(direct.context(), direct, { ...Session.errorEvent, data: "direct" });
    Session.onError(direct.context(), direct, Session.errorEvent);
    expect((direct as unknown as { goAwayReceived: boolean }).goAwayReceived).toBe(true);
    expect(direct.goAwayCode).toBe(YamuxGoAwayCode.Normal);
    expect(direct.lastError).toBeNull();
  });

  it("exercises read-loop and send-frame failures", async () => {
    const headerOnly = frameToBytes(YamuxFrameType.Data, 0, 1, 1, new Uint8Array([7])).subarray(0, HEADER_SIZE);
    const readerSession = startHarness(new HarnessSession("server", chunksReadable([headerOnly])));
    await readerSession.exposeReadLoop();
    expect(readerSession.lastError).toMatchObject({ code: "INVALID_FRAME" });

    const closed = startHarness();
    closed.setTransportClosed(true);
    await expect(closed.sendData(1, 0, new Uint8Array(0))).rejects.toMatchObject({ code: "SESSION_CLOSED" });

    const noWriter = startHarness();
    noWriter.setWriter(null);
    await expect(noWriter.sendData(1, 0, new Uint8Array(0))).rejects.toMatchObject({ code: "SESSION_CLOSED" });
  });

  it("settles pending accepts, streams, and pings on clean remote transport close", async () => {
    const session = startHarness(new HarnessSession("client", emptyReadable(), writableSink()));
    const accepted = session.acceptStream();
    const stream = startStream(session, 1);
    const deferred = new Deferred<number>();

    session.addStream(stream);
    session.addPendingPing(77, deferred);

    await session.exposeReadLoop();

    await expect(accepted).rejects.toMatchObject({ code: "SESSION_CLOSED" });
    await expect(deferred.promise).rejects.toMatchObject({ code: "SESSION_CLOSED" });
    expect(stream.resetError).toMatchObject({ code: "SESSION_CLOSED" });
    await expect(session.openStream()).rejects.toMatchObject({ code: "SESSION_CLOSED" });
  });

  it("covers constructor and helper fallback branches", async () => {
    const defaults = hsm.start(new Session({
      readable: pendingReadable(),
      writable: writableSink(),
    }), Session.model) as unknown as Session;
    expect(defaults.role).toBe("client");

    const session = startHarness();
    const stream = startStream(session, 23);
    session.addIncoming(stream);
    await expect(session.exposeShiftIncoming({})).resolves.toBe(stream);

    const descriptor = Object.getOwnPropertyDescriptor(globalThis, "performance");
    Object.defineProperty(globalThis, "performance", {
      configurable: true,
      value: undefined,
    });
    try {
      const deferred = new Deferred<number>();
      session.addPendingPing(42, deferred);
      await session.exposeHandlePing({
        version: 0,
        type: YamuxFrameType.Ping,
        flags: YamuxFlag.ACK,
        streamID: 0,
        length: 42,
      });
      await expect(deferred.promise).resolves.toBeGreaterThanOrEqual(0);
    } finally {
      if (descriptor) {
        Object.defineProperty(globalThis, "performance", descriptor);
      }
    }
  });
});
