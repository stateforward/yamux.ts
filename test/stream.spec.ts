import { describe, expect, it } from "vitest";

import { createClient } from "../src/client.js";
import { YamuxError } from "../src/errors.js";
import { INITIAL_STREAM_WINDOW, MAX_UINT32 } from "../src/protocol.js";
import { createServer } from "../src/server.js";
import { Stream } from "../src/stream.js";
import {
  RejectingResetSession,
  duplexPair,
  readBytes,
  startHarness,
  startStream,
} from "./helpers.js";

describe("yamux stream", () => {
  it("validates direct stream construction options", () => {
    const session = startHarness();
    const options = () => ({
      id: 1,
      session,
      local: true,
      receiveWindow: INITIAL_STREAM_WINDOW,
      maxFrameSize: 64 * 1024,
    });

    expect(() => new Stream({
      ...options(),
      id: 0,
    })).toThrow(RangeError);
    expect(() => new Stream({
      ...options(),
      session: null as unknown as typeof session,
    })).toThrow(TypeError);
    expect(() => new Stream({
      ...options(),
      local: "yes" as unknown as boolean,
    })).toThrow(TypeError);
    expect(() => new Stream({
      ...options(),
      receiveWindow: -1,
    })).toThrow(RangeError);
    expect(() => new Stream({
      ...options(),
      maxFrameSize: 0,
    })).toThrow(RangeError);
  });

  it("uses window updates to transfer more than the initial stream window", async () => {
    const pair = duplexPair();
    const client = createClient(pair.client);
    const server = createServer(pair.server);

    const accepted = server.acceptStream({ timeoutMs: 1_000 });
    const outbound = await client.openStream({ timeoutMs: 1_000 });
    const inbound = await accepted;

    const data = new Uint8Array(INITIAL_STREAM_WINDOW + 4096);
    for (let index = 0; index < data.length; index += 1) {
      data[index] = index % 251;
    }

    const writer = outbound.writable.getWriter();
    const write = writer.write(data);
    const received = await readBytes(inbound.readable, data.length);
    await write;
    await writer.close();

    expect(received).toEqual(data);

    await client.close();
    await server.close();
  });

  it("handles invalid writes, resets, and repeated closes", async () => {
    const session = startHarness();
    const stream = startStream(session);

    await expect(stream.write("bad" as unknown as Uint8Array)).rejects.toThrow(TypeError);
    await stream.finish();
    await stream.finish();
    await expect(stream.write(new Uint8Array([1]))).rejects.toMatchObject({ code: "STREAM_CLOSED" });

    const reset = startStream(session, 3);
    const reason = new YamuxError("CONNECTION_RESET", "reset");
    await reset.reset(reason);
    await reset.reset(new Error("ignored"));
    expect(reset.resetError).toBe(reason);
    await expect(reset.write(new Uint8Array([1]))).rejects.toBe(reason);
    await expect(reset.finish()).rejects.toBe(reason);

    const rejectingResetSession = startHarness(new RejectingResetSession());
    const resetWithRejectedSend = startStream(rejectingResetSession, 27);
    await resetWithRejectedSend.reset(new YamuxError("CONNECTION_RESET", "ignored send failure"));
    expect(resetWithRejectedSend.resetError).toBeInstanceOf(YamuxError);
  });

  it("resumes or rejects writes waiting on remote window", async () => {
    const session = startHarness();
    const resumed = startStream(session, 9);
    resumed.receiveWindowUpdate(0);
    (resumed as unknown as { remoteWindow: number }).remoteWindow = 0;
    const write = resumed.write(new Uint8Array([1]));
    resumed.receiveWindowUpdate(1);
    await expect(write).resolves.toBeUndefined();

    const rejected = startStream(session, 11);
    (rejected as unknown as { remoteWindow: number }).remoteWindow = 0;
    const blocked = rejected.write(new Uint8Array([1]));
    const reason = new YamuxError("CONNECTION_RESET", "blocked");
    await rejected.reset(reason);
    await expect(blocked).rejects.toBe(reason);

    const resetAfterResume = startStream(session, 19);
    (resetAfterResume as unknown as { remoteWindow: number }).remoteWindow = 0;
    const resumedThenReset = resetAfterResume.write(new Uint8Array([1]));
    resetAfterResume.receiveWindowUpdate(1);
    (resetAfterResume as unknown as { resetReason: unknown }).resetReason = reason;
    await expect(resumedThenReset).rejects.toBe(reason);

    const aborted = startStream(session, 21);
    const abortWriter = aborted.writable.getWriter();
    await abortWriter.abort("abort");
    expect(aborted.resetError).toBe("abort");

    const closedByWriter = startStream(session, 25);
    const closeWriter = closedByWriter.writable.getWriter();
    await closeWriter.close();
    expect(closedByWriter.closed).toBe(false);
  });

  it("handles inbound data validation and EOF states", async () => {
    const session = startHarness();
    const stream = startStream(session);

    await stream.receiveData(new Uint8Array(0));
    await (stream as unknown as { releaseReceiveWindow(length: number): Promise<void> }).releaseReceiveWindow(0);
    await stream.receiveFin();
    await stream.receiveFin();
    await expect(stream.receiveData(new Uint8Array([1]))).rejects.toMatchObject({ code: "PROTOCOL_ERROR" });

    const overflow = startStream(session, 5);
    await expect(overflow.receiveData(new Uint8Array(INITIAL_STREAM_WINDOW + 1))).rejects.toMatchObject({
      code: "RECEIVE_WINDOW_EXCEEDED",
    });

    const reset = startStream(session, 7);
    await reset.receiveReset();
    await reset.receiveReset();
    await reset.receiveData(new Uint8Array([1]));
    await reset.receiveFin();
    await (reset as unknown as { releaseReceiveWindow(length: number): Promise<void> }).releaseReceiveWindow(1);

    const disposed = startStream(session, 13);
    await disposed.finish();
    await disposed.receiveFin();
    expect(disposed.closed).toBe(true);
  });

  it("handles window updates and readable cancellation", async () => {
    const session = startHarness();
    const stream = startStream(session);

    stream.receiveWindowUpdate(0);
    expect(() => stream.receiveWindowUpdate(-1)).toThrow(YamuxError);
    expect(() => stream.receiveWindowUpdate(MAX_UINT32 + 1)).toThrow(YamuxError);
    stream.receiveWindowUpdate(MAX_UINT32);

    await stream.readable.cancel("stop");
    expect(stream.resetError).toBe("stop");

    const direct = startStream(session, 15);
    Stream.onReset(direct.context(), direct, Stream.resetEvent);
    expect(direct.resetError).toBeInstanceOf(YamuxError);

    const forced = startStream(session, 17);
    Stream.onForceClose(forced.context(), forced, Stream.forceCloseEvent);
    expect(forced.resetError).toBeInstanceOf(YamuxError);
  });
});
