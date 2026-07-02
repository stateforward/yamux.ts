import * as hsm from "@stateforward/hsm.ts";

import { Deferred } from "../src/async.js";
import { YamuxGoAwayCode, type YamuxHeader } from "../src/protocol.js";
import { Session, type WaitOptions } from "../src/session.js";
import { Stream } from "../src/stream.js";

export type TransportSide = {
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
};

export function emptyReadable(): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.close();
    },
  });
}

export function chunksReadable(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(chunk);
      }
      controller.close();
    },
  });
}

export function pendingReadable(): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>();
}

export function writableSink(): WritableStream<Uint8Array> {
  return new WritableStream<Uint8Array>();
}

export function duplexPair(): { client: TransportSide; server: TransportSide } {
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

export function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((length, chunk) => length + chunk.byteLength, 0);
  const output = new Uint8Array(total);
  let offset = 0;

  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return output;
}

export async function collect(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];

  for (;;) {
    const result = await reader.read();
    if (result.done) {
      break;
    }
    chunks.push(result.value);
  }

  reader.releaseLock();
  return concatBytes(chunks);
}

export async function readBytes(stream: ReadableStream<Uint8Array>, length: number): Promise<Uint8Array> {
  const reader = stream.getReader();
  const output = new Uint8Array(length);
  let offset = 0;

  while (offset < length) {
    const result = await reader.read();
    if (result.done) {
      throw new Error("stream ended before expected bytes were read");
    }
    const chunk = result.value.subarray(0, length - offset);
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }

  reader.releaseLock();
  return output;
}

export class HarnessSession extends Session {
  constructor(role: "client" | "server" = "client", readable = pendingReadable(), writable = writableSink()) {
    super({ readable, writable, role });
  }

  exposeHandleStreamFrame(frame: { header: YamuxHeader; payload: Uint8Array }): Promise<void> {
    return Promise.resolve().then(() => this.handleStreamFrame(frame));
  }

  exposeHandlePing(header: YamuxHeader): Promise<void> {
    return this.handlePing(header);
  }

  exposeHandleGoAway(header: YamuxHeader): Promise<void> {
    return this.handleGoAway(header);
  }

  exposeValidateHeader(header: YamuxHeader): void {
    this.validateHeader(header);
  }

  exposeAllocateStreamID(): number {
    return this.allocateStreamID();
  }

  exposeAllocatePingValue(): number {
    return this.allocatePingValue();
  }

  exposeShiftIncoming(options: WaitOptions): Promise<Stream> {
    return this.shiftIncoming(options);
  }

  exposeMarkTransportClosed(): Promise<void> {
    return this.markTransportClosed();
  }

  exposeCloseWriter(): Promise<void> {
    return this.closeWriter();
  }

  exposeFail(error: unknown): Promise<void> {
    return this.fail(error);
  }

  exposeReadLoop(): Promise<void> {
    return this.readLoop();
  }

  setTransportClosed(value: boolean): void {
    this.transportClosed = value;
  }

  setGoAwaySent(value: boolean): void {
    this.goAwaySent = value;
  }

  setGoAwayReceived(value: boolean): void {
    this.goAwayReceived = value;
  }

  setNextStreamID(value: number): void {
    this.nextStreamID = value;
  }

  setNextPingValue(value: number): void {
    this.nextPingValue = value;
  }

  addStream(stream: Stream): void {
    this.streams.set(stream.id, stream);
  }

  addIncoming(stream: Stream): void {
    this.incoming.push(stream);
  }

  addPendingPing(value: number, deferred: Deferred<number>): void {
    this.pendingPings.set(value, { started: 1, deferred });
  }

  pendingPingCount(): number {
    return this.pendingPings.size;
  }

  setWriter(writer: WritableStreamDefaultWriter<Uint8Array> | null): void {
    this.writer = writer;
  }

  setWriteChain(writeChain: Promise<void>): void {
    this.writeChain = writeChain;
  }
}

export class FailingGoAwaySession extends HarnessSession {
  override goAway(_code?: YamuxGoAwayCode): Promise<void> {
    return Promise.reject(new Error("go away failed"));
  }
}

export class RejectingResetSession extends HarnessSession {
  override sendReset(_streamID: number): Promise<void> {
    return Promise.reject(new Error("reset failed"));
  }
}

export function startHarness(session = new HarnessSession()): HarnessSession {
  const started = hsm.start(session, Session.model) as unknown as HarnessSession;
  started.setWriter(writableSink().getWriter());
  return started;
}

export function startStream(session: Session, id = 1): Stream {
  return new Stream({
    id,
    session,
    local: true,
    receiveWindow: 4,
    maxFrameSize: 2,
  });
}
