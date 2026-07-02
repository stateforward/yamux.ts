import * as hsm from "@stateforward/hsm.ts";

import { AsyncQueue, Deferred, withTimeout } from "./async.js";
import { ByteReader } from "./byte-reader.js";
import { timeoutError, YamuxError } from "./errors.js";
import {
  HEADER_SIZE,
  INITIAL_STREAM_WINDOW,
  MAX_UINT32,
  PROTOCOL_VERSION,
  decodeHeader,
  frameToBytes,
  hasFlag,
  YamuxFlag,
  YamuxFrameType,
  YamuxGoAwayCode,
  type YamuxFrame,
  type YamuxHeader,
} from "./protocol.js";
import { Stream } from "./stream.js";

export type SessionRole = "client" | "server";

export type SessionOptions = Readonly<{
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
  role?: SessionRole;
  acceptBacklog?: number;
  receiveWindow?: number;
  maxFrameSize?: number;
}>;

export type WaitOptions = Readonly<{
  signal?: AbortSignal;
  timeoutMs?: number;
}>;

export type PingOptions = WaitOptions & Readonly<{
  value?: number;
}>;

export type SessionEvent =
  | "start"
  | "frame"
  | "open_stream"
  | "accept_stream"
  | "stream_opened"
  | "stream_accepted"
  | "send_goaway"
  | "remote_goaway"
  | "close"
  | "transport_closed"
  | "error";

type PendingPing = {
  started: number;
  deferred: Deferred<number>;
};

export class Session extends hsm.Instance {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  readonly role: SessionRole;
  readonly acceptBacklog: number;
  readonly receiveWindow: number;
  readonly maxFrameSize: number;

  protected readonly streams = new Map<number, Stream>();
  protected readonly incoming = new AsyncQueue<Stream>();
  protected readonly pendingPings = new Map<number, PendingPing>();

  protected writer: WritableStreamDefaultWriter<Uint8Array> | null = null;
  protected readTask: Promise<void> | null = null;
  protected writeChain: Promise<void> = Promise.resolve();
  protected nextStreamID: number;
  protected nextPingValue = 1;
  protected goAwaySent = false;
  protected goAwayReceived = false;
  protected transportClosed = false;

  lastError: unknown;
  goAwayCode: YamuxGoAwayCode | null;

  static startEvent = hsm.Event("start");
  static frameEvent = hsm.Event("frame");
  static openStreamEvent = hsm.Event("open_stream");
  static acceptStreamEvent = hsm.Event("accept_stream");
  static streamOpenedEvent = hsm.Event("stream_opened");
  static streamAcceptedEvent = hsm.Event("stream_accepted");
  static sendGoAwayEvent = hsm.Event("send_goaway");
  static remoteGoAwayEvent = hsm.Event("remote_goaway");
  static closeEvent = hsm.Event("close");
  static transportClosedEvent = hsm.Event("transport_closed");
  static errorEvent = hsm.Event("error");

  static model: hsm.Model = hsm.define(
    "Session",
    hsm.state(
      "idle",
      hsm.transition(
        hsm.on(Session.startEvent),
        hsm.target("/Session/open"),
        hsm.effect(Session.onStart as hsm.Operation),
      ),
    ),
    hsm.state(
      "open",
      hsm.transition(hsm.on(Session.frameEvent), hsm.effect(Session.onFrame as hsm.Operation)),
      hsm.transition(hsm.on(Session.openStreamEvent), hsm.effect(Session.onOpenStream as hsm.Operation)),
      hsm.transition(hsm.on(Session.acceptStreamEvent), hsm.effect(Session.onAcceptStream as hsm.Operation)),
      hsm.transition(hsm.on(Session.streamOpenedEvent), hsm.effect(Session.onStreamOpened as hsm.Operation)),
      hsm.transition(hsm.on(Session.streamAcceptedEvent), hsm.effect(Session.onStreamAccepted as hsm.Operation)),
      hsm.transition(
        hsm.on(Session.sendGoAwayEvent),
        hsm.target("/Session/draining"),
        hsm.effect(Session.onSendGoAway as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Session.remoteGoAwayEvent),
        hsm.target("/Session/draining"),
        hsm.effect(Session.onRemoteGoAway as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Session.closeEvent),
        hsm.target("/Session/closing"),
        hsm.effect(Session.onClose as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Session.transportClosedEvent),
        hsm.target("/Session/closed"),
        hsm.effect(Session.onTransportClosed as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Session.errorEvent),
        hsm.target("/Session/closing"),
        hsm.effect(Session.onError as hsm.Operation),
      ),
    ),
    hsm.state(
      "draining",
      hsm.transition(hsm.on(Session.frameEvent), hsm.effect(Session.onFrame as hsm.Operation)),
      hsm.transition(
        hsm.on(Session.closeEvent),
        hsm.target("/Session/closing"),
        hsm.effect(Session.onClose as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Session.transportClosedEvent),
        hsm.target("/Session/closed"),
        hsm.effect(Session.onTransportClosed as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Session.errorEvent),
        hsm.target("/Session/closing"),
        hsm.effect(Session.onError as hsm.Operation),
      ),
    ),
    hsm.state(
      "closing",
      hsm.transition(
        hsm.on(Session.transportClosedEvent),
        hsm.target("/Session/closed"),
        hsm.effect(Session.onTransportClosed as hsm.Operation),
      ),
    ),
    hsm.state("closed"),
    hsm.initial(hsm.target("/Session/idle")),
  ) as hsm.Model;

  constructor(options: SessionOptions) {
    super();
    assertReadableStream(options.readable, "readable");
    assertWritableStream(options.writable, "writable");

    const role = options.role ?? "client";
    if (role !== "client" && role !== "server") {
      throw new RangeError("role must be either client or server");
    }

    this.readable = options.readable;
    this.writable = options.writable;
    this.role = role;
    this.acceptBacklog = assertNonNegativeInteger(options.acceptBacklog ?? 256, "acceptBacklog");
    this.receiveWindow = Math.max(
      INITIAL_STREAM_WINDOW,
      assertUint32(options.receiveWindow ?? INITIAL_STREAM_WINDOW, "receiveWindow"),
    );
    this.maxFrameSize = assertPositiveUint32(options.maxFrameSize ?? 64 * 1024, "maxFrameSize");
    this.nextStreamID = this.role === "client" ? 1 : 2;
    this.lastError = null;
    this.goAwayCode = null;
  }

  activate(): this {
    this.startTransport();
    void this.dispatch(Session.startEvent);
    return this;
  }

  async openStream(options: WaitOptions = {}): Promise<Stream> {
    return withTimeout(
      (async () => {
        this.assertCanOpenStream();
        await this.dispatch(Session.openStreamEvent);
        const stream = await this.createOutboundStream();
        await this.dispatch({ ...Session.streamOpenedEvent, data: { stream } });
        return stream;
      })(),
      options.timeoutMs,
      options.signal,
    );
  }

  async acceptStream(options: WaitOptions = {}): Promise<Stream> {
    if (this.goAwayReceived && this.incoming.length === 0) {
      throw new YamuxError("GO_AWAY", "peer sent go away; no new streams will arrive");
    }
    await this.dispatch(Session.acceptStreamEvent);
    return this.shiftIncoming(options);
  }

  async ping(options: PingOptions = {}): Promise<number> {
    if (this.transportClosed) {
      throw new YamuxError("SESSION_CLOSED", "cannot ping a closed session");
    }

    const value = options.value ?? this.allocatePingValue();
    assertUint32(value, "ping value");

    const deferred = new Deferred<number>();
    this.pendingPings.set(value, {
      started: now(),
      deferred,
    });

    try {
      await this.sendFrame(YamuxFrameType.Ping, YamuxFlag.SYN, 0, value);
      return await withTimeout(deferred.promise, options.timeoutMs, options.signal);
    } catch (error) {
      this.pendingPings.delete(value);
      throw error;
    }
  }

  async goAway(code: YamuxGoAwayCode = YamuxGoAwayCode.Normal): Promise<void> {
    assertGoAwayCode(code);
    if (this.goAwaySent) {
      return;
    }
    this.goAwaySent = true;
    this.goAwayCode = code;
    await this.sendFrame(YamuxFrameType.GoAway, 0, 0, code);
    await this.dispatch({ ...Session.sendGoAwayEvent, data: code });
  }

  async close(code: YamuxGoAwayCode = YamuxGoAwayCode.Normal): Promise<void> {
    if (this.transportClosed) {
      return;
    }

    await this.dispatch({ ...Session.closeEvent, data: code });
    await this.goAway(code);
    const reason = new YamuxError("SESSION_CLOSED", "session closed");
    for (const stream of [...this.streams.values()]) {
      await stream.forceClose(reason);
    }
    this.incoming.close(reason);
    await this.closeWriter();
    await this.markTransportClosed(reason);
  }

  sendData(streamID: number, flags: number, payload: Uint8Array): Promise<void> {
    return this.sendFrame(YamuxFrameType.Data, flags, streamID, payload.byteLength, payload);
  }

  sendWindowUpdate(streamID: number, flags: number, delta: number): Promise<void> {
    return this.sendFrame(YamuxFrameType.WindowUpdate, flags, streamID, delta);
  }

  sendReset(streamID: number): Promise<void> {
    return this.sendWindowUpdate(streamID, YamuxFlag.RST, 0);
  }

  deleteStream(streamID: number): void {
    this.streams.delete(streamID);
  }

  protected startTransport(): void {
    if (this.readTask) {
      return;
    }
    this.writer = this.writable.getWriter();
    this.readTask = this.readLoop();
  }

  protected async readLoop(): Promise<void> {
    const reader = new ByteReader(this.readable);
    try {
      for (;;) {
        const headerBytes = await reader.readExactly(HEADER_SIZE);
        if (headerBytes === null) {
          break;
        }
        const header = decodeHeader(headerBytes);
        this.validateHeader(header);

        const payload =
          header.type === YamuxFrameType.Data && header.length > 0
            ? await reader.readExactly(header.length)
            : new Uint8Array(0);

        if (payload === null) {
          throw new YamuxError("INVALID_FRAME", "data frame ended before payload");
        }

        const frame: YamuxFrame = { header, payload };
        await this.dispatch({ ...Session.frameEvent, data: { frame } });
        await this.handleFrame(frame);
      }
      await this.markTransportClosed(new YamuxError("SESSION_CLOSED", "session transport closed"));
    } catch (error) {
      await this.fail(error);
    } finally {
      reader.releaseLock();
    }
  }

  protected async handleFrame(frame: YamuxFrame): Promise<void> {
    switch (frame.header.type) {
      case YamuxFrameType.Data:
      case YamuxFrameType.WindowUpdate:
        await this.handleStreamFrame(frame);
        return;
      case YamuxFrameType.Ping:
        await this.handlePing(frame.header);
        return;
      case YamuxFrameType.GoAway:
        await this.handleGoAway(frame.header);
        return;
    }
  }

  protected async handleStreamFrame(frame: YamuxFrame): Promise<void> {
    const { header, payload } = frame;
    if (header.streamID === 0) {
      throw new YamuxError("PROTOCOL_ERROR", "data and window update frames require a stream id");
    }

    let stream = this.streams.get(header.streamID);

    if (hasFlag(header.flags, YamuxFlag.RST)) {
      if (stream) {
        await stream.receiveReset();
      }
      return;
    }

    if (hasFlag(header.flags, YamuxFlag.SYN)) {
      if (stream) {
        await this.sendReset(header.streamID);
        throw new YamuxError("DUPLICATE_STREAM", `duplicate stream id ${header.streamID}`);
      }
      stream = await this.createInboundStream(header.streamID);
    }

    if (!stream) {
      await this.sendReset(header.streamID);
      return;
    }

    if (hasFlag(header.flags, YamuxFlag.ACK)) {
      await stream.receiveAck();
    }

    if (header.type === YamuxFrameType.WindowUpdate) {
      stream.receiveWindowUpdate(header.length);
    } else {
      await stream.receiveData(payload);
    }

    if (hasFlag(header.flags, YamuxFlag.FIN)) {
      await stream.receiveFin();
    }
  }

  protected async handlePing(header: YamuxHeader): Promise<void> {
    if (header.streamID !== 0) {
      throw new YamuxError("PROTOCOL_ERROR", "ping frames must use stream id 0");
    }
    if (header.flags === YamuxFlag.SYN) {
      await this.sendFrame(YamuxFrameType.Ping, YamuxFlag.ACK, 0, header.length);
      return;
    }
    if (header.flags === YamuxFlag.ACK) {
      const pending = this.pendingPings.get(header.length);
      if (pending) {
        this.pendingPings.delete(header.length);
        pending.deferred.resolve(now() - pending.started);
      }
      return;
    }
    throw new YamuxError("PROTOCOL_ERROR", "ping frames must use SYN or ACK");
  }

  protected async handleGoAway(header: YamuxHeader): Promise<void> {
    if (header.streamID !== 0) {
      throw new YamuxError("PROTOCOL_ERROR", "go away frames must use stream id 0");
    }
    if (!isGoAwayCode(header.length)) {
      throw new YamuxError("PROTOCOL_ERROR", `invalid go away code ${header.length}`);
    }

    this.goAwayReceived = true;
    this.goAwayCode = header.length;
    await this.dispatch({ ...Session.remoteGoAwayEvent, data: header.length });
  }

  protected async createOutboundStream(): Promise<Stream> {
    const streamID = this.allocateStreamID();
    const stream = hsm.start(new Stream({
      id: streamID,
      session: this,
      local: true,
      receiveWindow: this.receiveWindow,
      maxFrameSize: this.maxFrameSize,
    }), Stream.model) as unknown as Stream;

    this.streams.set(streamID, stream);
    await stream.sentSyn();
    await this.sendWindowUpdate(streamID, YamuxFlag.SYN, stream.extraReceiveWindow);
    return stream;
  }

  protected async createInboundStream(streamID: number): Promise<Stream> {
    if (!this.isRemoteStreamID(streamID)) {
      await this.sendReset(streamID);
      throw new YamuxError("INVALID_STREAM", `peer opened stream with invalid id ${streamID}`);
    }
    if (this.goAwaySent || this.goAwayReceived || this.incoming.length >= this.acceptBacklog) {
      await this.sendReset(streamID);
      throw new YamuxError("GO_AWAY", "session is not accepting new streams");
    }

    const stream = hsm.start(new Stream({
      id: streamID,
      session: this,
      local: false,
      receiveWindow: this.receiveWindow,
      maxFrameSize: this.maxFrameSize,
    }), Stream.model) as unknown as Stream;

    this.streams.set(streamID, stream);
    await stream.acceptSyn();
    await this.sendWindowUpdate(streamID, YamuxFlag.ACK, stream.extraReceiveWindow);
    this.incoming.push(stream);
    await this.dispatch({ ...Session.streamAcceptedEvent, data: { stream } });
    return stream;
  }

  protected async sendFrame(
    type: YamuxFrameType,
    flags: number,
    streamID: number,
    length: number,
    payload?: Uint8Array,
  ): Promise<void> {
    if (this.transportClosed) {
      throw new YamuxError("SESSION_CLOSED", "session transport is closed");
    }
    const bytes = frameToBytes(type, flags, streamID, length, payload);
    const writer = this.writer;
    if (!writer) {
      throw new YamuxError("SESSION_CLOSED", "session transport has not started");
    }

    this.writeChain = this.writeChain.then(() => writer.write(bytes));
    return this.writeChain;
  }

  protected async shiftIncoming(options: WaitOptions): Promise<Stream> {
    if (options.timeoutMs === undefined) {
      return this.incoming.shift(options.signal);
    }
    assertNonNegativeFinite(options.timeoutMs, "timeoutMs");

    const timeoutController = new AbortController();
    const signal = combineSignals(options.signal, timeoutController.signal);
    const timeoutID = setTimeout(() => timeoutController.abort(), options.timeoutMs);

    try {
      const stream = await this.incoming.shift(signal);
      clearTimeout(timeoutID);
      return stream;
    } catch (error) {
      clearTimeout(timeoutID);
      if (timeoutController.signal.aborted && !options.signal?.aborted) {
        throw timeoutError();
      }
      throw error;
    }
  }

  protected async fail(error: unknown): Promise<void> {
    this.lastError = error;
    await this.dispatch({ ...Session.errorEvent, data: error });

    if (!this.goAwaySent && !this.transportClosed) {
      try {
        await this.goAway(YamuxGoAwayCode.ProtocolError);
      } catch {
        // The transport may already be gone; stream/session cleanup below is authoritative.
      }
    }

    const reason = error instanceof Error ? error : new YamuxError("PROTOCOL_ERROR", "yamux session failed");
    for (const stream of [...this.streams.values()]) {
      await stream.forceClose(reason);
    }
    this.incoming.close(reason);
    for (const pending of this.pendingPings.values()) {
      pending.deferred.reject(reason);
    }
    this.pendingPings.clear();
    await this.markTransportClosed(reason);
  }

  protected async closeWriter(): Promise<void> {
    const writer = this.writer;
    if (!writer) {
      return;
    }
    this.writer = null;
    await this.writeChain.catch(ignoreError);
    await writer.close().catch(ignoreError);
    writer.releaseLock();
  }

  protected async markTransportClosed(reason: unknown = new YamuxError("SESSION_CLOSED", "session transport closed")): Promise<void> {
    if (this.transportClosed) {
      return;
    }
    this.transportClosed = true;
    for (const stream of [...this.streams.values()]) {
      await stream.forceClose(reason).catch(ignoreError);
    }
    this.incoming.close(reason);
    for (const pending of this.pendingPings.values()) {
      pending.deferred.reject(reason);
    }
    this.pendingPings.clear();
    await this.closeWriter();
    await this.dispatch(Session.transportClosedEvent);
  }

  protected validateHeader(header: YamuxHeader): void {
    if (header.version !== PROTOCOL_VERSION) {
      throw new YamuxError("PROTOCOL_ERROR", `unsupported yamux version ${header.version}`);
    }
    if (header.type === YamuxFrameType.GoAway && header.flags !== 0) {
      throw new YamuxError("PROTOCOL_ERROR", "go away frames must not set flags");
    }
  }

  protected assertCanOpenStream(): void {
    if (this.transportClosed) {
      throw new YamuxError("SESSION_CLOSED", "session is closed");
    }
    if (this.goAwaySent || this.goAwayReceived) {
      throw new YamuxError("GO_AWAY", "session is draining and cannot open new streams");
    }
  }

  protected allocateStreamID(): number {
    const streamID = this.nextStreamID;
    if (streamID <= 0 || streamID > MAX_UINT32) {
      throw new YamuxError("STREAMS_EXHAUSTED", "yamux stream ids exhausted");
    }
    this.nextStreamID += 2;
    return streamID;
  }

  protected allocatePingValue(): number {
    const value = this.nextPingValue;
    this.nextPingValue = this.nextPingValue === MAX_UINT32 ? 1 : this.nextPingValue + 1;
    return value;
  }

  protected isRemoteStreamID(streamID: number): boolean {
    return this.role === "client" ? streamID % 2 === 0 : streamID % 2 === 1;
  }

  static onStart(_ctx: hsm.Context, session: Session, _event: hsm.Event): void {
    session.startTransport();
  }

  static onFrame(_ctx: hsm.Context, _session: Session, _event: hsm.Event): void {}

  static onOpenStream(_ctx: hsm.Context, _session: Session, _event: hsm.Event): void {}

  static onAcceptStream(_ctx: hsm.Context, _session: Session, _event: hsm.Event): void {}

  static onStreamOpened(_ctx: hsm.Context, _session: Session, _event: hsm.Event): void {}

  static onStreamAccepted(_ctx: hsm.Context, _session: Session, _event: hsm.Event): void {}

  static onSendGoAway(_ctx: hsm.Context, session: Session, event: hsm.Event): void {
    session.goAwayCode = (event.data as YamuxGoAwayCode | undefined) ?? YamuxGoAwayCode.Normal;
  }

  static onRemoteGoAway(_ctx: hsm.Context, session: Session, event: hsm.Event): void {
    session.goAwayReceived = true;
    session.goAwayCode = (event.data as YamuxGoAwayCode | undefined) ?? YamuxGoAwayCode.Normal;
  }

  static onClose(_ctx: hsm.Context, _session: Session, _event: hsm.Event): void {}

  static onTransportClosed(_ctx: hsm.Context, _session: Session, _event: hsm.Event): void {}

  static onError(_ctx: hsm.Context, session: Session, event: hsm.Event): void {
    session.lastError = event.data ?? null;
  }
}

function isGoAwayCode(value: number): value is YamuxGoAwayCode {
  return (
    value === YamuxGoAwayCode.Normal ||
    value === YamuxGoAwayCode.ProtocolError ||
    value === YamuxGoAwayCode.InternalError
  );
}

function assertGoAwayCode(value: YamuxGoAwayCode): void {
  if (!isGoAwayCode(value)) {
    throw new RangeError(`invalid go away code ${value}`);
  }
}

function assertUint32(value: number, name: string): number {
  if (!Number.isInteger(value) || value < 0 || value > MAX_UINT32) {
    throw new RangeError(`${name} must be an unsigned 32-bit integer`);
  }
  return value;
}

function assertPositiveUint32(value: number, name: string): number {
  assertUint32(value, name);
  if (value === 0) {
    throw new RangeError(`${name} must be greater than zero`);
  }
  return value;
}

function assertNonNegativeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative safe integer`);
  }
  return value;
}

function assertNonNegativeFinite(value: number, name: string): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative finite number`);
  }
}

function assertReadableStream(value: ReadableStream<Uint8Array>, name: string): void {
  if (!value || typeof value.getReader !== "function") {
    throw new TypeError(`${name} must be a ReadableStream`);
  }
}

function assertWritableStream(value: WritableStream<Uint8Array>, name: string): void {
  if (!value || typeof value.getWriter !== "function") {
    throw new TypeError(`${name} must be a WritableStream`);
  }
}

function now(): number {
  return globalThis.performance?.now?.() ?? Date.now();
}

function ignoreError(): void {}

function combineSignals(first: AbortSignal | undefined, second: AbortSignal): AbortSignal {
  if (!first) {
    return second;
  }
  if (first.aborted) {
    return first;
  }

  const controller = new AbortController();
  const abort = (): void => controller.abort();
  first.addEventListener("abort", abort, { once: true });
  second.addEventListener("abort", abort, { once: true });
  return controller.signal;
}
