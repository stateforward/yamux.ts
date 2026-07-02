import * as hsm from "@stateforward/hsm.ts";

import { AsyncQueue, Deferred } from "./async.js";
import { YamuxError } from "./errors.js";
import {
  INITIAL_STREAM_WINDOW,
  MAX_UINT32,
  YamuxFlag,
} from "./protocol.js";
import type { Session } from "./session.js";

const END = Symbol("yamux-stream-end");
type InboundItem = Uint8Array | typeof END;

export type StreamOptions = Readonly<{
  id: number;
  session: Session;
  local: boolean;
  receiveWindow: number;
  maxFrameSize: number;
}>;

export type StreamEvent =
  | "local_syn"
  | "remote_syn"
  | "remote_ack"
  | "local_fin"
  | "remote_fin"
  | "reset"
  | "force_close";

export class Stream extends hsm.Instance {
  readonly id: number;
  readonly session: Session;
  readonly local: boolean;
  readonly receiveWindow: number;
  readonly maxFrameSize: number;
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;

  private readonly inbound = new AsyncQueue<InboundItem>();
  private readonly windowWaiters: Array<Deferred<void>> = [];
  private localWindowRemaining: number;
  private remoteWindow: number;
  private localClosed = false;
  private remoteClosed = false;
  private resetReason: unknown = null;

  static localSynEvent = hsm.Event("local_syn");
  static remoteSynEvent = hsm.Event("remote_syn");
  static remoteAckEvent = hsm.Event("remote_ack");
  static localFinEvent = hsm.Event("local_fin");
  static remoteFinEvent = hsm.Event("remote_fin");
  static resetEvent = hsm.Event("reset");
  static forceCloseEvent = hsm.Event("force_close");

  static model: hsm.Model = hsm.define(
    "Stream",
    hsm.state(
      "idle",
      hsm.transition(
        hsm.on(Stream.localSynEvent),
        hsm.target("/Stream/opening"),
      ),
      hsm.transition(
        hsm.on(Stream.remoteSynEvent),
        hsm.target("/Stream/open"),
      ),
      hsm.transition(
        hsm.on(Stream.resetEvent),
        hsm.target("/Stream/reset"),
        hsm.effect(Stream.onReset as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Stream.forceCloseEvent),
        hsm.target("/Stream/closed"),
        hsm.effect(Stream.onForceClose as hsm.Operation),
      ),
    ),
    hsm.state(
      "opening",
      hsm.transition(
        hsm.on(Stream.remoteAckEvent),
        hsm.target("/Stream/open"),
      ),
      hsm.transition(
        hsm.on(Stream.localFinEvent),
        hsm.target("/Stream/localClosed"),
        hsm.effect(Stream.onLocalFin as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Stream.remoteFinEvent),
        hsm.target("/Stream/remoteClosed"),
        hsm.effect(Stream.onRemoteFin as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Stream.resetEvent),
        hsm.target("/Stream/reset"),
        hsm.effect(Stream.onReset as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Stream.forceCloseEvent),
        hsm.target("/Stream/closed"),
        hsm.effect(Stream.onForceClose as hsm.Operation),
      ),
    ),
    hsm.state(
      "open",
      hsm.transition(
        hsm.on(Stream.localFinEvent),
        hsm.target("/Stream/localClosed"),
        hsm.effect(Stream.onLocalFin as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Stream.remoteFinEvent),
        hsm.target("/Stream/remoteClosed"),
        hsm.effect(Stream.onRemoteFin as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Stream.resetEvent),
        hsm.target("/Stream/reset"),
        hsm.effect(Stream.onReset as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Stream.forceCloseEvent),
        hsm.target("/Stream/closed"),
        hsm.effect(Stream.onForceClose as hsm.Operation),
      ),
    ),
    hsm.state(
      "localClosed",
      hsm.transition(
        hsm.on(Stream.remoteFinEvent),
        hsm.target("/Stream/closed"),
        hsm.effect(Stream.onRemoteFin as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Stream.resetEvent),
        hsm.target("/Stream/reset"),
        hsm.effect(Stream.onReset as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Stream.forceCloseEvent),
        hsm.target("/Stream/closed"),
        hsm.effect(Stream.onForceClose as hsm.Operation),
      ),
    ),
    hsm.state(
      "remoteClosed",
      hsm.transition(
        hsm.on(Stream.localFinEvent),
        hsm.target("/Stream/closed"),
        hsm.effect(Stream.onLocalFin as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Stream.resetEvent),
        hsm.target("/Stream/reset"),
        hsm.effect(Stream.onReset as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Stream.forceCloseEvent),
        hsm.target("/Stream/closed"),
        hsm.effect(Stream.onForceClose as hsm.Operation),
      ),
    ),
    hsm.state(
      "closed",
      hsm.transition(
        hsm.on(Stream.forceCloseEvent),
        hsm.effect(Stream.onForceClose as hsm.Operation),
      ),
    ),
    hsm.state("reset"),
    hsm.initial(hsm.target("/Stream/idle")),
  ) as hsm.Model;

  constructor(options: StreamOptions) {
    super();
    this.id = assertPositiveUint32(options.id, "id");
    assertSession(options.session);
    if (typeof options.local !== "boolean") {
      throw new TypeError("local must be a boolean");
    }
    this.session = options.session;
    this.local = options.local;
    this.receiveWindow = Math.max(INITIAL_STREAM_WINDOW, assertUint32(options.receiveWindow, "receiveWindow"));
    this.maxFrameSize = assertPositiveUint32(options.maxFrameSize, "maxFrameSize");
    this.localWindowRemaining = this.receiveWindow;
    this.remoteWindow = INITIAL_STREAM_WINDOW;

    this.readable = new ReadableStream<Uint8Array>({
      pull: async (controller) => {
        try {
          const item = await this.inbound.shift();
          if (item === END) {
            controller.close();
            return;
          }
          controller.enqueue(item);
          await this.releaseReceiveWindow(item.byteLength);
        } catch (error) {
          controller.error(error);
        }
      },
      cancel: (reason) => {
        void this.reset(reason);
      },
    });

    this.writable = new WritableStream<Uint8Array>({
      write: (chunk) => this.write(chunk),
      close: () => this.finish(),
      abort: (reason) => this.reset(reason),
    });
  }

  get closed(): boolean {
    return this.localClosed && this.remoteClosed;
  }

  get resetError(): unknown {
    return this.resetReason;
  }

  get extraReceiveWindow(): number {
    return this.receiveWindow - INITIAL_STREAM_WINDOW;
  }

  async write(chunk: Uint8Array): Promise<void> {
    if (!(chunk instanceof Uint8Array)) {
      throw new TypeError("yamux streams only accept Uint8Array chunks");
    }
    if (this.localClosed) {
      throw new YamuxError("STREAM_CLOSED", `stream ${this.id} is locally closed`);
    }
    if (this.resetReason) {
      throw this.resetReason;
    }

    let offset = 0;
    while (offset < chunk.byteLength) {
      await this.waitForRemoteWindow();
      if (this.resetReason) {
        throw this.resetReason;
      }

      const length = Math.min(
        chunk.byteLength - offset,
        this.maxFrameSize,
        this.remoteWindow,
      );
      const payload = chunk.subarray(offset, offset + length);
      this.remoteWindow -= length;
      offset += length;
      await this.session.sendData(this.id, 0, payload);
    }
  }

  async finish(): Promise<void> {
    if (this.localClosed) {
      return;
    }
    if (this.resetReason) {
      throw this.resetReason;
    }
    this.localClosed = true;
    await this.session.sendData(this.id, YamuxFlag.FIN, new Uint8Array(0));
    await this.dispatch(Stream.localFinEvent);
    this.maybeDispose();
  }

  async reset(reason: unknown = new YamuxError("CONNECTION_RESET", `stream ${this.id} reset`)): Promise<void> {
    if (!this.resetReason) {
      this.resetReason = reason;
      this.rejectWindowWaiters(reason);
      this.inbound.close(reason);
      await this.session.sendReset(this.id).catch(ignoreError);
      await this.dispatch({ ...Stream.resetEvent, data: reason });
      this.session.deleteStream(this.id);
    }
  }

  async acceptSyn(): Promise<void> {
    await this.dispatch(Stream.remoteSynEvent);
  }

  async sentSyn(): Promise<void> {
    await this.dispatch(Stream.localSynEvent);
  }

  async receiveAck(): Promise<void> {
    await this.dispatch(Stream.remoteAckEvent);
  }

  receiveWindowUpdate(delta: number): void {
    if (delta === 0) {
      return;
    }
    if (!Number.isInteger(delta) || delta < 0 || delta > MAX_UINT32) {
      throw new YamuxError("INVALID_FRAME", `invalid window update delta for stream ${this.id}`);
    }
    this.remoteWindow = Math.min(MAX_UINT32, this.remoteWindow + delta);
    for (const waiter of this.windowWaiters.splice(0)) {
      waiter.resolve();
    }
  }

  async receiveData(payload: Uint8Array): Promise<void> {
    if (this.resetReason) {
      return;
    }
    if (this.remoteClosed) {
      throw new YamuxError("PROTOCOL_ERROR", `stream ${this.id} received data after FIN`);
    }
    if (payload.byteLength > this.localWindowRemaining) {
      throw new YamuxError("RECEIVE_WINDOW_EXCEEDED", `stream ${this.id} exceeded receive window`);
    }

    this.localWindowRemaining -= payload.byteLength;
    if (payload.byteLength > 0) {
      this.inbound.push(payload);
    }
  }

  async receiveFin(): Promise<void> {
    if (this.resetReason) {
      return;
    }
    if (this.remoteClosed) {
      return;
    }
    this.remoteClosed = true;
    this.inbound.push(END);
    await this.dispatch(Stream.remoteFinEvent);
    this.maybeDispose();
  }

  async receiveReset(reason: unknown = new YamuxError("CONNECTION_RESET", `stream ${this.id} reset by peer`)): Promise<void> {
    if (this.resetReason) {
      return;
    }
    this.resetReason = reason;
    this.rejectWindowWaiters(reason);
    this.inbound.close(reason);
    await this.dispatch({ ...Stream.resetEvent, data: reason });
    this.session.deleteStream(this.id);
  }

  async forceClose(reason: unknown = new YamuxError("SESSION_CLOSED", "session closed")): Promise<void> {
    this.localClosed = true;
    this.remoteClosed = true;
    this.rejectWindowWaiters(reason);
    this.inbound.close(reason);
    await this.dispatch({ ...Stream.forceCloseEvent, data: reason });
    this.session.deleteStream(this.id);
  }

  private async releaseReceiveWindow(length: number): Promise<void> {
    if (length === 0 || this.resetReason) {
      return;
    }
    this.localWindowRemaining = Math.min(this.receiveWindow, this.localWindowRemaining + length);
    await this.session.sendWindowUpdate(this.id, 0, length);
  }

  private async waitForRemoteWindow(): Promise<void> {
    while (this.remoteWindow <= 0) {
      const waiter = new Deferred<void>();
      this.windowWaiters.push(waiter);
      await waiter.promise;
    }
  }

  private maybeDispose(): void {
    if (this.closed || this.resetReason) {
      this.session.deleteStream(this.id);
    }
  }

  private rejectWindowWaiters(reason: unknown): void {
    for (const waiter of this.windowWaiters.splice(0)) {
      waiter.reject(reason);
    }
  }

  static onLocalFin(_ctx: hsm.Context, stream: Stream, _event: hsm.Event): void {
    stream.localClosed = true;
  }

  static onRemoteFin(_ctx: hsm.Context, stream: Stream, _event: hsm.Event): void {
    stream.remoteClosed = true;
  }

  static onReset(_ctx: hsm.Context, stream: Stream, event: hsm.Event): void {
    stream.resetReason = event.data ?? new YamuxError("CONNECTION_RESET", `stream ${stream.id} reset`);
  }

  static onForceClose(_ctx: hsm.Context, stream: Stream, event: hsm.Event): void {
    stream.resetReason = event.data ?? new YamuxError("SESSION_CLOSED", "session closed");
  }
}

function ignoreError(): void {}

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

function assertSession(value: Session): void {
  if (!value || typeof value.sendData !== "function" || typeof value.sendWindowUpdate !== "function") {
    throw new TypeError("session must be a yamux Session");
  }
}
