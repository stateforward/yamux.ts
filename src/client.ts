import * as hsm from "@stateforward/hsm.ts";

import { Session, type SessionOptions } from "./session.js";

export type ClientOptions = Omit<SessionOptions, "role">;

export const clientStreamIDs = {
  first: 1,
  step: 2,
} as const;

export type ClientEvent =
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

export class Client extends Session {
  static override model: hsm.Model = hsm.define(
    "Client",
    hsm.state(
      "idle",
      hsm.transition(
        hsm.on(Client.startEvent),
        hsm.target("/Client/open"),
        hsm.effect(Client.onStart as hsm.Operation),
      ),
    ),
    hsm.state(
      "open",
      hsm.transition(hsm.on(Client.frameEvent), hsm.effect(Client.onFrame as hsm.Operation)),
      hsm.transition(hsm.on(Client.openStreamEvent), hsm.effect(Client.onOpenStream as hsm.Operation)),
      hsm.transition(hsm.on(Client.acceptStreamEvent), hsm.effect(Client.onAcceptStream as hsm.Operation)),
      hsm.transition(hsm.on(Client.streamOpenedEvent), hsm.effect(Client.onStreamOpened as hsm.Operation)),
      hsm.transition(hsm.on(Client.streamAcceptedEvent), hsm.effect(Client.onStreamAccepted as hsm.Operation)),
      hsm.transition(
        hsm.on(Client.sendGoAwayEvent),
        hsm.target("/Client/draining"),
        hsm.effect(Client.onSendGoAway as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Client.remoteGoAwayEvent),
        hsm.target("/Client/draining"),
        hsm.effect(Client.onRemoteGoAway as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Client.closeEvent),
        hsm.target("/Client/closing"),
        hsm.effect(Client.onClose as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Client.transportClosedEvent),
        hsm.target("/Client/closed"),
        hsm.effect(Client.onTransportClosed as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Client.errorEvent),
        hsm.target("/Client/closing"),
        hsm.effect(Client.onError as hsm.Operation),
      ),
    ),
    hsm.state(
      "draining",
      hsm.transition(hsm.on(Client.frameEvent), hsm.effect(Client.onFrame as hsm.Operation)),
      hsm.transition(
        hsm.on(Client.closeEvent),
        hsm.target("/Client/closing"),
        hsm.effect(Client.onClose as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Client.transportClosedEvent),
        hsm.target("/Client/closed"),
        hsm.effect(Client.onTransportClosed as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Client.errorEvent),
        hsm.target("/Client/closing"),
        hsm.effect(Client.onError as hsm.Operation),
      ),
    ),
    hsm.state(
      "closing",
      hsm.transition(
        hsm.on(Client.transportClosedEvent),
        hsm.target("/Client/closed"),
        hsm.effect(Client.onTransportClosed as hsm.Operation),
      ),
    ),
    hsm.state("closed"),
    hsm.initial(hsm.target("/Client/idle")),
  ) as hsm.Model;

  constructor(options: ClientOptions) {
    super({ ...options, role: "client" });
  }
}

export function createClient(options: ClientOptions): Client {
  const client = hsm.start(new Client(options), Client.model) as unknown as Client;
  return client.activate();
}
