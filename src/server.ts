import * as hsm from "@stateforward/hsm.ts";

import { Session, type SessionOptions } from "./session.js";

export type ServerOptions = Omit<SessionOptions, "role">;

export const serverStreamIDs = {
  first: 2,
  step: 2,
} as const;

export type ServerEvent =
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

export class Server extends Session {
  static override model: hsm.Model = hsm.define(
    "Server",
    hsm.state(
      "idle",
      hsm.transition(
        hsm.on(Server.startEvent),
        hsm.target("/Server/open"),
        hsm.effect(Server.onStart as hsm.Operation),
      ),
    ),
    hsm.state(
      "open",
      hsm.transition(hsm.on(Server.frameEvent), hsm.effect(Server.onFrame as hsm.Operation)),
      hsm.transition(hsm.on(Server.openStreamEvent), hsm.effect(Server.onOpenStream as hsm.Operation)),
      hsm.transition(hsm.on(Server.acceptStreamEvent), hsm.effect(Server.onAcceptStream as hsm.Operation)),
      hsm.transition(hsm.on(Server.streamOpenedEvent), hsm.effect(Server.onStreamOpened as hsm.Operation)),
      hsm.transition(hsm.on(Server.streamAcceptedEvent), hsm.effect(Server.onStreamAccepted as hsm.Operation)),
      hsm.transition(
        hsm.on(Server.sendGoAwayEvent),
        hsm.target("/Server/draining"),
        hsm.effect(Server.onSendGoAway as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Server.remoteGoAwayEvent),
        hsm.target("/Server/draining"),
        hsm.effect(Server.onRemoteGoAway as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Server.closeEvent),
        hsm.target("/Server/closing"),
        hsm.effect(Server.onClose as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Server.transportClosedEvent),
        hsm.target("/Server/closed"),
        hsm.effect(Server.onTransportClosed as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Server.errorEvent),
        hsm.target("/Server/closing"),
        hsm.effect(Server.onError as hsm.Operation),
      ),
    ),
    hsm.state(
      "draining",
      hsm.transition(hsm.on(Server.frameEvent), hsm.effect(Server.onFrame as hsm.Operation)),
      hsm.transition(
        hsm.on(Server.closeEvent),
        hsm.target("/Server/closing"),
        hsm.effect(Server.onClose as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Server.transportClosedEvent),
        hsm.target("/Server/closed"),
        hsm.effect(Server.onTransportClosed as hsm.Operation),
      ),
      hsm.transition(
        hsm.on(Server.errorEvent),
        hsm.target("/Server/closing"),
        hsm.effect(Server.onError as hsm.Operation),
      ),
    ),
    hsm.state(
      "closing",
      hsm.transition(
        hsm.on(Server.transportClosedEvent),
        hsm.target("/Server/closed"),
        hsm.effect(Server.onTransportClosed as hsm.Operation),
      ),
    ),
    hsm.state("closed"),
    hsm.initial(hsm.target("/Server/idle")),
  ) as hsm.Model;

  constructor(options: ServerOptions) {
    super({ ...options, role: "server" });
  }
}

export function createServer(options: ServerOptions): Server {
  const server = hsm.start(new Server(options), Server.model) as unknown as Server;
  return server.activate();
}
