export type YamuxErrorCode =
  | "ABORTED"
  | "CONNECTION_RESET"
  | "DUPLICATE_STREAM"
  | "GO_AWAY"
  | "INTERNAL_ERROR"
  | "INVALID_FRAME"
  | "INVALID_STREAM"
  | "PROTOCOL_ERROR"
  | "RECEIVE_WINDOW_EXCEEDED"
  | "SESSION_CLOSED"
  | "STREAM_CLOSED"
  | "STREAMS_EXHAUSTED"
  | "TIMEOUT";

export class YamuxError extends Error {
  readonly code: YamuxErrorCode;

  constructor(code: YamuxErrorCode, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "YamuxError";
    this.code = code;
  }
}

export function abortedError(): YamuxError {
  return new YamuxError("ABORTED", "operation aborted");
}

export function timeoutError(): YamuxError {
  return new YamuxError("TIMEOUT", "operation timed out");
}
