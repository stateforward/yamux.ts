export const PROTOCOL_VERSION = 0;
export const HEADER_SIZE = 12;
export const INITIAL_STREAM_WINDOW = 256 * 1024;
export const MAX_UINT32 = 0xffff_ffff;

export enum YamuxFrameType {
  Data = 0x0,
  WindowUpdate = 0x1,
  Ping = 0x2,
  GoAway = 0x3,
}

export enum YamuxFlag {
  SYN = 0x1,
  ACK = 0x2,
  FIN = 0x4,
  RST = 0x8,
}

export enum YamuxGoAwayCode {
  Normal = 0x0,
  ProtocolError = 0x1,
  InternalError = 0x2,
}

export type YamuxHeader = Readonly<{
  version: number;
  type: YamuxFrameType;
  flags: number;
  streamID: number;
  length: number;
}>;

export type YamuxFrame = Readonly<{
  header: YamuxHeader;
  payload: Uint8Array;
}>;

export function hasFlag(flags: number, flag: YamuxFlag): boolean {
  return (flags & flag) === flag;
}

export function encodeHeader(
  type: YamuxFrameType,
  flags: number,
  streamID: number,
  length: number,
): Uint8Array {
  if (!isFrameType(type)) {
    throw new RangeError(`invalid yamux frame type: ${type}`);
  }
  assertUint32(streamID, "streamID");
  assertUint32(length, "length");
  assertFlags(flags);

  const bytes = new Uint8Array(HEADER_SIZE);
  writeHeader(bytes, type, flags, streamID, length);
  return bytes;
}

export function decodeHeader(bytes: Uint8Array): YamuxHeader {
  if (bytes.byteLength !== HEADER_SIZE) {
    throw new RangeError(`yamux header must be ${HEADER_SIZE} bytes`);
  }

  const version = bytes[0]!;
  const type = bytes[1]!;
  const flags = bytes[2]! * 0x100 + bytes[3]!;
  const streamID = readUint32(bytes, 4);
  const length = readUint32(bytes, 8);

  if (!isFrameType(type)) {
    throw new RangeError(`invalid yamux frame type: ${type}`);
  }
  assertFlags(flags);

  return {
    version,
    type,
    flags,
    streamID,
    length,
  };
}

export function frameToBytes(
  type: YamuxFrameType,
  flags: number,
  streamID: number,
  length: number,
  payload?: Uint8Array,
): Uint8Array {
  if (!isFrameType(type)) {
    throw new RangeError(`invalid yamux frame type: ${type}`);
  }
  assertUint32(streamID, "streamID");
  assertUint32(length, "length");
  assertFlags(flags);

  if (type === YamuxFrameType.Data && (payload?.byteLength ?? 0) !== length) {
    throw new RangeError("data frame payload length must match header length");
  }
  if (type !== YamuxFrameType.Data && payload && payload.byteLength > 0) {
    throw new RangeError("only data frames may include payload bytes");
  }

  const payloadLength = payload?.byteLength ?? 0;
  const bytes = new Uint8Array(HEADER_SIZE + payloadLength);
  writeHeader(bytes, type, flags, streamID, length);
  if (payloadLength > 0) {
    bytes.set(payload!, HEADER_SIZE);
  }
  return bytes;
}

export function isFrameType(type: number): type is YamuxFrameType {
  return (
    type === YamuxFrameType.Data ||
    type === YamuxFrameType.WindowUpdate ||
    type === YamuxFrameType.Ping ||
    type === YamuxFrameType.GoAway
  );
}

function assertUint32(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 0 || value > MAX_UINT32) {
    throw new RangeError(`${name} must be an unsigned 32-bit integer`);
  }
}

function assertFlags(flags: number): void {
  if (!Number.isInteger(flags) || flags < 0 || flags > 0xf) {
    throw new RangeError("yamux flags must use only SYN, ACK, FIN, and RST");
  }
}

function writeHeader(bytes: Uint8Array, type: YamuxFrameType, flags: number, streamID: number, length: number): void {
  bytes[0] = PROTOCOL_VERSION;
  bytes[1] = type;
  bytes[2] = flags >>> 8;
  bytes[3] = flags;
  bytes[4] = streamID >>> 24;
  bytes[5] = streamID >>> 16;
  bytes[6] = streamID >>> 8;
  bytes[7] = streamID;
  bytes[8] = length >>> 24;
  bytes[9] = length >>> 16;
  bytes[10] = length >>> 8;
  bytes[11] = length;
}

function readUint32(bytes: Uint8Array, offset: number): number {
  return (
    bytes[offset]! * 0x1_00_00_00 +
    bytes[offset + 1]! * 0x1_00_00 +
    bytes[offset + 2]! * 0x100 +
    bytes[offset + 3]!
  );
}
