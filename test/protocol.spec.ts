import { describe, expect, it } from "vitest";

import {
  HEADER_SIZE,
  MAX_UINT32,
  YamuxFlag,
  YamuxFrameType,
  decodeHeader,
  encodeHeader,
  frameToBytes,
  isFrameType,
} from "../src/protocol.js";

describe("yamux protocol", () => {
  it("encodes and decodes 12-byte big-endian headers", () => {
    const header = encodeHeader(YamuxFrameType.WindowUpdate, YamuxFlag.SYN | YamuxFlag.ACK, 17, 42);

    expect(header).toHaveLength(HEADER_SIZE);
    expect([...header]).toEqual([0, 1, 0, 3, 0, 0, 0, 17, 0, 0, 0, 42]);
    expect(decodeHeader(header)).toEqual({
      version: 0,
      type: YamuxFrameType.WindowUpdate,
      flags: YamuxFlag.SYN | YamuxFlag.ACK,
      streamID: 17,
      length: 42,
    });
  });

  it("rejects invalid headers and frames", () => {
    expect(() => decodeHeader(new Uint8Array(HEADER_SIZE - 1))).toThrow(RangeError);

    const invalidType = encodeHeader(YamuxFrameType.Data, 0, 0, 0);
    invalidType[1] = 99;
    expect(() => decodeHeader(invalidType)).toThrow(RangeError);

    expect(() => encodeHeader(99 as YamuxFrameType, 0, 0, 0)).toThrow(RangeError);
    expect(() => encodeHeader(YamuxFrameType.Data, 0x10, 0, 0)).toThrow(RangeError);
    expect(() => encodeHeader(YamuxFrameType.Data, 0, -1, 0)).toThrow(RangeError);
    expect(() => encodeHeader(YamuxFrameType.Data, 0, 0, MAX_UINT32 + 1)).toThrow(RangeError);
    expect(() => frameToBytes(99 as YamuxFrameType, 0, 0, 0)).toThrow(RangeError);
    expect(() => frameToBytes(YamuxFrameType.Data, 0, 1, 2, new Uint8Array([1]))).toThrow(RangeError);
    expect(frameToBytes(YamuxFrameType.Data, 0, 1, 0)).toHaveLength(HEADER_SIZE);
    expect(() => frameToBytes(YamuxFrameType.Ping, 0, 0, 0, new Uint8Array([1]))).toThrow(RangeError);
    expect(frameToBytes(YamuxFrameType.WindowUpdate, 0, 1, 0)).toHaveLength(HEADER_SIZE);
  });

  it("identifies supported frame types", () => {
    expect(isFrameType(YamuxFrameType.Data)).toBe(true);
    expect(isFrameType(YamuxFrameType.WindowUpdate)).toBe(true);
    expect(isFrameType(YamuxFrameType.Ping)).toBe(true);
    expect(isFrameType(YamuxFrameType.GoAway)).toBe(true);
    expect(isFrameType(99)).toBe(false);
  });
});
