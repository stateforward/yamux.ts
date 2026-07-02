import { describe, expect, it } from "vitest";

import { ByteReader } from "../src/byte-reader.js";
import { chunksReadable, emptyReadable } from "./helpers.js";

describe("byte reader", () => {
  it("handles empty, split, partial, and impossible buffer states", async () => {
    await expect(new ByteReader(emptyReadable()).readExactly(0)).resolves.toEqual(new Uint8Array(0));
    await expect(new ByteReader(emptyReadable()).readExactly(1)).resolves.toBeNull();

    const split = new ByteReader(chunksReadable([new Uint8Array([1, 2]), new Uint8Array([3])]));
    await expect(split.readExactly(2)).resolves.toEqual(new Uint8Array([1, 2]));
    await expect(split.readExactly(1)).resolves.toEqual(new Uint8Array([3]));

    const withEmptyChunk = new ByteReader(chunksReadable([new Uint8Array(0), new Uint8Array([9])]));
    await expect(withEmptyChunk.readExactly(1)).resolves.toEqual(new Uint8Array([9]));

    await expect(new ByteReader(chunksReadable([new Uint8Array([1])])).readExactly(2)).rejects.toMatchObject({
      code: "INVALID_FRAME",
    });

    const underflow = new ByteReader(emptyReadable());
    (underflow as unknown as { buffered: number }).buffered = 1;
    await expect(underflow.readExactly(1)).rejects.toMatchObject({ code: "INTERNAL_ERROR" });
  });
});
