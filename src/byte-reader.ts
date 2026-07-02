import { YamuxError } from "./errors.js";

export class ByteReader {
  private readonly reader: ReadableStreamDefaultReader<Uint8Array>;
  private readonly chunks: Uint8Array[] = [];
  private buffered = 0;
  private done = false;

  constructor(readable: ReadableStream<Uint8Array>) {
    this.reader = readable.getReader();
  }

  async readExactly(length: number): Promise<Uint8Array | null> {
    if (length === 0) {
      return new Uint8Array(0);
    }

    while (this.buffered < length && !this.done) {
      const result = await this.reader.read();
      if (result.done) {
        this.done = true;
        break;
      }
      if (result.value.byteLength > 0) {
        this.chunks.push(result.value);
        this.buffered += result.value.byteLength;
      }
    }

    if (this.buffered === 0 && this.done) {
      return null;
    }
    if (this.buffered < length) {
      throw new YamuxError("INVALID_FRAME", "transport ended in the middle of a yamux frame");
    }

    const output = new Uint8Array(length);
    let written = 0;

    while (written < length) {
      const chunk = this.chunks[0];
      if (!chunk) {
        throw new YamuxError("INTERNAL_ERROR", "byte reader underflow");
      }

      const remaining = length - written;
      if (chunk.byteLength <= remaining) {
        output.set(chunk, written);
        written += chunk.byteLength;
        this.chunks.shift();
        this.buffered -= chunk.byteLength;
      } else {
        output.set(chunk.subarray(0, remaining), written);
        this.chunks[0] = chunk.subarray(remaining);
        written += remaining;
        this.buffered -= remaining;
      }
    }

    return output;
  }

  releaseLock(): void {
    this.reader.releaseLock();
  }
}
