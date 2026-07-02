import { describe, expect, it } from "vitest";

import { AsyncQueue, withTimeout } from "../src/async.js";
import { YamuxError } from "../src/errors.js";

describe("async helpers", () => {
  it("resolves queued and waiting items", async () => {
    const queue = new AsyncQueue<number>();
    queue.push(1);
    await expect(queue.shift()).resolves.toBe(1);

    const waiting = queue.shift();
    queue.push(2);
    await expect(waiting).resolves.toBe(2);
  });

  it("rejects aborted and closed waits", async () => {
    const queue = new AsyncQueue<number>();
    const controller = new AbortController();
    controller.abort();
    await expect(queue.shift(controller.signal)).rejects.toMatchObject({ code: "ABORTED" });

    const waiting = queue.shift();
    const reason = new YamuxError("SESSION_CLOSED", "done");
    queue.close(reason);
    queue.close(new Error("ignored"));
    await expect(waiting).rejects.toBe(reason);
    expect(() => queue.push(1)).toThrow(reason);
    await expect(queue.shift()).rejects.toBe(reason);
  });

  it("handles timeouts, aborts, success, and rejection", async () => {
    await expect(withTimeout(Promise.resolve(1))).resolves.toBe(1);
    await expect(withTimeout(Promise.resolve(2), undefined, new AbortController().signal)).resolves.toBe(2);
    await expect(withTimeout(Promise.resolve(3), -1)).rejects.toThrow(RangeError);
    await expect(withTimeout(new Promise(() => undefined), 0)).rejects.toMatchObject({ code: "TIMEOUT" });
    await expect(withTimeout(Promise.resolve(5), 10)).resolves.toBe(5);
    await expect(withTimeout(Promise.resolve(6), 10, new AbortController().signal)).resolves.toBe(6);

    const preAborted = new AbortController();
    preAborted.abort();
    await expect(withTimeout(Promise.resolve(4), undefined, preAborted.signal)).rejects.toMatchObject({
      code: "ABORTED",
    });

    const aborting = new AbortController();
    const aborted = withTimeout(new Promise(() => undefined), undefined, aborting.signal);
    aborting.abort();
    await expect(aborted).rejects.toMatchObject({ code: "ABORTED" });

    await expect(withTimeout(Promise.reject(new Error("boom")), undefined, new AbortController().signal)).rejects.toThrow(
      "boom",
    );
  });
});
