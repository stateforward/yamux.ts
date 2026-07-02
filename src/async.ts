import { abortedError, timeoutError, YamuxError } from "./errors.js";

export class Deferred<T> {
  readonly promise: Promise<T>;
  resolve!: (value: T | PromiseLike<T>) => void;
  reject!: (reason?: unknown) => void;

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
  }
}

type Waiter<T> = {
  resolve: (value: T) => void;
  reject: (reason?: unknown) => void;
  cleanup: () => void;
};

export class AsyncQueue<T> {
  private readonly items: T[] = [];
  private readonly waiters: Array<Waiter<T>> = [];
  private closed: unknown | undefined;

  get length(): number {
    return this.items.length;
  }

  push(item: T): void {
    if (this.closed !== undefined) {
      throw this.closed;
    }

    const waiter = this.waiters.shift();
    if (waiter) {
      waiter.cleanup();
      waiter.resolve(item);
      return;
    }

    this.items.push(item);
  }

  async shift(signal?: AbortSignal): Promise<T> {
    const item = this.items.shift();
    if (item !== undefined) {
      return item;
    }

    if (this.closed !== undefined) {
      throw this.closed;
    }
    if (signal?.aborted) {
      throw abortedError();
    }

    return new Promise<T>((resolve, reject) => {
      const onAbort = (): void => {
        this.waiters.splice(this.waiters.indexOf(waiter), 1);
        reject(abortedError());
      };
      const cleanup = (): void => {
        signal?.removeEventListener("abort", onAbort);
      };
      const waiter: Waiter<T> = { resolve, reject, cleanup };

      signal?.addEventListener("abort", onAbort, { once: true });
      this.waiters.push(waiter);
    });
  }

  close(reason: unknown = new YamuxError("SESSION_CLOSED", "queue closed")): void {
    if (this.closed !== undefined) {
      return;
    }

    this.closed = reason;
    for (const waiter of this.waiters.splice(0)) {
      waiter.cleanup();
      waiter.reject(reason);
    }
  }
}

export async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs?: number,
  signal?: AbortSignal,
): Promise<T> {
  if (signal?.aborted) {
    throw abortedError();
  }
  if (timeoutMs === undefined) {
    return signal ? raceAbort(promise, signal) : promise;
  }
  if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
    throw new RangeError("timeoutMs must be a non-negative finite number");
  }

  let timeoutID: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timeoutID = setTimeout(() => reject(timeoutError()), timeoutMs);
  });

  try {
    const raced = Promise.race([promise, timeout]);
    return await (signal ? raceAbort(raced, signal) : raced);
  } finally {
    clearTimeout(timeoutID);
  }
}

function raceAbort<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(abortedError());
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (reason) => {
        signal.removeEventListener("abort", onAbort);
        reject(reason);
      },
    );
  });
}
