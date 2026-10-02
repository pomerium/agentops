import { Code, ConnectError } from "@connectrpc/connect";
import type { MessageInitShape } from "@bufbuild/protobuf";
import pRetry from "p-retry";
import {
  Sentinel,
  type Event,
  type SubscribeRequestSchema,
  type SubscribeResponse,
} from "./gen/harnessapi/v1/harnessapi_pb.js";
import type { HarnessClient } from "./client.js";
import { sentinelOf } from "./errors.js";

export interface SubscribeOptions {
  keepaliveIntervalMs?: number;
  missedKeepalives?: number;
  reconnectBackoffMs?: number;
}

export interface EventFeed extends AsyncIterable<Event> {
  close(): void;
}

interface Stream {
  first: SubscribeResponse;
  read: () => Promise<IteratorResult<SubscribeResponse>>;
  controller: AbortController;
}

const finalSentinels: ReadonlySet<Sentinel> = new Set([
  Sentinel.NOT_FOUND,
  Sentinel.FORBIDDEN,
  Sentinel.INVALID_ARGUMENT,
]);

export async function subscribe(
  client: HarnessClient,
  request: MessageInitShape<typeof SubscribeRequestSchema>,
  options: SubscribeOptions = {},
): Promise<EventFeed> {
  const silenceMs = (options.keepaliveIntervalMs ?? 20_000) * (options.missedKeepalives ?? 3);
  const backoffMs = options.reconnectBackoffMs ?? 2_000;
  const closed = new AbortController();
  let last = request.afterSeq ?? 0n;

  const open = async (): Promise<Stream | undefined> => {
    const controller = new AbortController();
    const signal = AbortSignal.any([closed.signal, controller.signal]);
    const iterator = client.subscribe({ ...request, afterSeq: last }, { signal })[Symbol.asyncIterator]();
    const read = () => withDeadline(iterator.next(), silenceMs);
    try {
      const first = await read();
      if (first.done === true) {
        controller.abort();
        return undefined;
      }
      return { first: first.value, read, controller };
    } catch (err) {
      controller.abort();
      throw ConnectError.from(err);
    }
  };

  const reopen = (dropped: Error): Promise<Stream | undefined> =>
    pRetry(
      async (attempt) => {
        if (attempt === 1) {
          throw dropped;
        }
        return open();
      },
      {
        retries: Number.POSITIVE_INFINITY,
        minTimeout: backoffMs,
        maxTimeout: 30_000,
        factor: 2,
        randomize: true,
        signal: closed.signal,
        shouldRetry: ({ error }) => {
          const sentinel = sentinelOf(error);
          return sentinel === undefined || !finalSentinels.has(sentinel);
        },
      },
    ).catch(() => undefined);

  async function* events(stream: Stream): AsyncGenerator<Event, boolean> {
    let msg = stream.first;
    try {
      for (;;) {
        const ev = msg.event;
        if (!msg.keepalive && ev !== undefined && ev.seq > last) {
          last = ev.seq;
          yield ev;
          if (ev.payload.case === "sessionEnded") {
            return true;
          }
        }
        const step = await stream.read();
        if (step.done === true) {
          return true;
        }
        msg = step.value;
      }
    } catch {
      return false;
    } finally {
      stream.controller.abort();
    }
  }

  async function* run(stream: Stream | undefined): AsyncGenerator<Event> {
    try {
      while (stream !== undefined) {
        if (yield* events(stream)) {
          return;
        }
        stream = await reopen(new ConnectError("the subscription stream dropped", Code.Unavailable));
      }
    } finally {
      closed.abort();
    }
  }

  const feed = run(await open());
  return {
    [Symbol.asyncIterator]: () => feed,
    close: () => {
      closed.abort();
      void feed.return(undefined);
    },
  };
}

async function withDeadline<T>(read: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const silence = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new ConnectError("no events or keepalives within the liveness window", Code.DeadlineExceeded)),
      ms,
    );
  });
  try {
    return await Promise.race([read, silence]);
  } finally {
    clearTimeout(timer);
    void read.catch(() => undefined);
  }
}
