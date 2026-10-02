import { Code, ConnectError, createClient, type Client, type Interceptor } from "@connectrpc/connect";
import { createConnectTransport as createWebTransport } from "@connectrpc/connect-web";
import { MethodOptions_IdempotencyLevel } from "@bufbuild/protobuf/wkt";
import { HarnessAPIService } from "./gen/harnessapi/v1/harnessapi_pb.js";

export type HarnessClient = Client<typeof HarnessAPIService>;

export interface ClientOptions {
  baseUrl: string;
  useJson?: boolean;
  requestTimeoutMs?: number;
  retries?: number;
  interceptors?: Interceptor[];
}

const retryable: ReadonlySet<Code> = new Set([Code.Unavailable, Code.DeadlineExceeded, Code.Unknown]);

export function createWebClient(opts: ClientOptions): HarnessClient {
  return createClient(
    HarnessAPIService,
    createWebTransport({
      ...transportOptions(opts),
      fetch: (input, init) => fetch(input, { ...init, credentials: "include" }),
    }),
  );
}

export function transportOptions(opts: ClientOptions, innermost: Interceptor[] = []) {
  return {
    baseUrl: opts.baseUrl,
    useBinaryFormat: opts.useJson !== true,
    jsonOptions: { ignoreUnknownFields: true },
    interceptors: [retry(opts), ...(opts.interceptors ?? []), ...innermost],
  };
}

function retry(opts: ClientOptions): Interceptor {
  const timeoutMs = opts.requestTimeoutMs ?? 30_000;
  const retries = Math.max(0, opts.retries ?? 2);
  return (next) => async (req) => {
    if (req.stream) {
      return next(req);
    }
    const attempt = async () => {
      const deadline = new AbortController();
      const timer = setTimeout(
        () => deadline.abort(new ConnectError(`no response within ${timeoutMs}ms`, Code.DeadlineExceeded)),
        timeoutMs,
      );
      const callerMs = Number(req.header.get("Connect-Timeout-Ms"));
      req.header.set("Connect-Timeout-Ms", String(callerMs > 0 ? Math.min(callerMs, timeoutMs) : timeoutMs));
      try {
        return await next({ ...req, signal: AbortSignal.any([req.signal, deadline.signal]) });
      } finally {
        clearTimeout(timer);
      }
    };
    const keyed = (req.message as { idempotencyKey?: unknown }).idempotencyKey;
    const repeatable =
      req.method.idempotency !== MethodOptions_IdempotencyLevel.IDEMPOTENCY_UNKNOWN ||
      (typeof keyed === "string" && keyed !== "");
    for (let n = 0; n < (repeatable ? retries : 0); n++) {
      try {
        return await attempt();
      } catch (err) {
        if (!retryable.has(ConnectError.from(err).code)) {
          throw err;
        }
      }
      await backoff((n + 1) * 200, req.signal);
    }
    return attempt();
  };
}

function backoff(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const stop = () => {
      clearTimeout(timer);
      reject(ConnectError.from(signal.reason));
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", stop);
      resolve();
    }, ms);
    if (signal.aborted) {
      stop();
    } else {
      signal.addEventListener("abort", stop, { once: true });
    }
  });
}
