import {
  createHash,
  randomBytes,
  randomUUID,
  timingSafeEqual,
} from "node:crypto";
import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import type { McpServer, ServerContext } from "@modelcontextprotocol/server";
import { ProtocolError, ProtocolErrorCode } from "@modelcontextprotocol/server";
import { Effect, Result, Schema } from "effect";
import { getConfig } from "./config.ts";
import { userFromContext } from "./identity.ts";
import {
  JsonObjectSchema,
  type JsonObject,
  type JsonValue,
} from "./lib/json.ts";
import { toMcpSchema } from "./lib/mcp-schema.ts";
import { logger } from "./logger.ts";
import {
  isTerminal,
  taskStore,
  type TaskRecord,
  type TaskStore,
} from "./tasks.ts";
import {
  CallbackError,
  MAX_EVENT_BYTES,
  decodeSecret,
  safePost,
  validateCallbackUrl,
  webhookHeaders,
  type WebhookPost,
} from "./webhook.ts";

/**
 * MCP Events (the draft Triggers & Events extension), webhook delivery only,
 * as ChatGPT dots and Work chats implement it:
 *
 *   events/list         → the run events below
 *   events/subscribe    → verify the callback, store the subscription
 *   events/unsubscribe  → drop it
 *
 * Events come from the runs this process drives (the shared TaskStore), and
 * go only to subscriptions of the user who launched the run. They are not
 * replayable (`cursor: null`): a run that finishes while this process is down
 * is reported by get_run_status, not by an event.
 */

export const RUN_APPROVAL_REQUIRED = "run.approval_required";
export const RUN_FINISHED = "run.finished";

/** JSON-RPC error for a callback that failed verification. */
export const CALLBACK_ENDPOINT_ERROR = -32015;

const DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const MIN_TTL_MS = 5 * 60 * 1000;

/** How long a verified (user, callback URL) pair skips re-verification. */
const VERIFIED_TTL_MS = 24 * 60 * 60 * 1000;

/** How long a replaced secret still signs deliveries. */
const ROTATION_WINDOW_MS = 60 * 60 * 1000;

/** Longest answer sent inline; get_run_status has the full text. */
const MAX_ANSWER_CHARS = 32 * 1024;

const RETRY_DELAYS_MS = [1_000, 5_000, 30_000, 120_000];

const runIdFilter: JsonObject = {
  type: "object",
  properties: {
    run_id: {
      type: "string",
      description:
        "Only this run (the runId from launch_hello_demo). Omit to watch every run you launch.",
    },
  },
  additionalProperties: false,
};

const nullableString = { type: ["string", "null"] };

export const EVENT_DEFINITIONS: JsonObject[] = [
  {
    name: RUN_APPROVAL_REQUIRED,
    description:
      "An AgentOps agent run you launched is waiting for approval. The payload has " +
      "the Pomerium approval link: ask the user to open it, sign in, and approve.",
    delivery: ["webhook"],
    inputSchema: runIdFilter,
    payloadSchema: {
      type: "object",
      properties: {
        run_id: { type: "string" },
        session_id: nullableString,
        template: { type: "string" },
        approval_url: { type: "string" },
        expires_at: nullableString,
      },
      required: [
        "run_id",
        "session_id",
        "template",
        "approval_url",
        "expires_at",
      ],
      additionalProperties: false,
    },
  },
  {
    name: RUN_FINISHED,
    description:
      "An AgentOps agent run you launched finished: completed with the agent's " +
      "answer, failed with an error, or was cancelled.",
    delivery: ["webhook"],
    inputSchema: runIdFilter,
    payloadSchema: {
      type: "object",
      properties: {
        run_id: { type: "string" },
        session_id: nullableString,
        template: { type: "string" },
        status: { type: "string", enum: ["completed", "failed", "cancelled"] },
        answer: nullableString,
        answer_truncated: { type: "boolean" },
        error: nullableString,
      },
      required: [
        "run_id",
        "session_id",
        "template",
        "status",
        "answer",
        "answer_truncated",
        "error",
      ],
      additionalProperties: false,
    },
  },
];

const EVENT_NAMES: ReadonlySet<string> = new Set([
  RUN_APPROVAL_REQUIRED,
  RUN_FINISHED,
]);

const Subscription = Schema.Struct({
  id: Schema.String,
  owner: Schema.String,
  name: Schema.String,
  arguments: JsonObjectSchema,
  url: Schema.String,
  secret: Schema.String,
  previousSecret: Schema.optional(Schema.String),
  previousSecretUntil: Schema.optional(Schema.Number),
  /** Epoch ms; delivery stops after it. */
  expiresAt: Schema.Number,
  createdAt: Schema.String,
});

export type Subscription = typeof Subscription.Type;

const SubscriptionFile = Schema.Struct({
  subscriptions: Schema.Array(Subscription),
});

/** JSON with object keys sorted, so key order never splits an identity. */
export function canonicalJson(value: JsonValue): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  }

  if (Schema.is(JsonObjectSchema)(value)) {
    const object = value;

    return `{${Object.keys(object)
      .sort()
      .map(
        (key) => `${JSON.stringify(key)}:${canonicalJson(object[key] ?? null)}`,
      )
      .join(",")}}`;
  }

  return JSON.stringify(value);
}

/** Deterministic id from the principal, callback URL, event and arguments. */
export function subscriptionId(
  owner: string,
  url: string,
  name: string,
  args: JsonObject,
): string {
  const digest = createHash("sha256")
    .update(canonicalJson([owner, url, name, args]))
    .digest("base64url");

  return `sub_${digest.slice(0, 32)}`;
}

/**
 * Subscriptions, persisted to a JSON file (mode 0600: it holds signing
 * secrets) so they survive restarts. Without a file they live in memory.
 */
export class SubscriptionStore {
  #subscriptions = new Map<string, Subscription>();
  readonly #file: string | undefined;

  constructor(file?: string) {
    this.#file = file || undefined;
    this.#load();
  }

  get(id: string): Subscription | undefined {
    return this.#subscriptions.get(id);
  }

  /** Live subscriptions, dropping expired ones. */
  active(now: number): Subscription[] {
    let expired = false;

    for (const [id, sub] of this.#subscriptions) {
      if (sub.expiresAt <= now) {
        this.#subscriptions.delete(id);
        expired = true;
      }
    }

    if (expired) this.#save();

    return [...this.#subscriptions.values()];
  }

  put(sub: Subscription): void {
    this.#subscriptions.set(sub.id, sub);
    this.#save();
  }

  delete(id: string): boolean {
    const deleted = this.#subscriptions.delete(id);

    if (deleted) this.#save();

    return deleted;
  }

  #load(): void {
    if (!this.#file) return;
    let text: string;

    try {
      text = readFileSync(this.#file, "utf8");
    } catch {
      return;
    }

    const parsed = Schema.decodeUnknownResult(SubscriptionFile)(
      JSON.parse(text),
    );

    if (Result.isFailure(parsed)) {
      throw new Error(`Unreadable subscription store ${this.#file}`);
    }

    for (const sub of parsed.success.subscriptions) {
      this.#subscriptions.set(sub.id, sub);
    }
  }

  #save(): void {
    if (!this.#file) return;
    mkdirSync(dirname(this.#file), { recursive: true });
    const temp = `${this.#file}.${process.pid}.tmp`;

    writeFileSync(
      temp,
      JSON.stringify({ subscriptions: [...this.#subscriptions.values()] }),
      { mode: 0o600 },
    );
    renameSync(temp, this.#file);
  }
}

const Delivery = Schema.Struct({
  mode: Schema.String,
  url: Schema.String,
  secret: Schema.optional(Schema.String),
});

const SubscribeParams = Schema.Struct({
  name: Schema.String,
  arguments: Schema.optional(Schema.NullOr(JsonObjectSchema)),
  delivery: Delivery,
  cursor: Schema.optional(Schema.NullOr(Schema.String)),
  ttlMs: Schema.optional(Schema.NullOr(Schema.Number)),
});

export type SubscribeRequest = typeof SubscribeParams.Type;

const UnsubscribeParams = Schema.Struct({
  name: Schema.String,
  arguments: Schema.optional(Schema.NullOr(JsonObjectSchema)),
  delivery: Delivery,
});

export type UnsubscribeRequest = typeof UnsubscribeParams.Type;

const ListParams = Schema.Struct({
  cursor: Schema.optional(Schema.NullOr(Schema.String)),
});

const VerificationReply = Schema.Struct({ challenge: Schema.String });

function invalidParams(message: string): ProtocolError {
  return new ProtocolError(ProtocolErrorCode.InvalidParams, message);
}

function callbackError(error: CallbackError): ProtocolError {
  return new ProtocolError(CALLBACK_ENDPOINT_ERROR, error.message, {
    reason: error.reason,
  });
}

const RunFilterArguments = Schema.Struct({
  run_id: Schema.optional(Schema.NonEmptyString),
});

/** Subscription arguments: an object with at most a string `run_id`. */
function validateArguments(args: JsonObject | null | undefined): JsonObject {
  const parsed = Schema.decodeUnknownResult(RunFilterArguments)(args ?? {}, {
    onExcessProperty: "error",
  });

  if (Result.isFailure(parsed)) {
    throw invalidParams("arguments may only hold run_id, a non-empty string");
  }

  return parsed.success.run_id === undefined
    ? {}
    : { run_id: parsed.success.run_id };
}

function grantedTtl(ttlMs: number | null | undefined): number {
  // A request for no expiry still gets a finite one; the client refreshes.
  if (ttlMs === undefined) return DEFAULT_TTL_MS;

  if (ttlMs === null) return MAX_TTL_MS;

  return Math.min(MAX_TTL_MS, Math.max(MIN_TTL_MS, ttlMs));
}

type RunEvent = {
  /** Same across retries and subscribers. */
  eventId: string;
  name: string;
  runId: string;
  owner: string;
  timestamp: string;
  data: JsonObject;
};

export type EventHubOptions = {
  store: SubscriptionStore;
  post?: WebhookPost;
  template: () => string;
  now?: () => number;
  retryDelaysMs?: readonly number[];
};

/** Turns task changes into events and delivers them to matching subscribers. */
export class EventHub {
  readonly #store: SubscriptionStore;
  readonly #post: WebhookPost;
  readonly #template: () => string;
  readonly #now: () => number;
  readonly #retryDelaysMs: readonly number[];
  /** `${owner}\n${url}` → when its verification stops counting. */
  #verified = new Map<string, number>();
  /** runId → event keys already emitted, so each fires once. */
  #emitted = new Map<string, Set<string>>();
  #inFlight = new Set<Promise<void>>();

  constructor(options: EventHubOptions) {
    this.#store = options.store;
    this.#post = options.post ?? safePost;
    this.#template = options.template;
    this.#now = options.now ?? Date.now;
    this.#retryDelaysMs = options.retryDelaysMs ?? RETRY_DELAYS_MS;
  }

  /** Emits events for every change in `tasks`. Returns a detach function. */
  attach(tasks: TaskStore): () => void {
    return tasks.onUpdate((record) => this.#onTaskUpdate(record));
  }

  /**
   * Whether `owner` will hear about this run through events: an active
   * run.finished subscription that covers it. The launch result carries the
   * approval link, but only run.finished delivers the answer.
   */
  covers(owner: string | undefined, runId: string): boolean {
    if (!owner) return false;

    return this.#store
      .active(this.#now())
      .some(
        (sub) =>
          sub.owner === owner &&
          sub.name === RUN_FINISHED &&
          (sub.arguments.run_id === undefined ||
            sub.arguments.run_id === runId),
      );
  }

  /** Resolves once every delivery started so far has finished (tests). */
  async idle(): Promise<void> {
    while (this.#inFlight.size > 0) await Promise.all(this.#inFlight);
  }

  async subscribe(owner: string, params: SubscribeRequest) {
    if (!EVENT_NAMES.has(params.name)) {
      throw invalidParams(`Unknown event: ${params.name}`);
    }

    const args = validateArguments(params.arguments);

    if (params.delivery.mode !== "webhook") {
      throw invalidParams("Only webhook delivery is supported");
    }

    const secret = params.delivery.secret ?? "";

    if (!decodeSecret(secret)) {
      throw invalidParams(
        "delivery.secret must be whsec_ followed by base64 of 24 to 64 bytes",
      );
    }

    const url = params.delivery.url;

    try {
      validateCallbackUrl(url);
    } catch (error) {
      if (error instanceof CallbackError) throw callbackError(error);
      throw error;
    }

    const id = subscriptionId(owner, url, params.name, args);
    const now = this.#now();
    const existing = this.#store.get(id);
    await this.#verify(owner, url, secret, id);

    const rotated = existing && existing.secret !== secret;
    const expiresAt = now + grantedTtl(params.ttlMs);

    const sub: Subscription = {
      id,
      owner,
      name: params.name,
      arguments: args,
      url,
      secret,
      ...(rotated
        ? {
            previousSecret: existing.secret,
            previousSecretUntil: now + ROTATION_WINDOW_MS,
          }
        : existing?.previousSecret !== undefined
          ? {
              previousSecret: existing.previousSecret,
              previousSecretUntil: existing.previousSecretUntil,
            }
          : {}),
      expiresAt,
      createdAt: existing?.createdAt ?? new Date(now).toISOString(),
    };

    this.#store.put(sub);

    return {
      id,
      refreshBefore: new Date(expiresAt).toISOString(),
      cursor: null,
      truncated: false,
      refreshed: existing !== undefined,
    };
  }

  unsubscribe(owner: string, params: UnsubscribeRequest): boolean {
    const args = validateArguments(params.arguments);

    return this.#store.delete(
      subscriptionId(owner, params.delivery.url, params.name, args),
    );
  }

  /** Sends a signed, single-use challenge; the callback must echo it. */
  async #verify(
    owner: string,
    url: string,
    secret: string,
    id: string,
  ): Promise<void> {
    const key = `${owner}\n${url}`;
    const now = this.#now();

    if ((this.#verified.get(key) ?? 0) > now) return;

    const challenge = randomBytes(32).toString("base64url");
    const body = JSON.stringify({ type: "verification", challenge });

    const headers = webhookHeaders(
      [secret],
      `msg_verification_${randomUUID()}`,
      id,
      body,
    );

    let reply;

    try {
      reply = await this.#post(url, headers, body);
    } catch (error) {
      throw error instanceof CallbackError
        ? callbackError(error)
        : callbackError(new CallbackError("unreachable", String(error)));
    }

    if (reply.status < 200 || reply.status > 299) {
      throw callbackError(
        new CallbackError(
          "http_error",
          `The callback answered verification with HTTP ${reply.status}`,
        ),
      );
    }

    if (!challengeMatches(reply.body, challenge)) {
      throw callbackError(
        new CallbackError(
          "challenge_failed",
          "The callback did not echo the verification challenge",
        ),
      );
    }

    // Bounded: drop the oldest entries past 1000 pairs.
    this.#verified.delete(key);
    this.#verified.set(key, now + VERIFIED_TTL_MS);

    for (const old of this.#verified.keys()) {
      if (this.#verified.size <= 1000) break;
      this.#verified.delete(old);
    }
  }

  #onTaskUpdate(record: TaskRecord): void {
    const event = this.#eventFor(record);

    if (!event) return;

    const now = this.#now();

    const targets = this.#store
      .active(now)
      .filter(
        (sub) =>
          sub.owner === event.owner &&
          sub.name === event.name &&
          (sub.arguments.run_id === undefined ||
            sub.arguments.run_id === event.runId),
      );

    for (const sub of targets) {
      const delivery = this.#deliver(sub.id, event).finally(() =>
        this.#inFlight.delete(delivery),
      );

      this.#inFlight.add(delivery);
    }
  }

  /** The event a task change should emit, once per run and kind. */
  #eventFor(record: TaskRecord): RunEvent | undefined {
    if (!record.owner) return undefined;

    let key: string;
    let name: string;
    let data: JsonObject;

    const base = {
      run_id: record.taskId,
      session_id: record.sessionId ?? null,
      template: this.#template(),
    };

    if (record.status === "input_required" && record.approvalUrl) {
      key = `approval:${record.approvalUrl}`;
      name = RUN_APPROVAL_REQUIRED;
      data = {
        ...base,
        approval_url: record.approvalUrl,
        expires_at: record.approvalExpiresAt ?? null,
      };
    } else if (isTerminal(record.status)) {
      key = "finished";
      name = RUN_FINISHED;
      data = { ...base, ...finishedFields(record) };
    } else {
      return undefined;
    }

    const seen = this.#emitted.get(record.taskId) ?? new Set<string>();

    if (seen.has(key)) return undefined;
    seen.add(key);
    this.#emitted.set(record.taskId, seen);

    if (key === "finished") this.#emitted.delete(record.taskId);

    for (const old of this.#emitted.keys()) {
      if (this.#emitted.size <= 1000) break;
      this.#emitted.delete(old);
    }

    const eventId = `evt_${createHash("sha256")
      .update(`${record.taskId}\n${key}`)
      .digest("base64url")
      .slice(0, 32)}`;

    return {
      eventId,
      name,
      runId: record.taskId,
      owner: record.owner,
      timestamp: record.lastUpdatedAt,
      data,
    };
  }

  /** One event to one subscription, with bounded retries. */
  async #deliver(subscriptionIdValue: string, event: RunEvent): Promise<void> {
    const body = JSON.stringify({
      eventId: event.eventId,
      name: event.name,
      timestamp: event.timestamp,
      data: event.data,
      cursor: null,
    });

    const fields = {
      subscriptionId: subscriptionIdValue,
      eventId: event.eventId,
      event: event.name,
      runId: event.runId,
    };

    if (Buffer.byteLength(body, "utf8") > MAX_EVENT_BYTES) {
      await Effect.runPromise(
        logger.error(fields, "Event exceeds 256 KiB; not delivered"),
      );

      return;
    }

    for (let attempt = 0; attempt <= this.#retryDelaysMs.length; attempt++) {
      if (attempt > 0) {
        await sleep(this.#retryDelaysMs[attempt - 1] ?? 0);
      }

      // Re-read every attempt: an unsubscribe or expiry stops retries.
      const sub = this.#store.get(subscriptionIdValue);

      if (!sub || sub.expiresAt <= this.#now()) return;

      const secrets = [sub.secret];

      if (sub.previousSecret && (sub.previousSecretUntil ?? 0) > this.#now()) {
        secrets.push(sub.previousSecret);
      }

      let status = 0;
      let failure = "";

      try {
        const reply = await this.#post(
          sub.url,
          webhookHeaders(secrets, event.eventId, sub.id, body),
          body,
        );

        status = reply.status;
      } catch (error) {
        failure = error instanceof Error ? error.message : String(error);
      }

      if (status >= 200 && status <= 299) {
        await Effect.runPromise(
          logger.info({ ...fields, status, attempt }, "Event delivered"),
        );

        return;
      }

      // 410: the receiver dropped the subscription. Other 4xx (but 408/429)
      // will fail the same way again.
      const permanent =
        status === 410 ||
        status === 413 ||
        (status >= 400 && status < 500 && status !== 408 && status !== 429);

      await Effect.runPromise(
        logger.warn(
          { ...fields, status, attempt, error: failure || null, permanent },
          "Event delivery failed",
        ),
      );

      if (status === 410) {
        this.#store.delete(sub.id);

        return;
      }

      if (permanent) return;
    }
  }
}

/** The structuredContent of a run's result (see demo-run.ts). */
const RunResultContent = Schema.Struct({
  answer: Schema.optional(Schema.String),
  error: Schema.optional(Schema.String),
});

function finishedFields(record: TaskRecord): JsonObject {
  if (record.status !== "completed") {
    return {
      status: "cancelled",
      answer: null,
      answer_truncated: false,
      error: record.statusMessage ?? null,
    };
  }

  const parsed = Schema.decodeUnknownResult(RunResultContent)(
    record.result?.structuredContent ?? {},
  );

  const content = Result.isSuccess(parsed) ? parsed.success : {};

  if (record.result?.isError === true) {
    return {
      status: "failed",
      answer: null,
      answer_truncated: false,
      error: content.error ?? "The run failed",
    };
  }

  const text = content.answer ?? "";

  return {
    status: "completed",
    answer: text.slice(0, MAX_ANSWER_CHARS),
    answer_truncated: text.length > MAX_ANSWER_CHARS,
    error: null,
  };
}

function challengeMatches(body: string, challenge: string): boolean {
  let parsed;

  try {
    parsed = Schema.decodeUnknownResult(VerificationReply)(JSON.parse(body));
  } catch {
    return false;
  }

  if (Result.isFailure(parsed)) return false;
  const given = Buffer.from(parsed.success.challenge);
  const expected = Buffer.from(challenge);

  return given.length === expected.length && timingSafeEqual(given, expected);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let defaultHub: EventHub | undefined;

/** The process-wide hub, attached to the shared task store. */
export function getEventHub(): EventHub {
  if (!defaultHub) {
    defaultHub = new EventHub({
      store: new SubscriptionStore(getConfig().EVENTS_STORE_FILE),
      template: () => getConfig().DEMO_TEMPLATE,
    });
    defaultHub.attach(taskStore);
  }

  return defaultHub;
}

function requireOwner(ctx: ServerContext): string {
  const owner = userFromContext(ctx);

  if (!owner) {
    throw new ProtocolError(
      ProtocolErrorCode.InvalidRequest,
      "Events need a signed-in user (no Pomerium identity on this request)",
    );
  }

  return owner;
}

/** Advertises `events` and serves `events/list|subscribe|unsubscribe`. */
export function registerEvents(
  server: McpServer,
  hub: EventHub = getEventHub(),
): void {
  // SAFETY: `events` is the draft extension's top-level capability; the
  // SDK's ServerCapabilities type predates it, and discover passes it as-is.
  server.server.registerCapabilities({ events: {} } as never);

  server.server.setRequestHandler(
    "events/list",
    { params: toMcpSchema(ListParams) },
    async () => ({ resultType: "complete", events: EVENT_DEFINITIONS }),
  );

  server.server.setRequestHandler(
    "events/subscribe",
    { params: toMcpSchema(SubscribeParams) },
    async (params, ctx) => {
      const owner = requireOwner(ctx);

      const fields = {
        requestId: ctx.mcpReq.id,
        event: params.name,
        callbackHost: hostOf(params.delivery.url),
        filtered: params.arguments?.run_id !== undefined,
      };

      try {
        const { refreshed, ...result } = await hub.subscribe(owner, params);
        await Effect.runPromise(
          logger.info(
            { ...fields, subscriptionId: result.id, refreshed },
            "Event subscription stored",
          ),
        );

        return { resultType: "complete", ...result };
      } catch (error) {
        await Effect.runPromise(
          logger.warn(
            {
              ...fields,
              error: error instanceof Error ? error.message : String(error),
            },
            "Event subscription refused",
          ),
        );
        throw error;
      }
    },
  );

  server.server.setRequestHandler(
    "events/unsubscribe",
    { params: toMcpSchema(UnsubscribeParams) },
    async (params, ctx) => {
      const owner = requireOwner(ctx);
      const removed = hub.unsubscribe(owner, params);

      await Effect.runPromise(
        logger.info(
          {
            requestId: ctx.mcpReq.id,
            event: params.name,
            callbackHost: hostOf(params.delivery.url),
            removed,
          },
          "Event subscription removed",
        ),
      );

      return { resultType: "complete" };
    },
  );
}

/** The callback's host, for logs (never the path: it can carry a token). */
function hostOf(url: string): string | null {
  try {
    return new URL(url).host;
  } catch {
    return null;
  }
}
