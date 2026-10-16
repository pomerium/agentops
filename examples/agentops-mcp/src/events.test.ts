import { createHmac } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { McpServer, createMcpHandler } from "@modelcontextprotocol/server";
import { Schema } from "effect";
import {
  CALLBACK_ENDPOINT_ERROR,
  EventHub,
  RUN_APPROVAL_REQUIRED,
  RUN_FINISHED,
  SubscriptionStore,
  canonicalJson,
  registerEvents,
  subscriptionId,
} from "./events.ts";
import type { JsonObject, JsonValue } from "./lib/json.ts";
import { createErrorResult, createTextResult } from "./lib/utils.ts";
import { TaskStore } from "./tasks.ts";
import {
  CallbackError,
  isPublicAddress,
  signWebhook,
  validateCallbackUrl,
  type WebhookHeaders,
  type WebhookResponse,
} from "./webhook.ts";

const PROTOCOL = "2026-07-28";
const CALLBACK = "https://receiver.example.com/mcp-events/callback_123";
const SECRET = `whsec_${Buffer.alloc(32, 7).toString("base64")}`;
const OTHER_SECRET = `whsec_${Buffer.alloc(32, 9).toString("base64")}`;
const APPROVAL_URL = "https://agentic.example.com/approve?run_id=r1";

type RpcReply = {
  result?: JsonObject;
  error?: { code: number; message: string; data?: JsonObject };
};

type Sent = { url: string; headers: WebhookHeaders; body: JsonObject };

const VerificationBody = Schema.Struct({
  type: Schema.Literal("verification"),
  challenge: Schema.String,
});

/**
 * A fake callback receiver: answers verification by echoing the challenge
 * (or `verifyReply`), and event deliveries with `statuses` in turn (then 200).
 */
function fakeReceiver(
  options: { statuses?: number[]; verifyReply?: (c: string) => string } = {},
) {
  const sent: Sent[] = [];
  const statuses = [...(options.statuses ?? [])];

  const post = async (
    url: string,
    headers: WebhookHeaders,
    body: string,
  ): Promise<WebhookResponse> => {
    // SAFETY: every body this server sends is a JSON object.
    const parsed = JSON.parse(body) as JsonObject;
    sent.push({ url, headers, body: parsed });

    // The signature covers id, timestamp and the exact bytes sent.
    const given = headers["webhook-signature"].split(" ");

    const valid = [SECRET, OTHER_SECRET].map((secret) =>
      signWebhook(
        [secret],
        headers["webhook-id"],
        Number(headers["webhook-timestamp"]),
        body,
      ),
    );

    expect(given.some((signature) => valid.includes(signature))).toBe(true);

    if (Schema.is(VerificationBody)(parsed)) {
      const challenge = parsed.challenge;

      return {
        status: 200,
        body: JSON.stringify({
          challenge: options.verifyReply?.(challenge) ?? challenge,
        }),
      };
    }

    return { status: statuses.shift() ?? 200, body: "" };
  };

  return {
    post,
    sent,
    deliveries: () => sent.filter((s) => s.body.eventId !== undefined),
  };
}

function base64url(value: JsonValue): string {
  return Buffer.from(JSON.stringify(value)).toString("base64url");
}

function assertionFor(email: string): string {
  return `${base64url({ alg: "none" })}.${base64url({ email })}.sig`;
}

const cleanups: Array<() => Promise<void> | void> = [];

afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()?.();
});

function setup(receiver = fakeReceiver(), store = new SubscriptionStore()) {
  const tasks = new TaskStore();

  const hub = new EventHub({
    store,
    post: receiver.post,
    template: () => "hello",
    retryDelaysMs: [1, 1, 1],
  });

  cleanups.push(hub.attach(tasks));

  const handler = createMcpHandler(() => {
    const server = new McpServer({ name: "test-server", version: "0.0.0" });
    registerEvents(server, hub);

    return server;
  });

  cleanups.push(() => handler.close());

  let nextId = 0;

  const rpc = async (
    method: string,
    params: JsonObject,
    user: string | undefined = "alice@example.com",
  ): Promise<RpcReply> => {
    const headers = new Headers({
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
      "mcp-protocol-version": PROTOCOL,
      "mcp-method": method,
    });

    if (user) headers.set("x-pomerium-jwt-assertion", assertionFor(user));

    const response = await handler.fetch(
      new Request("http://localhost/mcp", {
        method: "POST",
        headers,
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: ++nextId,
          method,
          params: {
            ...params,
            _meta: {
              "io.modelcontextprotocol/protocolVersion": PROTOCOL,
              "io.modelcontextprotocol/clientInfo": {
                name: "openai-mcp",
                version: "1.0.0",
              },
              "io.modelcontextprotocol/clientCapabilities": {},
            },
          },
        }),
      }),
    );

    // SAFETY: the handler answers every request with a JSON-RPC envelope.
    return (await response.json()) as RpcReply;
  };

  const subscribe = (
    name: string,
    args: JsonObject = {},
    user?: string,
    extra: JsonObject = {},
  ) =>
    rpc(
      "events/subscribe",
      {
        name,
        arguments: args,
        delivery: { mode: "webhook", url: CALLBACK, secret: SECRET },
        cursor: null,
        ...extra,
      },
      user,
    );

  return { tasks, hub, rpc, subscribe, receiver };
}

/** Drives a task to input_required with an approval link, then completed. */
function runToAnswer(tasks: TaskStore, owner: string, answer = "Hello world") {
  const record = tasks.create(owner);
  tasks.update(record.taskId, { status: "working", sessionId: "s1" });
  tasks.update(record.taskId, {
    status: "input_required",
    approvalUrl: APPROVAL_URL,
    approvalExpiresAt: "2026-10-16T20:00:00Z",
  });
  tasks.update(record.taskId, { status: "working" });
  tasks.update(record.taskId, {
    status: "completed",
    result: createTextResult({ sessionId: "s1", answer }),
  });

  return record.taskId;
}

describe("events capability and catalog", () => {
  it("advertises events on server/discover", async () => {
    const { rpc } = setup();

    const reply = await rpc("server/discover", {});

    expect(reply.result?.capabilities).toMatchObject({ events: {} });
  });

  it("lists the run events with webhook delivery", async () => {
    const { rpc } = setup();

    const reply = await rpc("events/list", {});
    // SAFETY: events/list returns an array of event definitions.
    const events = reply.result?.events as JsonObject[];

    expect(events.map((e) => e.name)).toEqual([
      RUN_APPROVAL_REQUIRED,
      RUN_FINISHED,
    ]);

    for (const event of events) {
      expect(event.delivery).toEqual(["webhook"]);
      expect(event.inputSchema).toMatchObject({ type: "object" });
      expect(event.payloadSchema).toMatchObject({ type: "object" });
    }
  });
});

describe("events/subscribe", () => {
  it("verifies the callback, then returns a deterministic id", async () => {
    const { subscribe, receiver } = setup();

    const reply = await subscribe(RUN_FINISHED);

    expect(reply.error).toBeUndefined();
    expect(reply.result).toMatchObject({
      id: subscriptionId("alice@example.com", CALLBACK, RUN_FINISHED, {}),
      cursor: null,
      truncated: false,
    });
    expect(Date.parse(String(reply.result?.refreshBefore))).toBeGreaterThan(
      Date.now(),
    );

    const [verification] = receiver.sent;
    expect(verification?.body.type).toBe("verification");
    expect(verification?.headers["webhook-id"]).toMatch(/^msg_verification_/);
    expect(verification?.headers["x-mcp-subscription-id"]).toBe(
      reply.result?.id,
    );
  });

  it("is idempotent and verifies a (user, callback) pair only once", async () => {
    const { subscribe, receiver } = setup();

    const first = await subscribe(RUN_FINISHED, {}, undefined, {
      ttlMs: 600_000,
    });

    const second = await subscribe(RUN_FINISHED);
    const third = await subscribe(RUN_APPROVAL_REQUIRED);

    expect(second.result?.id).toBe(first.result?.id);
    expect(third.result?.id).not.toBe(first.result?.id);
    expect(receiver.sent).toHaveLength(1);
  });

  it("grants the requested ttl within bounds, and a finite one for null", async () => {
    const { subscribe } = setup();

    const short = await subscribe(RUN_FINISHED, {}, undefined, {
      ttlMs: 600_000,
    });

    const unbounded = await subscribe(RUN_FINISHED, {}, undefined, {
      ttlMs: null,
    });

    const left = (reply: RpcReply) =>
      Date.parse(String(reply.result?.refreshBefore)) - Date.now();

    expect(left(short)).toBeLessThanOrEqual(600_000);
    expect(left(short)).toBeGreaterThan(590_000);
    expect(unbounded.result?.refreshBefore).not.toBeNull();
  });

  it("rejects a secret that is not whsec_ with a 24-64 byte key", async () => {
    const { rpc } = setup();

    const reply = await rpc("events/subscribe", {
      name: RUN_FINISHED,
      arguments: {},
      delivery: {
        mode: "webhook",
        url: CALLBACK,
        secret: `whsec_${Buffer.alloc(8).toString("base64")}`,
      },
    });

    expect(reply.error?.code).toBe(-32602);
  });

  it("rejects unknown events and unknown arguments", async () => {
    const { subscribe } = setup();

    expect((await subscribe("run.unknown")).error?.code).toBe(-32602);
    expect((await subscribe(RUN_FINISHED, { channel: "x" })).error?.code).toBe(
      -32602,
    );
  });

  it("refuses a non-https callback with CallbackEndpointError", async () => {
    const { rpc } = setup();

    const reply = await rpc("events/subscribe", {
      name: RUN_FINISHED,
      arguments: {},
      delivery: {
        mode: "webhook",
        url: "http://receiver.example.com/cb",
        secret: SECRET,
      },
    });

    expect(reply.error?.code).toBe(CALLBACK_ENDPOINT_ERROR);
    expect(reply.error?.data).toMatchObject({ reason: "invalid_url" });
  });

  it("refuses a callback that does not echo the challenge", async () => {
    const { subscribe } = setup(fakeReceiver({ verifyReply: () => "wrong" }));

    const reply = await subscribe(RUN_FINISHED);

    expect(reply.error?.code).toBe(CALLBACK_ENDPOINT_ERROR);
    expect(reply.error?.data).toMatchObject({ reason: "challenge_failed" });
  });

  it("needs a signed-in user", async () => {
    const { rpc } = setup();

    const reply = await rpc(
      "events/subscribe",
      {
        name: RUN_FINISHED,
        delivery: { mode: "webhook", url: CALLBACK, secret: SECRET },
      },
      "",
    );

    expect(reply.error).toBeDefined();
  });
});

describe("event delivery", () => {
  it("delivers approval and finish events for the owner's runs, signed", async () => {
    const { tasks, hub, subscribe, receiver } = setup();
    await subscribe(RUN_APPROVAL_REQUIRED);
    const finished = await subscribe(RUN_FINISHED);

    const runId = runToAnswer(tasks, "alice@example.com");
    await hub.idle();

    const deliveries = receiver.deliveries();
    expect(deliveries.map((d) => d.body.name)).toEqual([
      RUN_APPROVAL_REQUIRED,
      RUN_FINISHED,
    ]);

    expect(deliveries[0]?.body).toMatchObject({
      cursor: null,
      data: {
        run_id: runId,
        session_id: "s1",
        template: "hello",
        approval_url: APPROVAL_URL,
        expires_at: "2026-10-16T20:00:00Z",
      },
    });

    const done = deliveries[1];
    expect(done?.body.data).toEqual({
      run_id: runId,
      session_id: "s1",
      template: "hello",
      status: "completed",
      answer: "Hello world",
      answer_truncated: false,
      error: null,
    });
    expect(done?.headers["webhook-id"]).toBe(done?.body.eventId);
    expect(done?.headers["x-mcp-subscription-id"]).toBe(finished.result?.id);
    expect(Date.parse(String(done?.body.timestamp))).not.toBeNaN();
  });

  it("emits each event once even when the task keeps changing", async () => {
    const { tasks, hub, subscribe, receiver } = setup();
    await subscribe(RUN_APPROVAL_REQUIRED);

    const record = tasks.create("alice@example.com");
    tasks.update(record.taskId, {
      status: "input_required",
      approvalUrl: APPROVAL_URL,
    });
    tasks.update(record.taskId, { statusMessage: "still waiting" });
    tasks.update(record.taskId, { statusMessage: "still waiting…" });
    await hub.idle();

    expect(receiver.deliveries()).toHaveLength(1);
  });

  it("reports failures and cancellations as run.finished", async () => {
    const { tasks, hub, subscribe, receiver } = setup();
    await subscribe(RUN_FINISHED);

    const failed = tasks.create("alice@example.com");
    tasks.update(failed.taskId, {
      status: "completed",
      result: createErrorResult({ sessionId: "s1", error: "turn failed" }),
    });
    const cancelled = tasks.create("alice@example.com");
    tasks.update(cancelled.taskId, {
      status: "cancelled",
      statusMessage: "Approval declined",
    });
    await hub.idle();

    expect(receiver.deliveries().map((d) => d.body.data)).toMatchObject([
      { status: "failed", error: "turn failed", answer: null },
      { status: "cancelled", error: "Approval declined", answer: null },
    ]);
  });

  it("delivers nothing for other users' runs or other run ids", async () => {
    const { tasks, hub, subscribe, receiver } = setup();
    await subscribe(RUN_FINISHED, { run_id: "some-other-run" });
    await subscribe(RUN_FINISHED, {}, "bob@example.com");

    runToAnswer(tasks, "alice@example.com");
    await hub.idle();

    expect(receiver.deliveries()).toHaveLength(0);
  });

  it("delivers to a subscription filtered on the run's id", async () => {
    const { tasks, hub, subscribe, receiver } = setup();
    const record = tasks.create("alice@example.com");
    await subscribe(RUN_FINISHED, { run_id: record.taskId });

    tasks.update(record.taskId, {
      status: "completed",
      result: createTextResult({ sessionId: "s1", answer: "hi" }),
    });
    await hub.idle();

    expect(receiver.deliveries()).toHaveLength(1);
  });

  it("retries transient failures with the same event id", async () => {
    const receiver = fakeReceiver({ statuses: [503, 500] });
    const { tasks, hub, subscribe } = setup(receiver);
    await subscribe(RUN_FINISHED);

    runToAnswer(tasks, "alice@example.com");
    await hub.idle();

    const attempts = receiver.deliveries();
    expect(attempts).toHaveLength(3);
    expect(new Set(attempts.map((a) => a.headers["webhook-id"])).size).toBe(1);
  });

  it("stops on 413 and drops the subscription on 410", async () => {
    const tooLarge = fakeReceiver({ statuses: [413] });
    const first = setup(tooLarge);
    await first.subscribe(RUN_FINISHED);
    runToAnswer(first.tasks, "alice@example.com");
    await first.hub.idle();
    expect(tooLarge.deliveries()).toHaveLength(1);

    const gone = fakeReceiver({ statuses: [410] });
    const second = setup(gone);
    await second.subscribe(RUN_FINISHED);
    runToAnswer(second.tasks, "alice@example.com");
    await second.hub.idle();
    runToAnswer(second.tasks, "alice@example.com");
    await second.hub.idle();

    expect(gone.deliveries()).toHaveLength(1);
    expect(second.hub.covers("alice@example.com", "any")).toBe(false);
  });

  it("stops delivering after events/unsubscribe", async () => {
    const { tasks, hub, rpc, subscribe, receiver } = setup();
    await subscribe(RUN_FINISHED);

    const reply = await rpc("events/unsubscribe", {
      name: RUN_FINISHED,
      arguments: {},
      delivery: { mode: "webhook", url: CALLBACK },
    });

    runToAnswer(tasks, "alice@example.com");
    await hub.idle();

    expect(reply.error).toBeUndefined();
    expect(receiver.deliveries()).toHaveLength(0);

    // Idempotent.
    const again = await rpc("events/unsubscribe", {
      name: RUN_FINISHED,
      delivery: { mode: "webhook", url: CALLBACK },
    });

    expect(again.error).toBeUndefined();
  });

  it("signs with both secrets while one is being rotated", async () => {
    const { tasks, hub, subscribe, receiver } = setup();
    await subscribe(RUN_FINISHED, {}, undefined, {
      delivery: { mode: "webhook", url: CALLBACK, secret: OTHER_SECRET },
    });
    await subscribe(RUN_FINISHED);

    runToAnswer(tasks, "alice@example.com");
    await hub.idle();

    const [delivery] = receiver.deliveries();
    expect(delivery?.headers["webhook-signature"].split(" ")).toHaveLength(2);
  });

  it("keeps the fallback for approval-only subscriptions", async () => {
    const { hub, subscribe } = setup();
    await subscribe(RUN_APPROVAL_REQUIRED);

    expect(hub.covers("alice@example.com", "new-run")).toBe(false);
  });

  it("covers a launch only for the subscribing user", async () => {
    const { hub, subscribe } = setup();
    await subscribe(RUN_FINISHED);

    expect(hub.covers("alice@example.com", "r1")).toBe(true);
    expect(hub.covers("bob@example.com", "r1")).toBe(false);
    expect(hub.covers(undefined, "r1")).toBe(false);
  });
});

describe("SubscriptionStore", () => {
  it("persists subscriptions to a 0600 file and reloads them", async () => {
    const dir = mkdtempSync(join(tmpdir(), "events-"));
    cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
    const file = join(dir, "nested", "subscriptions.json");

    const { subscribe } = setup(fakeReceiver(), new SubscriptionStore(file));
    const reply = await subscribe(RUN_FINISHED);

    expect(statSync(file).mode & 0o777).toBe(0o600);
    expect(readFileSync(file, "utf8")).toContain(String(reply.result?.id));

    const reloaded = new SubscriptionStore(file);
    expect(reloaded.get(String(reply.result?.id))?.url).toBe(CALLBACK);
  });

  it("drops expired subscriptions", () => {
    const store = new SubscriptionStore();
    store.put({
      id: "sub_old",
      owner: "alice@example.com",
      name: RUN_FINISHED,
      arguments: {},
      url: CALLBACK,
      secret: SECRET,
      expiresAt: 1000,
      createdAt: new Date(0).toISOString(),
    });

    expect(store.active(2000)).toEqual([]);
    expect(store.get("sub_old")).toBeUndefined();
  });
});

describe("canonicalJson", () => {
  it("ignores object key order", () => {
    expect(
      canonicalJson({ b: 1, a: { d: [1, { y: 2, x: 1 }], c: null } }),
    ).toBe(canonicalJson({ a: { c: null, d: [1, { x: 1, y: 2 }] }, b: 1 }));
  });
});

describe("webhook safety", () => {
  it.each([
    ["8.8.8.8", true],
    ["2606:4700:4700::1111", true],
    ["10.1.2.3", false],
    ["127.0.0.1", false],
    ["169.254.169.254", false],
    ["172.20.0.1", false],
    ["192.168.1.1", false],
    ["100.64.0.1", false],
    ["::1", false],
    ["fd00::1", false],
    ["fe80::1", false],
    ["::ffff:10.0.0.1", false],
    ["::ffff:8.8.8.8", true],
    ["not-an-ip", false],
  ])("isPublicAddress(%s) is %s", (address, expected) => {
    expect(isPublicAddress(address)).toBe(expected);
  });

  it.each([
    "http://example.com/cb",
    "https://127.0.0.1/cb",
    "https://[::1]/cb",
    "https://localhost/cb",
    "https://user:pass@example.com/cb",
    "not a url",
  ])("rejects callback %s", (url) => {
    expect(() => validateCallbackUrl(url)).toThrow(CallbackError);
  });

  it("matches the Standard Webhooks signature scheme", () => {
    const key = Buffer.alloc(32, 7);

    const expected = createHmac("sha256", key)
      .update("msg_1.1700000000.{}")
      .digest("base64");

    expect(signWebhook([SECRET], "msg_1", 1_700_000_000, "{}")).toBe(
      `v1,${expected}`,
    );
  });
});
