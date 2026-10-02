import { build } from "esbuild";
import { createNodeClient } from "../src/node.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { mkdtempSync, writeFileSync } from "node:fs";
import { createServer, type AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Code, ConnectError } from "@connectrpc/connect";
import { create, toBinary, toJson } from "@bufbuild/protobuf";
import { timestampDate, ValueSchema } from "@bufbuild/protobuf/wkt";
import {
  EndReason,
  ErrorInfoSchema,
  Reason,
  SessionState,
  Sentinel,
  SentinelSchema,
  isLive,
  sentinelOf,
  subscribe,
  type HarnessClient,
} from "../src/index.js";
import { collect, failure, firstOf, sentinelName, Stub, Stubbed, type StubClientOptions } from "./stub.js";

let stub: Stub;

beforeAll(async () => {
  stub = await Stub.start();
});

afterAll(() => {
  stub?.stop();
});

const byId = (sessionId: string) => ({ ref: { sessionId } });

const events = async (client: HarnessClient, sessionId: string, page: { afterSeq?: bigint; limit?: number } = {}) =>
  (await client.listEvents({ ...byId(sessionId), ...page })).events;

const codecs = [
  { name: "binary", useJson: false },
  { name: "json", useJson: true },
];

for (const codec of codecs) {
  describe(`conformance over ${codec.name}`, () => {
    const newClient = (extra: StubClientOptions = {}) => stub.client({ useJson: codec.useJson, ...extra });

    const open = async (client: HarnessClient, conversationRef: string, initialPrompt = "") =>
      (await client.createSession({ template: "runid", conversationRef, approvalPrompt: "ship it", initialPrompt }))
        .session!;

    it("drives the full lifecycle, taking the approval URL off the log", async () => {
      const client = newClient();
      const session = await open(client, `${codec.name}-lifecycle`);
      expect(session.id).not.toBe("");
      expect(session.state).toBe(SessionState.PENDING);
      expect(isLive(session.state)).toBe(true);

      const log = await events(client, session.id);
      const approval = firstOf(log, "approvalRequired");
      expect(approval, "an approval_required event").toBeDefined();
      expect(approval!.approvalUrl).toContain("/approve/");
      expect(firstOf(log, "stateChanged")).toMatchObject({
        old: SessionState.PENDING,
        new: SessionState.LAUNCHING,
        reason: Reason.LAUNCH,
      });
      expect(log.map((e) => e.seq)).toEqual(log.map((_, i) => BigInt(i + 1)));
      const times = log.map((e) => timestampDate(e.timestamp!).getTime());
      expect(times.slice(1).every((t, i) => t > times[i])).toBe(true);

      const turn = await client.prompt({ ...byId(session.id), content: "do the thing" });
      expect(turn.turnId).not.toBe("");
      expect((await client.listTemplates({})).templates.length).toBeGreaterThan(0);

      await client.endSession({ ...byId(session.id), reason: EndReason.ENDED });
      const ended = (await client.getSession(byId(session.id))).session!;
      expect(ended.state).toBe(SessionState.ENDED);
      expect(isLive(ended.state)).toBe(false);
      const tail = (await events(client, session.id)).slice(-2);
      expect(tail[0].payload).toMatchObject({
        case: "stateChanged",
        value: { new: SessionState.ENDED, reason: Reason.UNSPECIFIED },
      });
      expect(tail[1].payload).toMatchObject({ case: "sessionEnded", value: { reason: EndReason.ENDED } });
    });

    it("maps every published sentinel, including the pairs that share a code", async () => {
      const expected = new Map<Sentinel, Code>([
        [Sentinel.NOT_FOUND, Code.NotFound],
        [Sentinel.FORBIDDEN, Code.PermissionDenied],
        [Sentinel.CONFLICT, Code.AlreadyExists],
        [Sentinel.INVALID_STATE, Code.FailedPrecondition],
        [Sentinel.INVALID_ARGUMENT, Code.InvalidArgument],
        [Sentinel.UNKNOWN_REQUEST, Code.NotFound],
        [Sentinel.NOT_REVIVABLE, Code.FailedPrecondition],
        [Sentinel.UNAVAILABLE, Code.Unavailable],
        [Sentinel.QUOTA_EXCEEDED, Code.ResourceExhausted],
      ]);
      const published = SentinelSchema.values.map((v) => v.number as Sentinel).filter((s) => s !== Sentinel.UNSPECIFIED);
      expect(new Set(expected.keys())).toEqual(new Set(published));

      const client = newClient({ retries: 0 });
      for (const [sentinel, code] of expected) {
        const name = sentinelName(sentinel);
        const err = await failure(client.getSession(byId(Stubbed.sentinelRef(sentinel))));
        expect(err, `${name} produced no error`).toBeInstanceOf(ConnectError);
        expect(sentinelOf(err), `${name} lost its sentinel`).toBe(sentinel);
        expect((err as ConnectError).code, `${name} arrived as the wrong code`).toBe(code);
      }
    });

    it("hides another client's session behind not_found, never permission_denied", async () => {
      const alice = newClient({ client: "alice" });
      const bob = newClient({ client: "bob" });
      const session = await open(alice, `${codec.name}-cross`);
      expect(sentinelOf(await failure(bob.getSession(byId(session.id))))).toBe(Sentinel.NOT_FOUND);
      expect(sentinelOf(await failure(subscribe(bob, byId(session.id))))).toBe(Sentinel.NOT_FOUND);
      expect((await bob.listSessions({})).sessions).toHaveLength(0);
    });

    it("stops the feed on the session_ended EVENT, not only on end of stream", async () => {
      const client = newClient();
      const session = await open(client, `${codec.name}-ended`);
      const feed = await subscribe(client, byId(session.id));
      await client.endSession(byId(session.id));
      const got = await collect(feed, 10_000);
      expect(got.at(-1)?.payload).toMatchObject({ case: "sessionEnded", value: { reason: EndReason.ENDED } });
    });

    it("ends the feed when it is closed inside the loop", async () => {
      const client = newClient();
      const session = await open(client, `${codec.name}-close-inside`, "do the thing");
      const feed = await subscribe(client, byId(session.id));
      const got = [];
      for await (const ev of feed) {
        got.push(ev);
        if (ev.payload.case === "turnCompleted") {
          feed.close();
        }
      }
      expect(got.at(-1)?.payload.case).toBe("turnCompleted");
    });

    it("treats an already-finished log as a success with an empty feed", async () => {
      const client = newClient();
      const session = await open(client, `${codec.name}-finished`);
      await client.endSession(byId(session.id));
      const feed = await subscribe(client, { ...byId(session.id), afterSeq: 1n << 30n });
      expect(await collect(feed, 10_000)).toHaveLength(0);
    });

    it("survives a stream that ends cleanly before saying anything", async () => {
      const client = newClient({ scenario: "no-frames" });
      const session = await open(client, `${codec.name}-noframes`);
      expect(await collect(await subscribe(client, byId(session.id)), 10_000)).toHaveLength(0);
    });

    it("resumes after a dropped connection with no gap and no duplicate", async () => {
      const driver = newClient();
      const session = await open(driver, `${codec.name}-resume`, "do the thing");
      const before = await events(driver, session.id);
      expect(before.length).toBeGreaterThan(3);

      const reader = newClient({ scenario: "drop-after=3", scenarioKey: `${codec.name}-resume` });
      const feed = await subscribe(reader, byId(session.id), { reconnectBackoffMs: 100 });
      setTimeout(() => void driver.endSession(byId(session.id)), 500);
      const got = await collect(feed, 20_000);

      expect(got.length).toBeGreaterThanOrEqual(before.length);
      expect(got.slice(0, before.length).map((e) => e.seq)).toEqual(before.map((e) => e.seq));
      for (let i = 1; i < got.length; i++) {
        expect(got[i].seq > got[i - 1].seq, "a duplicate crossed the resume").toBe(true);
      }
    });

    it("gives up on a stream that goes silent and resumes from where it stopped", async () => {
      const driver = newClient();
      const session = await open(driver, `${codec.name}-silent`, "do the thing");
      const reader = newClient({ scenario: "silent", scenarioKey: `${codec.name}-silent` });
      const feed = await subscribe(reader, byId(session.id), {
        keepaliveIntervalMs: 100,
        missedKeepalives: 3,
        reconnectBackoffMs: 100,
      });
      setTimeout(() => void driver.endSession(byId(session.id)), 1000);
      const got = await collect(feed, 20_000);
      expect(got.length, "the client never gave up on a silent stream").toBeGreaterThan(0);
      expect(got[0].seq).toBe(1n);
      expect(got.at(-1)?.payload.case).toBe("sessionEnded");
    });

    it("tolerates a response field it has never heard of", async () => {
      const client = newClient({ scenario: "unknown-field" });
      const session = await open(client, `${codec.name}-unknown-field`);
      expect(session.id).not.toBe("");
      expect((await events(client, session.id)).length).toBeGreaterThan(0);
      const feed = await subscribe(client, byId(session.id));
      await client.endSession(byId(session.id));
      expect((await collect(feed, 10_000)).length).toBeGreaterThan(0);
    });

    it("delivers an event whose payload it has never heard of", async () => {
      const client = newClient();
      const session = await open(client, `${codec.name}-unknown-event`, Stubbed.promptUnknownEvent);
      const log = await events(client, session.id);
      const future = log.find((e) => e.payload.case === undefined);
      expect(future, "the unknown event did not survive").toBeDefined();
      expect(future!.seq).toBeGreaterThan(0n);
      expect(future!.turnId).not.toBe("");
      expect(log.some((e) => e.payload.case === "turnCompleted")).toBe(true);

      const feed = await subscribe(client, byId(session.id));
      await client.endSession(byId(session.id));
      const got = await collect(feed, 10_000);
      expect(got.find((e) => e.seq === future!.seq)?.payload.case).toBeUndefined();
      expect(got.map((e) => e.seq)).toContain(future!.seq);
    });

    it("reads absent fields as proto3 defaults, including a seq of 0", async () => {
      const client = newClient();
      const session = await open(client, `${codec.name}-zero`);
      const feed = await subscribe(client, byId(session.id));
      await client.prompt({ ...byId(session.id), content: Stubbed.promptZeroEvent });
      await client.endSession(byId(session.id));
      const got = await collect(feed, 10_000);
      expect(got.length).toBeGreaterThan(0);
      expect(got.some((e) => e.seq === 0n)).toBe(false);
      expect(got.at(-1)?.payload.case).toBe("sessionEnded");
    });

    it("round-trips a permission request", async () => {
      const client = newClient();
      const session = await open(client, `${codec.name}-perm`, Stubbed.promptPermission);
      const log = await events(client, session.id);
      const ask = firstOf(log, "permissionRequest");
      expect(ask, "a permission_request event").toBeDefined();
      expect(ask!.options.length).toBeGreaterThan(0);
      expect(log.some((e) => e.payload.case === "turnCompleted")).toBe(false);
      const call = firstOf(log, "toolCall");
      expect(call?.id).toBe(ask!.toolCallId);
      expect(toJson(ValueSchema, call!.toolInput!)).toEqual({ target: "production" });

      const answer = { ...byId(session.id), requestId: ask!.requestId, optionId: "allow" };
      await client.respondPermission(answer);
      const after = await events(client, session.id);
      const resolved = firstOf(after, "permissionResolved");
      expect(resolved?.requestId).toBe(ask!.requestId);
      expect(resolved?.resolution).toEqual({ case: "optionId", value: "allow" });
      expect(after.some((e) => e.payload.case === "turnCompleted")).toBe(true);
      expect(sentinelOf(await failure(client.respondPermission(answer)))).toBe(Sentinel.UNKNOWN_REQUEST);
    });

    it("addresses a session by conversation ref, terminal ones on request", async () => {
      const client = newClient();
      const conversationRef = `${codec.name}-byref`;
      const session = await open(client, conversationRef);
      expect((await client.getSession({ ref: { conversationRef } })).session?.id).toBe(session.id);
      await client.endSession(byId(session.id));
      await expect(client.getSession({ ref: { conversationRef } })).rejects.toThrow();
      const past = (await client.getSession({ ref: { conversationRef, includeTerminal: true } })).session!;
      expect(past.id).toBe(session.id);
      expect(past.template).toBe("runid");
    });

    it("refuses a second live session on one conversation", async () => {
      const client = newClient();
      const req = { template: "runid", conversationRef: `${codec.name}-conflict`, approvalPrompt: "ship it" };
      await client.createSession(req);
      expect(sentinelOf(await failure(client.createSession(req)))).toBe(Sentinel.CONFLICT);
    });

    it("gives a repeated idempotency key the first prompt's turn", async () => {
      const client = newClient();
      const session = await open(client, `${codec.name}-keyed`);
      const prompt = (idempotencyKey: string) =>
        client.prompt({ ...byId(session.id), content: "deploy", idempotencyKey });
      const first = await prompt("msg-1");
      expect((await prompt("msg-1")).turnId).toBe(first.turnId);
      expect((await prompt("msg-2")).turnId).not.toBe(first.turnId);
    });

    it("requires an approval prompt", async () => {
      const client = newClient();
      const err = await failure(
        client.createSession({ template: "runid", conversationRef: `${codec.name}-noprompt`, approvalPrompt: "" }),
      );
      expect(sentinelOf(err)).toBe(Sentinel.INVALID_ARGUMENT);
    });

    it("pages listEvents from a sequence", async () => {
      const client = newClient();
      const session = await open(client, `${codec.name}-page`, "do the thing");
      const all = await events(client, session.id);
      expect(all.length).toBeGreaterThan(4);
      const page = await events(client, session.id, { limit: 2 });
      expect(page).toHaveLength(2);
      expect(page[0].seq).toBe(1n);
      const rest = await events(client, session.id, { afterSeq: page[1].seq });
      expect(rest[0].seq).toBe(3n);
      expect(rest.length).toBe(all.length - 2);
    });
  });
}

describe("retries", () => {
  const unavailable = byId(Stubbed.sentinelRef(Sentinel.UNAVAILABLE));

  it("re-sends an unavailable call and gives up after the configured count", async () => {
    const err = await failure(stub.client({ retries: 2 }).getSession(unavailable));
    expect(sentinelOf(err)).toBe(Sentinel.UNAVAILABLE);
    expect((err as ConnectError).code).toBe(Code.Unavailable);
  });

  it("never resends a prompt without an idempotency key", async () => {
    const started = Date.now();
    const err = await failure(stub.client({ retries: 2 }).prompt({ ...unavailable, content: "deploy" }));
    expect(sentinelOf(err)).toBe(Sentinel.UNAVAILABLE);
    expect(Date.now() - started).toBeLessThan(500);
  });

  it("resends a prompt that carries an idempotency key", async () => {
    const started = Date.now();
    const err = await failure(
      stub.client({ retries: 2 }).prompt({ ...unavailable, content: "deploy", idempotencyKey: "msg-1" }),
    );
    expect(sentinelOf(err)).toBe(Sentinel.UNAVAILABLE);
    expect(Date.now() - started).toBeGreaterThanOrEqual(550);
  });

  it("gives each attempt its own deadline and reports the timeout as deadline_exceeded", async () => {
    const server = createServer(() => undefined);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as AddressInfo;
    try {
      const client = createNodeClient({ baseUrl: `http://127.0.0.1:${port}`, requestTimeoutMs: 100, retries: 1 });
      const started = Date.now();
      const err = await failure(client.listTemplates({}));
      expect((err as ConnectError).code).toBe(Code.DeadlineExceeded);
      expect(Date.now() - started).toBeGreaterThanOrEqual(400);
    } finally {
      server.close();
    }
  });

  it("stops retrying when the caller cancels during the backoff", async () => {
    const controller = new AbortController();
    setTimeout(() => controller.abort(), 100);
    const started = Date.now();
    const err = await failure(stub.client({ retries: 5 }).getSession(unavailable, { signal: controller.signal }));
    expect((err as ConnectError).code).toBe(Code.Canceled);
    expect(Date.now() - started).toBeLessThan(400);
  });

  it("does not retry a considered refusal", async () => {
    const started = Date.now();
    await failure(stub.client({ retries: 2 }).getSession(byId(Stubbed.sentinelRef(Sentinel.FORBIDDEN))));
    expect(Date.now() - started).toBeLessThan(500);
  });
});

describe("sentinelOf", () => {
  const refusal = (sentinel: Sentinel) => {
    const err = new ConnectError("refused", Code.NotFound);
    err.details.push({
      type: ErrorInfoSchema.typeName,
      value: toBinary(ErrorInfoSchema, create(ErrorInfoSchema, { sentinel, detail: "why" })),
    });
    return err;
  };

  it("tolerates a sentinel it has never heard of, leaving the code to branch on", () => {
    const err = refusal(42 as Sentinel);
    expect(sentinelOf(err)).toBeUndefined();
    expect(err.code).toBe(Code.NotFound);
  });

  it("reads an unspecified sentinel as no sentinel", () => {
    expect(sentinelOf(refusal(Sentinel.UNSPECIFIED))).toBeUndefined();
  });

  it("reads no sentinel off an error that is not a Connect error", () => {
    expect(sentinelOf(new Error("boom"))).toBeUndefined();
  });
});

describe("client", () => {
  it("sends the token file as the bearer on calls and streams", async () => {
    const tokenFile = join(mkdtempSync(join(tmpdir(), "harness-")), "token");
    writeFileSync(tokenFile, "carol\n");
    const client = createNodeClient({ baseUrl: stub.baseUrl, tokenFile });
    const { session } = await client.createSession({
      template: "runid",
      conversationRef: "bearer",
      approvalPrompt: "ship it",
    });
    const feed = await subscribe(client, byId(session!.id));
    await client.endSession(byId(session!.id));
    expect((await collect(feed, 10_000)).at(-1)?.payload.case).toBe("sessionEnded");
    expect((await stub.client({ client: "carol" }).getSession(byId(session!.id))).session?.id).toBe(session!.id);
  });

  it("ends a feed closed from elsewhere without waiting out the backoff", async () => {
    const driver = stub.client();
    const { session } = await driver.createSession({
      template: "runid",
      conversationRef: "close-elsewhere",
      approvalPrompt: "ship it",
    });
    const reader = stub.client({ scenario: "drop-after=2", scenarioKey: "ts-close-elsewhere" });
    const feed = await subscribe(reader, byId(session!.id), { reconnectBackoffMs: 30_000 });
    const consumer = collect(feed, 10_000);
    await new Promise((resolve) => setTimeout(resolve, 300));
    const started = Date.now();
    feed.close();
    await consumer;
    expect(Date.now() - started).toBeLessThan(1000);
  });
});

describe("packaging", () => {
  it("keeps Node modules out of the main entry point", async () => {
    const result = await build({ entryPoints: ["src/index.ts"], bundle: true, platform: "browser", write: false, logLevel: "silent" });
    expect(result.errors).toHaveLength(0);
  });
});
