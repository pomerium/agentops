import { afterEach, describe, expect, it } from "vite-plus/test";
import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import {
  McpServer,
  createMcpHandler,
  type CallToolResult,
  type ElicitRequest,
  type ElicitResult,
} from "@modelcontextprotocol/server";
import { Effect } from "effect";
import type { JsonObject, JsonValue } from "./lib/json.ts";
import {
  HarnessError,
  type HarnessClient,
  type HarnessEvent,
  type TemplateSummary,
} from "./harness.ts";
import { userFromHeaders } from "./identity.ts";
import {
  registerTools,
  signState,
  verifyState,
  type RunEvents,
  type ToolDeps,
} from "./tools.ts";
import { registerTasks, TASKS_EXTENSION, TaskStore } from "./tasks.ts";

const PROTOCOL = "2026-07-28";
const SESSION_ID = "s1";
const APPROVAL_URL = "https://agentic.example.com/agentic/approve?run_id=run-1";
const APPROVAL_KEY = "approve-run-2";

type RpcReply = {
  jsonrpc: "2.0";
  id: number;
  result?: JsonObject;
  error?: { code: number; message: string };
};

type FakeHarness = {
  client: HarnessClient;
  /** Mutable: tests append events to drive the task forward. */
  events: HarnessEvent[];
  endSessions: string[];
};

/** A scripted Harness API: `listEvents` replays events after `afterSeq`. */
function fakeHarness(
  initial: HarnessEvent[] = [],
  overrides: Partial<HarnessClient> = {},
): FakeHarness {
  const events = [...initial];
  const endSessions: string[] = [];

  const client: HarnessClient = {
    listTemplates: () =>
      Effect.succeed([{ name: "hello", description: "Says hello" }]),
    createSession: () => Effect.succeed({ id: SESSION_ID }),
    listEvents: (_sessionId, afterSeq) =>
      Effect.succeed(events.filter((e) => Number(e.seq) > afterSeq)),
    endSession: (sessionId) =>
      Effect.sync(() => {
        endSessions.push(sessionId);
      }),
    listSessions: () => Effect.succeed([]),
    ...overrides,
  };

  return { client, events, endSessions };
}

function event(seq: number, payload: Omit<HarnessEvent, "seq">): HarnessEvent {
  return { seq: String(seq), ...payload };
}

const LAUNCH_EVENTS: HarnessEvent[] = [
  event(1, { stateChanged: { new: "SESSION_STATE_LAUNCHING" } }),
  event(2, { approvalRequired: { approvalUrl: APPROVAL_URL } }),
];

const APPROVED_AND_ANSWERED: HarnessEvent[] = [
  event(3, { approved: { approverSubject: "approver@example.com" } }),
  event(4, { stateChanged: { new: "SESSION_STATE_RUNNING" } }),
  event(5, { agentMessage: { text: "Hello " } }),
  event(6, { agentMessage: { text: "world" } }),
  event(7, { turnCompleted: { stopReason: "end_turn" } }),
];

function base64url(value: JsonValue): string {
  return Buffer.from(JSON.stringify(value)).toString("base64url");
}

/** A JWT-shaped `X-Pomerium-Jwt-Assertion` value; the signature is not checked. */
function assertionFor(email: string): string {
  return `${base64url({ alg: "none" })}.${base64url({ email })}.sig`;
}

const cleanups: Array<() => Promise<void> | void> = [];

afterEach(async () => {
  while (cleanups.length > 0) {
    await cleanups.pop()?.();
  }
});

function metaFor(
  declareTasks: boolean,
  declareUrlElicitation = true,
  declareFormElicitation = false,
): JsonObject {
  const capabilities: JsonObject = declareUrlElicitation
    ? { elicitation: { url: {} } }
    : declareFormElicitation
      ? { elicitation: {} }
      : {};

  if (declareTasks) {
    capabilities.extensions = { [TASKS_EXTENSION]: {} };
  }

  return {
    "io.modelcontextprotocol/protocolVersion": PROTOCOL,
    "io.modelcontextprotocol/clientInfo": {
      name: "test-client",
      version: "0.0.0",
    },
    "io.modelcontextprotocol/clientCapabilities": capabilities,
  };
}

type RpcOptions = {
  /** `mcp-name` header: the tool name, or the task id for tasks/* methods. */
  name?: string;
  /** Pomerium identity to send as `X-Pomerium-Jwt-Assertion`. */
  user?: string;
  /** Whether the client declares the tasks extension. Defaults to true. */
  declareTasks?: boolean;
  /** Whether the client declares URL-mode elicitation. Defaults to true. */
  declareUrlElicitation?: boolean;
  /** Form-only elicitation (`elicitation: {}`), when URL mode is off. */
  declareFormElicitation?: boolean;
};

type TaskServer = {
  store: TaskStore;
  /** Raw JSON-RPC over the 2026-07-28 stateless handler, bypassing the SDK client. */
  rpc: (
    method: string,
    params: JsonObject,
    options?: RpcOptions,
  ) => Promise<RpcReply>;
};

/**
 * A raw-JSON-RPC client for the task flow: the SDK Client cannot consume
 * `resultType: "task"`, so these tests send the requests directly.
 */
function setupTaskServer(
  harness: HarnessClient,
  events?: RunEvents,
): TaskServer {
  const store = new TaskStore();

  const handler = createMcpHandler(() => {
    const server = new McpServer({ name: "test-server", version: "0.0.0" });
    registerTasks(server, store);

    const deps: ToolDeps = {
      harness: () => harness,
      store,
      template: "hello",
    };

    if (events) deps.events = () => events;
    registerTools(server, deps);

    return server;
  });

  cleanups.push(() => handler.close());

  let nextId = 0;

  const rpc = async (
    method: string,
    params: JsonObject,
    options: RpcOptions = {},
  ): Promise<RpcReply> => {
    const headers = new Headers({
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
      "mcp-protocol-version": PROTOCOL,
      "mcp-method": method,
    });

    if (options.name) {
      headers.set("mcp-name", options.name);
    }

    if (options.user) {
      headers.set("x-pomerium-jwt-assertion", assertionFor(options.user));
    }

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
            _meta: metaFor(
              options.declareTasks ?? true,
              options.declareUrlElicitation ?? true,
              options.declareFormElicitation ?? false,
            ),
          },
        }),
      }),
    );

    // SAFETY: the handler answers every request with a JSON-RPC envelope.
    return (await response.json()) as RpcReply;
  };

  return { store, rpc };
}

async function launch(
  server: TaskServer,
  options: { user?: string } = {},
): Promise<string> {
  const reply = await server.rpc(
    "tools/call",
    { name: "launch_hello_demo", arguments: { prompt: "Hi" } },
    { name: "launch_hello_demo", ...options },
  );

  expect(reply.error).toBeUndefined();
  expect(reply.result?.resultType).toBe("task");

  // SAFETY: a CreateTaskResult always carries a string taskId.
  return reply.result?.taskId as string;
}

async function getTask(
  server: TaskServer,
  taskId: string,
  options: RpcOptions = {},
): Promise<JsonObject> {
  const reply = await server.rpc(
    "tasks/get",
    { taskId },
    {
      name: taskId,
      ...options,
    },
  );

  expect(reply.error).toBeUndefined();

  // SAFETY: a successful tasks/get returns a DetailedTask object.
  return reply.result as JsonObject;
}

/** Polls tasks/get until `done` holds (or the deadline passes). */
async function pollTask(
  server: TaskServer,
  taskId: string,
  done: (task: JsonObject) => boolean,
  timeoutMs = 10_000,
): Promise<JsonObject> {
  const deadline = Date.now() + timeoutMs;

  for (;;) {
    const task = await getTask(server, taskId);

    if (done(task) || Date.now() > deadline) {
      return task;
    }

    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

/** Polls a condition until it holds (or the deadline passes). */
async function waitFor<T>(
  read: () => T,
  done: (value: T) => boolean,
  timeoutMs = 10_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;

  for (;;) {
    const value = read();

    if (done(value) || Date.now() > deadline) {
      return value;
    }

    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

function parseText(result: CallToolResult): JsonObject {
  const item = result.content[0];
  expect(item?.type).toBe("text");

  if (item?.type !== "text") {
    throw new Error("expected text content");
  }

  // SAFETY: every tool in this server returns a JSON object as text.
  return JSON.parse(item.text) as JsonObject;
}

/** The task's result, parsed from its text content. */
function taskResultJson(task: JsonObject): JsonObject {
  expect(task.result).toBeDefined();

  // SAFETY: a completed task's result is a CallToolResult (see demo-run.ts).
  return parseText(task.result as CallToolResult);
}

const isTerminalStatus = (task: JsonObject): boolean =>
  ["completed", "failed", "cancelled"].some((s) => s === task.status);

/**
 * An SDK client talking to createMcpHandler over an in-process fetch bridge,
 * pinned to 2026-07-28. It does not declare the tasks extension, so the server
 * takes the MRTR fallback.
 */
async function connectModernClient(
  fake: FakeHarness = fakeHarness(LAUNCH_EVENTS),
  options: { user?: string } = {},
): Promise<{ client: Client; fake: FakeHarness }> {
  const store = new TaskStore();

  const handler = createMcpHandler(() => {
    const server = new McpServer({ name: "test-server", version: "0.0.0" });
    registerTasks(server, store);
    registerTools(server, {
      harness: () => fake.client,
      store,
      template: "hello",
    });

    return server;
  });

  cleanups.push(() => handler.close());

  const transport = new StreamableHTTPClientTransport(
    new URL("http://localhost/mcp"),
    {
      fetch: async (input, init) => {
        const headers = new Headers(init?.headers);

        if (options.user) {
          headers.set("x-pomerium-jwt-assertion", assertionFor(options.user));
        }

        return handler.fetch(new Request(input, { ...init, headers }));
      },
    },
  );

  const client = new Client(
    { name: "test-client", version: "0.0.0" },
    {
      capabilities: { elicitation: { url: {} } },
      versionNegotiation: { mode: { pin: PROTOCOL } },
    },
  );

  cleanups.push(() => client.close());

  await client.connect(transport);
  expect(client.getProtocolEra()).toBe("modern");

  return { client, fake };
}

describe("tool and task surface", () => {
  it("lists exactly the AgentOps tools", async () => {
    const { client } = await connectModernClient();

    const result = await client.listTools();

    expect(result.tools.map((tool) => tool.name).sort()).toEqual([
      "end_session",
      "get_run_status",
      "launch_hello_demo",
      "list_agents",
      "list_pending_approvals",
    ]);
  });

  it("advertises the tasks extension on server/discover", async () => {
    const { client } = await connectModernClient();

    expect(client.getServerCapabilities()?.extensions).toMatchObject({
      [TASKS_EXTENSION]: {},
    });
  });
});

describe("list_agents", () => {
  it("returns the templates from the Harness API", async () => {
    const { client } = await connectModernClient();

    const result = await client.callTool({
      name: "list_agents",
      arguments: {},
    });

    expect(result.isError).toBeFalsy();

    const templates: TemplateSummary[] = [
      { name: "hello", description: "Says hello" },
    ];

    expect(parseText(result)).toEqual({ templates });
  });

  it("returns an error result when the Harness API fails", async () => {
    const fake = fakeHarness(LAUNCH_EVENTS, {
      listTemplates: () =>
        Effect.fail(
          new HarnessError({ method: "ListTemplates", message: "boom" }),
        ),
    });

    const { client } = await connectModernClient(fake);

    const result = await client.callTool({
      name: "list_agents",
      arguments: {},
    });

    expect(result.isError).toBe(true);
    expect(parseText(result)).toEqual({ error: "boom" });
  });
});

describe("launch_hello_demo as a task", () => {
  it("reports approval as input_required, then completes with the answer", async () => {
    const fake = fakeHarness(LAUNCH_EVENTS);
    const server = setupTaskServer(fake.client);

    const taskId = await launch(server);

    const waiting = await pollTask(
      server,
      taskId,
      (task) => task.status === "input_required",
    );

    expect(waiting.status).toBe("input_required");
    expect(waiting.inputRequests).toEqual({
      [APPROVAL_KEY]: {
        method: "elicitation/create",
        params: expect.objectContaining({
          mode: "url",
          url: APPROVAL_URL,
        }),
      },
    });

    fake.events.push(...APPROVED_AND_ANSWERED);

    const done = await pollTask(server, taskId, isTerminalStatus);
    expect(done.status).toBe("completed");
    expect(taskResultJson(done)).toEqual({
      sessionId: SESSION_ID,
      answer: "Hello world",
    });
  }, 20_000);

  it("cancels the task and ends the session when the approval is declined", async () => {
    const fake = fakeHarness(LAUNCH_EVENTS);
    const server = setupTaskServer(fake.client);
    const taskId = await launch(server);
    await pollTask(server, taskId, (task) => task.status === "input_required");

    const reply = await server.rpc(
      "tasks/update",
      {
        taskId,
        inputResponses: { [APPROVAL_KEY]: { action: "decline" } },
      },
      { name: taskId },
    );

    expect(reply.error).toBeUndefined();

    const cancelled = await pollTask(
      server,
      taskId,
      (task) => task.status === "cancelled",
    );

    expect(cancelled.status).toBe("cancelled");
    expect(cancelled.statusMessage).toMatch(/declined/);
    expect(
      await waitFor(
        () => fake.endSessions,
        (ids) => ids.length > 0,
      ),
    ).toEqual([SESSION_ID]);
  }, 20_000);

  it("cancels the task and ends the session on tasks/cancel", async () => {
    const fake = fakeHarness(LAUNCH_EVENTS);
    const server = setupTaskServer(fake.client);
    const taskId = await launch(server);
    await pollTask(server, taskId, (task) => task.status === "input_required");

    const reply = await server.rpc(
      "tasks/cancel",
      { taskId },
      { name: taskId },
    );

    expect(reply.error).toBeUndefined();

    const cancelled = await pollTask(
      server,
      taskId,
      (task) => task.status === "cancelled",
    );

    expect(cancelled.status).toBe("cancelled");
    expect(
      await waitFor(
        () => fake.endSessions,
        (ids) => ids.length > 0,
      ),
    ).toEqual([SESSION_ID]);
  }, 20_000);

  it("completes with an error result when the turn fails", async () => {
    const fake = fakeHarness(LAUNCH_EVENTS);
    const server = setupTaskServer(fake.client);
    const taskId = await launch(server);
    await pollTask(server, taskId, (task) => task.status === "input_required");

    fake.events.push(
      event(3, { approved: {} }),
      event(4, { turnFailed: { reason: "model overloaded" } }),
    );

    const done = await pollTask(server, taskId, isTerminalStatus);
    expect(done.status).toBe("completed");
    expect(done.result).toMatchObject({ isError: true });
    expect(taskResultJson(done)).toMatchObject({
      sessionId: SESSION_ID,
      error: "model overloaded",
    });
  }, 20_000);

  it("completes with an error result when the session ends before an answer", async () => {
    const fake = fakeHarness(LAUNCH_EVENTS);
    const server = setupTaskServer(fake.client);
    const taskId = await launch(server);
    await pollTask(server, taskId, (task) => task.status === "input_required");

    fake.events.push(
      event(3, {
        sessionEnded: { reason: "EXPIRED", detail: "approval timed out" },
      }),
    );

    const done = await pollTask(server, taskId, isTerminalStatus);
    expect(done.status).toBe("completed");
    expect(done.result).toMatchObject({ isError: true });
    expect(taskResultJson(done)).toMatchObject({
      error: "EXPIRED: approval timed out",
    });
  }, 20_000);

  it("scopes tasks to the user who created them", async () => {
    const server = setupTaskServer(fakeHarness(LAUNCH_EVENTS).client);
    const taskId = await launch(server, { user: "alice@example.com" });

    const anonymous = await server.rpc(
      "tasks/get",
      { taskId },
      { name: taskId },
    );

    expect(anonymous.error).toBeDefined();
    expect(anonymous.result).toBeUndefined();

    const bob = await server.rpc(
      "tasks/get",
      { taskId },
      { name: taskId, user: "bob@example.com" },
    );

    expect(bob.error).toBeDefined();
    expect(bob.result).toBeUndefined();

    const alice = await server.rpc(
      "tasks/get",
      { taskId },
      { name: taskId, user: "alice@example.com" },
    );

    expect(alice.error).toBeUndefined();
    expect(alice.result).toMatchObject({ taskId });
  });
});

describe("launch_hello_demo with MCP Events first", () => {
  const subscribed: RunEvents = { covers: () => true };

  it("prefers events over the tasks extension when the user subscribed", async () => {
    const server = setupTaskServer(
      fakeHarness(LAUNCH_EVENTS).client,
      subscribed,
    );

    const reply = await server.rpc(
      "tools/call",
      { name: "launch_hello_demo", arguments: {} },
      { name: "launch_hello_demo", user: "alice@example.com" },
    );

    expect(reply.error).toBeUndefined();
    expect(reply.result?.resultType).not.toBe("task");
    expect(reply.result?.structuredContent).toMatchObject({
      delivery: "events",
      status: "input_required",
      approvalUrl: APPROVAL_URL,
    });
    expect(JSON.stringify(reply.result?.content)).toContain("run.finished");
    expect(JSON.stringify(reply.result?.content)).toContain(
      "No need to call get_run_status",
    );
  }, 20_000);

  it("falls back to a task when no subscription covers the run", async () => {
    const server = setupTaskServer(fakeHarness(LAUNCH_EVENTS).client, {
      covers: () => false,
    });

    await launch(server, { user: "alice@example.com" });
  });
});

describe("launch_hello_demo over MRTR (no tasks extension)", () => {
  it("asks for approval by URL and resolves with the answer after accept", async () => {
    const fake = fakeHarness(LAUNCH_EVENTS);
    const { client } = await connectModernClient(fake);
    const requests: JsonObject[] = [];
    client.setRequestHandler(
      "elicitation/create",
      async (request: ElicitRequest): Promise<ElicitResult> => {
        // SAFETY: the SDK validated this as an elicitation request before dispatch.
        requests.push(request.params as JsonObject);
        fake.events.push(...APPROVED_AND_ANSWERED);

        return { action: "accept" };
      },
    );

    const result = await client.callTool({
      name: "launch_hello_demo",
      arguments: { prompt: "Hi" },
    });

    expect(requests).toEqual([
      expect.objectContaining({ mode: "url", url: APPROVAL_URL }),
    ]);
    expect(result.isError).toBeFalsy();
    expect(parseText(result)).toEqual({
      sessionId: SESSION_ID,
      answer: "Hello world",
    });
  }, 20_000);

  it("returns a non-error cancelled result when the approval is declined", async () => {
    const fake = fakeHarness(LAUNCH_EVENTS);
    const { client } = await connectModernClient(fake);
    client.setRequestHandler(
      "elicitation/create",
      async (): Promise<ElicitResult> => ({ action: "decline" }),
    );

    const result = await client.callTool({
      name: "launch_hello_demo",
      arguments: {},
    });

    expect(result.isError).toBeFalsy();
    expect(parseText(result)).toMatchObject({ cancelled: true });
    expect(
      await waitFor(
        () => fake.endSessions,
        (ids) => ids.length > 0,
      ),
    ).toEqual([SESSION_ID]);
  }, 20_000);

  // A client that dismissed or couldn't show the prompt answers `cancel`:
  // the run must survive and the link must reach the user as plain text.
  it("keeps the run and returns the link when the prompt is cancelled", async () => {
    const fake = fakeHarness(LAUNCH_EVENTS);
    const { client } = await connectModernClient(fake);
    client.setRequestHandler(
      "elicitation/create",
      async (): Promise<ElicitResult> => ({ action: "cancel" }),
    );

    const result = await client.callTool({
      name: "launch_hello_demo",
      arguments: {},
    });

    expect(result.isError).toBeFalsy();
    expect(result.structuredContent).toMatchObject({
      status: "input_required",
      approvalUrl: APPROVAL_URL,
    });
    expect(fake.endSessions).toEqual([]);
  }, 20_000);
});

describe("launch_hello_demo with plain results (no tasks, no URL elicitation)", () => {
  // What ChatGPT declares: the 2026-07-28 protocol, but neither the tasks
  // extension nor URL-mode elicitation.
  const plain: RpcOptions = {
    declareTasks: false,
    declareUrlElicitation: false,
  };

  async function callTool(
    server: TaskServer,
    name: string,
    args: JsonObject,
  ): Promise<JsonObject> {
    const reply = await server.rpc(
      "tools/call",
      { name, arguments: args },
      { name, ...plain },
    );

    expect(reply.error).toBeUndefined();
    expect(reply.result?.resultType).not.toBe("input_required");
    expect(reply.result?.resultType).not.toBe("task");

    // SAFETY: a successful tools/call returns a CallToolResult object.
    return reply.result as JsonObject;
  }

  function structured(result: JsonObject): JsonObject {
    // SAFETY: these tools always emit structuredContent alongside the text.
    return result.structuredContent as JsonObject;
  }

  it("returns the approval link and a runId as a plain result", async () => {
    const server = setupTaskServer(fakeHarness(LAUNCH_EVENTS).client);

    const result = await callTool(server, "launch_hello_demo", {});

    expect(result.isError).toBeFalsy();
    expect(structured(result)).toMatchObject({
      status: "input_required",
      approvalUrl: APPROVAL_URL,
    });
    expect(JSON.stringify(result.content)).toContain(APPROVAL_URL);
    expect(JSON.stringify(result.content)).toContain("get_run_status");
  }, 20_000);

  it("get_run_status returns the answer once the run is approved", async () => {
    const fake = fakeHarness(LAUNCH_EVENTS);
    const server = setupTaskServer(fake.client);
    const launched = await callTool(server, "launch_hello_demo", {});
    const runId = structured(launched).runId;

    expect(runId).toEqual(expect.any(String));
    fake.events.push(...APPROVED_AND_ANSWERED);

    // One call after approving is enough: it waits for the answer.
    const status = await callTool(server, "get_run_status", {
      runId: String(runId),
    });

    expect(status.isError).toBeFalsy();
    expect(structured(status)).toEqual({
      sessionId: SESSION_ID,
      answer: "Hello world",
    });
  }, 30_000);

  it("get_run_status rejects an unknown runId", async () => {
    const server = setupTaskServer(fakeHarness().client);

    const result = await callTool(server, "get_run_status", { runId: "nope" });

    expect(result.isError).toBe(true);
  });

  function platformWith(events: HarnessEvent[]): HarnessClient {
    return fakeHarness(events, {
      listSessions: () =>
        Effect.succeed([
          {
            id: "old-session",
            state: "SESSION_STATE_ENDED",
            conversationRef: "mcp-lost-run",
          },
        ]),
    }).client;
  }

  it("get_run_status replays an untracked run waiting for approval from the platform", async () => {
    const server = setupTaskServer(platformWith(LAUNCH_EVENTS));

    const result = await callTool(server, "get_run_status", {
      runId: "lost-run",
    });

    expect(result.isError).toBeFalsy();
    expect(structured(result)).toMatchObject({
      runId: "lost-run",
      status: "input_required",
      approvalUrl: APPROVAL_URL,
    });
  });

  it("get_run_status replays an untracked finished run's answer from the platform", async () => {
    const server = setupTaskServer(
      platformWith([...LAUNCH_EVENTS, ...APPROVED_AND_ANSWERED]),
    );

    const result = await callTool(server, "get_run_status", {
      runId: "lost-run",
    });

    expect(structured(result)).toEqual({
      sessionId: "old-session",
      answer: "Hello world",
    });
  });
});

describe("list_pending_approvals", () => {
  it("lists tracked and stale runs waiting for approval with their links", async () => {
    const staleUrl = "https://agentic.example.com/agentic/approve?run_id=stale";

    const sessionEvents = new Map<string, HarnessEvent[]>([
      [
        "stale-session",
        [
          event(1, {
            approvalRequired: {
              approvalUrl: staleUrl,
              expiresAt: "2026-10-16T16:30:00Z",
            },
          }),
        ],
      ],
      ["s1", LAUNCH_EVENTS],
    ]);

    let trackedRef = "";

    const fake = fakeHarness(LAUNCH_EVENTS, {
      createSession: (request) =>
        Effect.sync(() => {
          trackedRef = request.conversationRef;

          return { id: SESSION_ID };
        }),
      listEvents: (sessionId, afterSeq) =>
        Effect.succeed(
          (sessionEvents.get(sessionId) ?? []).filter(
            (e) => Number(e.seq) > afterSeq,
          ),
        ),
      listSessions: () =>
        Effect.succeed([
          {
            id: SESSION_ID,
            state: "SESSION_STATE_AWAITING_APPROVAL",
            conversationRef: trackedRef,
          },
          {
            id: "stale-session",
            state: "SESSION_STATE_AWAITING_APPROVAL",
            conversationRef: "mcp-lost-run",
          },
          {
            id: "running",
            state: "SESSION_STATE_RUNNING",
            conversationRef: "mcp-other",
          },
          {
            id: "not-ours",
            state: "SESSION_STATE_AWAITING_APPROVAL",
            conversationRef: "slack-thread",
          },
        ]),
    });

    const server = setupTaskServer(fake.client);
    const runId = await launch(server);
    await pollTask(server, runId, (task) => task.status === "input_required");

    const reply = await server.rpc(
      "tools/call",
      { name: "list_pending_approvals", arguments: {} },
      { name: "list_pending_approvals" },
    );

    // SAFETY: a successful tools/call returns a CallToolResult object.
    const result = reply.result as JsonObject;
    // SAFETY: the tool always emits structuredContent alongside the text.
    const structured = result.structuredContent as JsonObject;

    expect(result.isError).toBeFalsy();
    expect(structured.pending).toEqual([
      {
        sessionId: SESSION_ID,
        runId,
        approvalUrl: APPROVAL_URL,
        expiresAt: null,
        tracked: true,
      },
      {
        sessionId: "stale-session",
        runId: "lost-run",
        approvalUrl: staleUrl,
        expiresAt: "2026-10-16T16:30:00Z",
        tracked: false,
      },
    ]);
    expect(JSON.stringify(result.content)).toContain("stale");
    expect(JSON.stringify(result.content)).toContain(staleUrl);
  }, 20_000);

  it("says so when nothing is waiting", async () => {
    const server = setupTaskServer(fakeHarness().client);

    const reply = await server.rpc(
      "tools/call",
      { name: "list_pending_approvals", arguments: {} },
      { name: "list_pending_approvals" },
    );

    expect(JSON.stringify(reply.result?.content)).toContain("No AgentOps runs");
  });
});

describe("launch_hello_demo with form-only elicitation (ChatGPT)", () => {
  // What ChatGPT declares: `elicitation: {}` (form mode), no URL mode, no
  // tasks. It gets the plain link, never a form: dots draw forms ~1px wide.
  const form: RpcOptions = {
    declareTasks: false,
    declareUrlElicitation: false,
    declareFormElicitation: true,
  };

  it("returns the approval link as plain text, not a form", async () => {
    const fake = fakeHarness(LAUNCH_EVENTS);
    const server = setupTaskServer(fake.client);

    const reply = await server.rpc(
      "tools/call",
      { name: "launch_hello_demo", arguments: {} },
      { ...form, name: "launch_hello_demo" },
    );

    expect(reply.error).toBeUndefined();
    expect(reply.result?.resultType).not.toBe("input_required");
    expect(reply.result?.structuredContent).toMatchObject({
      status: "input_required",
      approvalUrl: APPROVAL_URL,
    });

    const text = JSON.stringify(reply.result?.content);
    expect(text).toContain(APPROVAL_URL);
    expect(text).toContain("get_run_status");
    expect(text).toContain("run.finished");

    // SAFETY: the link result always carries structuredContent with runId.
    const runId = (reply.result?.structuredContent as JsonObject).runId;
    fake.events.push(...APPROVED_AND_ANSWERED);

    const status = await server.rpc(
      "tools/call",
      { name: "get_run_status", arguments: { runId: String(runId) } },
      { ...form, name: "get_run_status" },
    );

    expect(status.result?.structuredContent).toMatchObject({
      answer: "Hello world",
    });
  }, 30_000);
});

describe("end_session", () => {
  it("ends the session and reports its id", async () => {
    const fake = fakeHarness();
    const { client } = await connectModernClient(fake);

    const result = await client.callTool({
      name: "end_session",
      arguments: { sessionId: SESSION_ID },
    });

    expect(result.isError).toBeFalsy();
    expect(parseText(result)).toEqual({ ended: SESSION_ID });
    expect(fake.endSessions).toEqual([SESSION_ID]);
  });
});

describe("signState and verifyState", () => {
  it("round-trips a task id", () => {
    const state = signState("task-123");

    expect(verifyState(state)).toBe("task-123");
  });

  it("rejects a state whose task id was swapped", () => {
    const state = signState("task-123");
    const forged = state.replace("task-123", "task-999");

    expect(verifyState(forged)).toBeUndefined();
  });

  it("rejects a state whose signature was altered", () => {
    const state = signState("task-123");
    const last = state.slice(-1);
    const tampered = `${state.slice(0, -1)}${last === "A" ? "B" : "A"}`;

    expect(verifyState(tampered)).toBeUndefined();
  });

  it("rejects a state without a signature", () => {
    expect(verifyState("task-123")).toBeUndefined();
    expect(verifyState(".")).toBeUndefined();
  });
});

describe("userFromHeaders", () => {
  it("extracts the email from the assertion payload", () => {
    const headers = new Headers({
      "x-pomerium-jwt-assertion": assertionFor("alice@example.com"),
    });

    expect(userFromHeaders(headers)).toBe("alice@example.com");
  });

  it("falls back to the sub claim when there is no email", () => {
    const headers = new Headers({
      "x-pomerium-jwt-assertion": `${base64url({ alg: "none" })}.${base64url({ sub: "user-42" })}.sig`,
    });

    expect(userFromHeaders(headers)).toBe("user-42");
  });

  it("returns undefined without the header", () => {
    expect(userFromHeaders(new Headers())).toBeUndefined();
    expect(userFromHeaders(undefined)).toBeUndefined();
  });

  it("returns undefined for a malformed assertion", () => {
    const notAJwt = new Headers({
      "x-pomerium-jwt-assertion": "not-a-jwt",
    });

    const badPayload = new Headers({
      "x-pomerium-jwt-assertion": "abc.!!!.sig",
    });

    expect(userFromHeaders(notAJwt)).toBeUndefined();
    expect(userFromHeaders(badPayload)).toBeUndefined();
  });
});
