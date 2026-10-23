import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import type {
  CallToolResult,
  InputRequiredResult,
  McpServer,
  ServerContext,
} from "@modelcontextprotocol/server";
import { inputRequired, inputResponse } from "@modelcontextprotocol/server";
import { Effect, Schema } from "effect";
import { getConfig } from "./config.ts";
import {
  ownsSession,
  replayRun,
  runDemoSession,
  runIdFromRef,
  sessionRef,
} from "./demo-run.ts";
import { getEventHub, RUN_APPROVAL_REQUIRED, RUN_FINISHED } from "./events.ts";
import {
  getHarnessClient,
  listAllEvents,
  type HarnessClient,
} from "./harness.ts";
import { userFromContext } from "./identity.ts";
import type { JsonObject } from "./lib/json.ts";
import { toMcpSchema } from "./lib/mcp-schema.ts";
import {
  createErrorResult,
  createTextResult,
  runMcpEffect,
} from "./lib/utils.ts";
import { logger } from "./logger.ts";
import {
  clientSupport,
  createTaskResult,
  isTerminal,
  taskStore,
  type TaskInputResponses,
  type TaskRecord,
  type TaskStore,
} from "./tasks.ts";

const DEFAULT_DEMO_PROMPT = "Hello! Did the install work?";

/** How long a non-task (MRTR) call waits before answering the client. */
const SYNC_WAIT_MS = 90_000;

/**
 * How long a plain call (no tasks, no URL elicitation) or get_run_status waits
 * for the run to move on. Kept under common client tool-call timeouts.
 */
const STATUS_WAIT_MS = 45_000;

const LaunchInputSchema = toMcpSchema(
  Schema.Struct({
    prompt: Schema.optional(
      Schema.String.annotate({
        description: `The prompt for the hello agent. Default: "${DEFAULT_DEMO_PROMPT}"`,
      }),
    ),
  }),
);

const RunIdInputSchema = toMcpSchema(
  Schema.Struct({
    runId: Schema.String.annotate({
      description: "The runId from launch_hello_demo's result",
    }),
  }),
);

const SessionIdInputSchema = toMcpSchema(
  Schema.Struct({
    sessionId: Schema.String.annotate({
      description: "The AgentOps session id, from launch_hello_demo's result",
    }),
  }),
);

/** A CreateTaskResult: a tools/call result shape from the tasks extension. */
type ToolCallResult =
  | CallToolResult
  | InputRequiredResult
  | ReturnType<typeof createTaskResult>;

/** Whether a user's MCP Events subscriptions cover a run. */
export type RunEvents = {
  covers(owner: string | undefined, runId: string): boolean;
};

export type ToolDeps = {
  harness: () => HarnessClient;
  store: TaskStore;
  template: string;
  /** MCP Events; without it, launches use the fallbacks only. */
  events?: () => RunEvents;
};

const defaultDeps: ToolDeps = {
  harness: getHarnessClient,
  store: taskStore,
  get template() {
    return getConfig().DEMO_TEMPLATE;
  },
  events: getEventHub,
};

/**
 * Registers all MCP tools on the server.
 * Called once per request from getServer() in src/app.ts (the 2026-07-28
 * spec serves each request from a fresh instance — there is no per-connection
 * session to register tools once for).
 */
export function registerTools(
  server: McpServer,
  deps: ToolDeps = defaultDeps,
): void {
  server.registerTool(
    "launch_hello_demo",
    {
      title: "Launch the AgentOps hello demo",
      description:
        "Start a run of the AgentOps `hello` demo agent in a Kubernetes sandbox. " +
        "The user must approve the run on Pomerium's consent page. Preferred: " +
        `subscribe to the MCP events ${RUN_APPROVAL_REQUIRED} and ${RUN_FINISHED} ` +
        "first; the result then has the runId and approval link, and the answer " +
        "arrives as an event, so don't poll. Without a subscription, clients with " +
        "the MCP Tasks extension get a task whose approval arrives as a URL " +
        "elicitation, and other clients get the approval link and a runId: show the " +
        "user the link, then call get_run_status with the runId until the run completes.",
      inputSchema: LaunchInputSchema,
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: false,
        openWorldHint: true,
      },
    },
    // SAFETY: the handler returns a CreateTaskResult only when the client
    // declared the tasks extension (clientSupport), where SEP-2663 makes
    // it a valid tools/call result; the SDK's handler type predates it.
    ((args: { prompt?: string }, ctx: ServerContext) =>
      runMcpEffect(launchHelloDemo(args, ctx, deps))) as never,
  );

  server.registerTool(
    "get_run_status",
    {
      title: "Get AgentOps run status",
      description:
        "Wait briefly for an AgentOps run started by launch_hello_demo to move on, then " +
        "report its status: the approval link while it waits for approval, progress " +
        "while the agent works, and the agent's answer when it completes. Call it " +
        "again until the status is completed or cancelled.",
      inputSchema: RunIdInputSchema,
      annotations: {
        readOnlyHint: true,
        idempotentHint: false,
        openWorldHint: true,
      },
    },
    (args, ctx) => runMcpEffect(getRunStatus(args, ctx, deps)),
  );

  server.registerTool(
    "list_pending_approvals",
    {
      title: "List AgentOps runs waiting for approval",
      description:
        "List the caller's AgentOps runs that are still waiting for " +
        "approval, with its Pomerium approval link and expiry, including stale runs " +
        "that no client is following any more (for example after a restart). Approve " +
        "a run on its link, or end it with end_session to free the approval quota.",
      annotations: {
        readOnlyHint: true,
        idempotentHint: true,
        openWorldHint: true,
      },
    },
    (ctx) => runMcpEffect(listPendingApprovals(ctx, deps)),
  );

  server.registerTool(
    "list_agents",
    {
      title: "List AgentOps agents",
      description: "List the AgentOps agent templates this server may run",
      annotations: {
        readOnlyHint: true,
        idempotentHint: true,
        openWorldHint: true,
      },
    },
    (ctx) => runMcpEffect(listAgents(ctx, deps)),
  );

  server.registerTool(
    "end_session",
    {
      title: "End an AgentOps session",
      description:
        "End an AgentOps agent session and free its sandbox pod. Cancelling a " +
        "launch_hello_demo task does the same.",
      inputSchema: SessionIdInputSchema,
      annotations: {
        readOnlyHint: false,
        destructiveHint: true,
        idempotentHint: true,
        openWorldHint: true,
      },
    },
    (args, ctx) => runMcpEffect(endSession(args, ctx, deps)),
  );
}

function launchHelloDemo(
  args: { prompt?: string },
  ctx: ServerContext,
  deps: ToolDeps,
): Effect.Effect<ToolCallResult> {
  return Effect.gen(function* () {
    const toolName = "launch_hello_demo";
    const requestId = ctx.mcpReq.id;
    const owner = userFromContext(ctx);

    // Round 2+ of the MRTR fallback: the client answered our input requests.
    const state = ctx.mcpReq.requestState<string>();

    if (state !== undefined) {
      const taskId = verifyState(state);
      const record = taskId ? deps.store.get(taskId, owner) : undefined;

      if (!record) {
        return createErrorResult({ error: "Unknown or expired request state" });
      }

      const responses = ctx.mcpReq.inputResponses ?? {};
      record.onInputResponses?.(responses);

      // The prompt was dismissed or couldn't be shown: asking again would
      // loop, so hand over the link and runId as plain text instead.
      if (promptCancelled(responses)) {
        yield* logger.info(
          { toolName, requestId, taskId: record.taskId, mode: "link" },
          "Approval prompt cancelled; falling back to the link",
        );

        return runStatusResult(record);
      }

      return yield* awaitSync(deps.store, record, owner);
    }

    const record = deps.store.create(owner);
    Effect.runFork(
      runDemoSession({
        store: deps.store,
        harness: deps.harness(),
        taskId: record.taskId,
        template: deps.template,
        prompt: args.prompt ?? DEFAULT_DEMO_PROMPT,
        owner,
      }),
    );

    const client = clientSupport(ctx);
    const viaEvents = deps.events?.().covers(owner, record.taskId) ?? false;

    // MCP Events first, then the fallbacks, from richest to plainest.
    const mode = viaEvents
      ? "events"
      : client.tasks
        ? "task"
        : client.urlElicitation
          ? "mrtr-url-elicitation"
          : "link";

    yield* logger.info(
      {
        toolName,
        requestId,
        taskId: record.taskId,
        client: client.name,
        tasks: client.tasks,
        urlElicitation: client.urlElicitation,
        mode,
        // The client's full declaration (no secrets), to spot what each client
        // supports, e.g. an MCP Apps UI extension.
        envelope: JSON.stringify(ctx.mcpReq.envelope ?? null),
      },
      "Run started",
    );

    // The user's event subscriptions cover this run: hand back the link as
    // soon as it exists; the approval and the answer follow as events.
    if (viaEvents) {
      return yield* Effect.promise(() =>
        deps.store.waitFor(
          record.taskId,
          (r) =>
            isTerminal(r.status) ||
            (r.status === "input_required" && r.approvalUrl !== undefined),
          STATUS_WAIT_MS,
        ),
      ).pipe(
        Effect.map((current) =>
          current
            ? eventsLaunchResult(current)
            : createErrorResult({ error: "The run expired" }),
        ),
      );
    }

    // Tasks extension: return the task; the approval is a URL elicitation.
    if (client.tasks) return createTaskResult(record);

    // URL elicitation without tasks: drive the same task over MRTR.
    if (client.urlElicitation) {
      return yield* awaitSync(deps.store, record, owner);
    }

    // Neither (e.g. ChatGPT): answer with the approval link and a runId, and
    // let the model follow up with get_run_status. Form-only clients land
    // here too: the client draws a form however it likes (in ChatGPT dots, a
    // ~1px card or nothing), while plain text renders as a normal link.
    // A fresh launch has shown no link yet, even if the run already needs
    // approval, so return as soon as the link exists.
    return yield* awaitStatus(deps.store, record.taskId, owner, false);
  });
}

/** Whether the client answered an approval prompt with `cancel`. */
function promptCancelled(responses: TaskInputResponses): boolean {
  return Object.keys(responses).some((key) => {
    const response = inputResponse(responses, key);

    return response.kind === "elicit" && response.action === "cancel";
  });
}

/**
 * MRTR fallback: wait until the task needs input (return input_required with
 * signed state) or finishes (return its result).
 */
function awaitSync(
  store: TaskStore,
  record: TaskRecord,
  owner: string | undefined,
): Effect.Effect<CallToolResult | InputRequiredResult> {
  return Effect.promise(() =>
    store.waitFor(
      record.taskId,
      (r) =>
        isTerminal(r.status) ||
        (r.status === "input_required" && r.inputRequests !== undefined),
      SYNC_WAIT_MS,
    ),
  ).pipe(
    Effect.map((current) => {
      const latest = current ?? store.get(record.taskId, owner);

      if (!latest) return createErrorResult({ error: "The task expired" });

      if (latest.status === "input_required" && latest.inputRequests) {
        return inputRequired({
          inputRequests: latest.inputRequests,
          requestState: signState(latest.taskId),
        });
      }

      if (latest.status === "completed" && latest.result) return latest.result;

      if (latest.status === "cancelled") {
        return createTextResult({
          cancelled: true,
          reason: latest.statusMessage ?? "cancelled",
        });
      }

      // Still working after SYNC_WAIT_MS: report where it is.
      return createTextResult({
        taskId: latest.taskId,
        status: latest.status,
        statusMessage: latest.statusMessage ?? null,
      });
    }),
  );
}

function getRunStatus(
  args: { runId: string },
  ctx: ServerContext,
  deps: ToolDeps,
): Effect.Effect<CallToolResult> {
  return Effect.gen(function* () {
    const toolName = "get_run_status";
    const requestId = ctx.mcpReq.id;
    const owner = userFromContext(ctx);

    const record = deps.store.get(args.runId, owner);

    // Not tracked by this process: rebuild it from the platform's events.
    if (!record) {
      yield* logger.info(
        { toolName, requestId, taskId: args.runId },
        "Run not in memory; replaying from the platform",
      );

      return yield* replayFromPlatform(args.runId, owner, deps.harness());
    }

    yield* logger.info(
      { toolName, requestId, taskId: args.runId },
      "Tool executed",
    );

    // The caller has seen the link if the run is waiting for approval: wait
    // for it to move on rather than repeating the link at once.
    return yield* awaitStatus(
      deps.store,
      args.runId,
      owner,
      record.status === "input_required",
    );
  });
}

/**
 * Waits up to STATUS_WAIT_MS for the run to change state (or to need
 * approval), then reports it as a plain tool result any client can show.
 */
function awaitStatus(
  store: TaskStore,
  taskId: string,
  owner: string | undefined,
  waitingForApproval: boolean,
): Effect.Effect<CallToolResult> {
  // Return as soon as the run finishes, or, on a fresh launch, as soon as it
  // needs approval (show the link). A caller that has seen the link waits for
  // the answer itself, so one call after approving is enough. Otherwise report
  // where the run is after the wait.
  const somethingNew = (r: TaskRecord) =>
    isTerminal(r.status) ||
    (!waitingForApproval &&
      r.status === "input_required" &&
      r.approvalUrl !== undefined);

  return Effect.promise(() =>
    store.waitFor(taskId, somethingNew, STATUS_WAIT_MS),
  ).pipe(
    Effect.map((current) =>
      current
        ? runStatusResult(current)
        : createErrorResult({ error: `Unknown or expired run: ${taskId}` }),
    ),
  );
}

/** A launch whose progress arrives as MCP events. */
export function eventsLaunchResult(record: TaskRecord): CallToolResult {
  if (isTerminal(record.status)) return runStatusResult(record);

  const runId = record.taskId;

  const lines = [
    `Run ${runId} started. Its updates arrive as MCP events: ${RUN_APPROVAL_REQUIRED} ` +
      `with the approval link, then ${RUN_FINISHED} with the agent's answer. ` +
      "No need to call get_run_status.",
  ];

  if (record.approvalUrl) {
    lines.push(
      "Ask the user to open this link, sign in, review the run and click Approve:",
      record.approvalUrl,
    );
  }

  return statusResult(lines.join("\n"), {
    runId,
    status: record.status,
    statusMessage: record.statusMessage ?? null,
    approvalUrl: record.approvalUrl ?? null,
    delivery: "events",
  });
}

/** A run's state as text for the model plus structuredContent. */
export function runStatusResult(record: TaskRecord): CallToolResult {
  const runId = record.taskId;

  if (record.status === "completed" && record.result) return record.result;

  if (record.status === "input_required" && record.approvalUrl) {
    return statusResult(
      [
        `Approval needed: ${record.statusMessage ?? "waiting for approval"}.`,
        `Ask the user to open this link, sign in, review the run and click Approve:`,
        record.approvalUrl,
        `Once the user says they approved, call get_run_status once with runId "${runId}": it waits up to 45 seconds for the agent's answer.`,
        `If you support MCP Events, subscribe to ${RUN_FINISHED} with run_id "${runId}" instead, and the answer is delivered when the run finishes.`,
      ].join("\n"),
      {
        runId,
        status: record.status,
        statusMessage: record.statusMessage ?? null,
        approvalUrl: record.approvalUrl,
      },
    );
  }

  const next = isTerminal(record.status)
    ? ""
    : ` Call get_run_status with runId "${runId}" again to keep waiting.`;

  return statusResult(
    `Run ${runId} is ${record.status}: ${record.statusMessage ?? ""}.${next}`,
    {
      runId,
      status: record.status,
      statusMessage: record.statusMessage ?? null,
    },
  );
}

function statusResult(text: string, data: JsonObject): CallToolResult {
  return { ...createTextResult(data), content: [{ type: "text", text }] };
}

type PendingApproval = {
  sessionId: string;
  runId: string;
  approvalUrl: string | null;
  expiresAt: string | null;
  /** Whether this server still follows the run (get_run_status works). */
  tracked: boolean;
};

/**
 * Pending approvals come from the platform, not the in-memory task store, so
 * runs this process no longer tracks (after a restart, or a client that gave
 * up) still show up and can be approved or ended.
 */
function listPendingApprovals(
  ctx: ServerContext,
  deps: ToolDeps,
): Effect.Effect<CallToolResult> {
  const toolName = "list_pending_approvals";
  const requestId = ctx.mcpReq.id;
  const owner = userFromContext(ctx);
  const harness = deps.harness();

  return Effect.gen(function* () {
    const sessions = yield* harness.listSessions(true);

    const waiting = sessions.filter(
      (session) =>
        session.state === "SESSION_STATE_AWAITING_APPROVAL" &&
        ownsSession(session.conversationRef, owner),
    );

    const pending = yield* Effect.forEach(
      waiting,
      (session) =>
        listAllEvents(harness, session.id).pipe(
          Effect.map((events): PendingApproval => {
            const approval = events
              .filter((e) => e.approvalRequired)
              .at(-1)?.approvalRequired;

            const runId = runIdFromRef(session.conversationRef ?? "");

            return {
              sessionId: session.id,
              runId,
              approvalUrl: approval?.approvalUrl ?? null,
              expiresAt: approval?.expiresAt ?? null,
              tracked: deps.store.get(runId, owner) !== undefined,
            };
          }),
        ),
      { concurrency: 4 },
    );

    yield* logger.info(
      { toolName, requestId, pending: pending.length },
      "Tool executed",
    );

    return statusResult(pendingApprovalsText(pending), {
      pending: pending.map((p) => ({ ...p })),
    });
  }).pipe(
    Effect.catch((error) =>
      logger
        .error(
          { toolName, requestId, error: error.message },
          "Tool execution failed",
        )
        .pipe(Effect.as(createErrorResult({ error: error.message }))),
    ),
  );
}

function pendingApprovalsText(pending: PendingApproval[]): string {
  if (pending.length === 0) return "No AgentOps runs are waiting for approval.";

  const lines = [`${pending.length} AgentOps run(s) waiting for approval:`];

  for (const [index, p] of pending.entries()) {
    const expires = p.expiresAt ? `, expires ${p.expiresAt}` : "";

    const followUp = p.tracked
      ? `after approval, get_run_status with runId "${p.runId}" returns the answer`
      : "stale: no client is following it; approving still runs the agent, or end it with end_session";

    lines.push(
      "",
      `${index + 1}. session ${p.sessionId}${expires} (${followUp})`,
      p.approvalUrl ?? "(no approval link yet)",
    );
  }

  return lines.join("\n");
}

function replayFromPlatform(
  runId: string,
  owner: string | undefined,
  harness: HarnessClient,
): Effect.Effect<CallToolResult> {
  return Effect.gen(function* () {
    const sessions = yield* harness.listSessions(false);

    // Matching the owner's ref keeps other users' runs unknown.
    const session = sessions.find(
      (s) => s.conversationRef === sessionRef(runId, owner),
    );

    if (!session) {
      return createErrorResult({ error: `Unknown run: ${runId}` });
    }

    const events = yield* listAllEvents(harness, session.id);

    return runStatusResult(replayRun(runId, session.id, events));
  }).pipe(
    Effect.catch((error) =>
      Effect.succeed(
        createErrorResult({
          error: `Harness API ${error.method} failed: ${error.message}`,
        }),
      ),
    ),
  );
}

function listAgents(
  ctx: ServerContext,
  deps: ToolDeps,
): Effect.Effect<CallToolResult> {
  const toolName = "list_agents";
  const requestId = ctx.mcpReq.id;

  return deps
    .harness()
    .listTemplates()
    .pipe(
      Effect.tap(() => logger.info({ toolName, requestId }, "Tool executed")),
      Effect.map((templates) => createTextResult({ templates })),
      Effect.catch((error) =>
        logger
          .error(
            { toolName, requestId, error: error.message },
            "Tool execution failed",
          )
          .pipe(Effect.as(createErrorResult({ error: error.message }))),
      ),
    );
}

function endSession(
  args: { sessionId: string },
  ctx: ServerContext,
  deps: ToolDeps,
): Effect.Effect<CallToolResult> {
  const toolName = "end_session";
  const requestId = ctx.mcpReq.id;
  const owner = userFromContext(ctx);
  const harness = deps.harness();

  return Effect.gen(function* () {
    // Every session runs as this server's ServiceAccount: check that the
    // caller launched this one before ending it.
    const sessions = yield* harness.listSessions(false);

    const session = sessions.find(
      (s) => s.id === args.sessionId && ownsSession(s.conversationRef, owner),
    );

    if (!session) {
      return createErrorResult({ error: `Unknown session: ${args.sessionId}` });
    }

    yield* harness.endSession(session.id);
    yield* logger.info({ toolName, requestId }, "Tool executed");

    return createTextResult({ ended: args.sessionId });
  }).pipe(
    Effect.catch((error) =>
      logger
        .error(
          { toolName, requestId, error: error.message },
          "Tool execution failed",
        )
        .pipe(Effect.as(createErrorResult({ error: error.message }))),
    ),
  );
}

let stateKey: Buffer | undefined;

function getStateKey(): Buffer {
  stateKey ??= getConfig().STATE_SECRET
    ? Buffer.from(getConfig().STATE_SECRET)
    : randomBytes(32);

  return stateKey;
}

function mac(value: string): string {
  return createHmac("sha256", getStateKey()).update(value).digest("base64url");
}

/** requestState is client-held: sign the task id so it can't be swapped. */
export function signState(taskId: string): string {
  return `${taskId}.${mac(taskId)}`;
}

export function verifyState(state: string): string | undefined {
  const dot = state.lastIndexOf(".");

  if (dot <= 0) return undefined;
  const taskId = state.slice(0, dot);
  const given = Buffer.from(state.slice(dot + 1));
  const expected = Buffer.from(mac(taskId));

  return given.length === expected.length && timingSafeEqual(given, expected)
    ? taskId
    : undefined;
}
