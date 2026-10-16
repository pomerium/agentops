import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import type {
  CallToolResult,
  InputRequiredResult,
  InputRequests,
  McpServer,
  ServerContext,
} from "@modelcontextprotocol/server";
import { inputRequired } from "@modelcontextprotocol/server";
import { Effect, Schema } from "effect";
import { getConfig } from "./config.ts";
import { replayRun, runDemoSession, SESSION_REF_PREFIX } from "./demo-run.ts";
import { getHarnessClient, type HarnessClient } from "./harness.ts";
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

export type ToolDeps = {
  harness: () => HarnessClient;
  store: TaskStore;
  template: string;
};

const defaultDeps: ToolDeps = {
  harness: getHarnessClient,
  store: taskStore,
  get template() {
    return getConfig().DEMO_TEMPLATE;
  },
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
        "The user must approve the run on Pomerium's consent page. Clients with the " +
        "MCP Tasks extension get a task whose approval arrives as a URL elicitation. " +
        "Other clients get the approval link and a runId in the result: show the user " +
        "the link, then call get_run_status with the runId until the run completes.",
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
        "List every AgentOps run this server started that is still waiting for " +
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

      record.onInputResponses?.(ctx.mcpReq.inputResponses ?? {});

      return yield* awaitSync(
        deps.store,
        record,
        owner,
        !clientSupport(ctx).urlElicitation,
      );
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

    const mode = client.tasks
      ? "task"
      : client.urlElicitation
        ? "mrtr-url-elicitation"
        : client.formElicitation
          ? "mrtr-form-elicitation"
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

    // Tasks extension: return the task; the approval is a URL elicitation.
    if (client.tasks) return createTaskResult(record);

    // URL elicitation without tasks: drive the same task over MRTR.
    if (client.urlElicitation) {
      return yield* awaitSync(deps.store, record, owner, false);
    }

    // Form elicitation only (e.g. ChatGPT): the same MRTR flow, with the
    // approval link in a form the user confirms once they have approved.
    if (client.formElicitation) {
      return yield* awaitSync(deps.store, record, owner, true);
    }

    // Neither (e.g. ChatGPT): answer with the approval link and a runId, and
    // let the model follow up with get_run_status.
    // A fresh launch has shown no link yet, even if the run already needs
    // approval, so return as soon as the link exists.
    return yield* awaitStatus(deps.store, record.taskId, owner, false);
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
  asForm: boolean,
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
          inputRequests: asForm
            ? approvalForms(latest.inputRequests, latest)
            : latest.inputRequests,
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

      return yield* replayFromPlatform(args.runId, deps.harness());
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
        session.conversationRef?.startsWith(SESSION_REF_PREFIX),
    );

    const pending = yield* Effect.forEach(
      waiting,
      (session) =>
        harness.listEvents(session.id, 0).pipe(
          Effect.map((events): PendingApproval => {
            const approval = events
              .filter((e) => e.approvalRequired)
              .at(-1)?.approvalRequired;

            const runId = (session.conversationRef ?? "").slice(
              SESSION_REF_PREFIX.length,
            );

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
  harness: HarnessClient,
): Effect.Effect<CallToolResult> {
  return Effect.gen(function* () {
    const sessions = yield* harness.listSessions(false);

    const session = sessions.find(
      (s) => s.conversationRef === `${SESSION_REF_PREFIX}${runId}`,
    );

    if (!session) {
      return createErrorResult({ error: `Unknown run: ${runId}` });
    }

    const events = yield* harness.listEvents(session.id, 0);

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

/**
 * The approval as a form-mode elicitation, for clients without URL mode: the
 * message carries the Pomerium link, and the form only confirms that the user
 * approved there. The approval itself still happens only on Pomerium's page;
 * accepting the form just makes this call wait for the agent's answer, and
 * declining it ends the run. Keys match the URL requests, so the run driver
 * handles the response the same way.
 */
function approvalForms(
  requests: InputRequests,
  record: TaskRecord,
): InputRequests {
  const forms: InputRequests = {};

  for (const key of Object.keys(requests)) {
    forms[key] = inputRequired.elicit({
      message:
        `Approve this AgentOps run on Pomerium's consent page: ${record.approvalUrl ?? "(link unavailable)"}\n` +
        "Sign in, review what the agent asks for, and click Approve there. " +
        "Then confirm here, and the agent's answer will follow.",
      requestedSchema: {
        type: "object",
        properties: {
          approved: {
            type: "boolean",
            title: "I approved the run in Pomerium",
            default: true,
          },
        },
      },
    });
  }

  return forms;
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

  return deps
    .harness()
    .endSession(args.sessionId)
    .pipe(
      Effect.tap(() => logger.info({ toolName, requestId }, "Tool executed")),
      Effect.as(createTextResult({ ended: args.sessionId })),
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
