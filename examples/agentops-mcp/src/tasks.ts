import { randomUUID } from "node:crypto";
import type {
  CallToolResult,
  InputRequests,
  McpServer,
  ServerContext,
} from "@modelcontextprotocol/server";
import { ProtocolError, ProtocolErrorCode } from "@modelcontextprotocol/server";
import { Result, Schema } from "effect";
import { toMcpSchema } from "./lib/mcp-schema.ts";
import { userFromContext } from "./identity.ts";

/**
 * The MCP Tasks extension (SEP-2663, `io.modelcontextprotocol/tasks`).
 *
 * Protocol revision 2026-07-28 removed tasks from core and the SDK ships no
 * task runtime, so this module implements the extension: an in-memory task
 * store (the server runs as one replica) plus the `tasks/get`,
 * `tasks/update` and `tasks/cancel` handlers. Tools create tasks through
 * {@link TaskStore.create} and return {@link createTaskResult}.
 */

export const TASKS_EXTENSION = "io.modelcontextprotocol/tasks";

const CLIENT_CAPABILITIES_META_KEY =
  "io.modelcontextprotocol/clientCapabilities";

export type TaskStatus =
  | "working"
  | "input_required"
  | "completed"
  | "cancelled"
  | "failed";

const TERMINAL: ReadonlySet<TaskStatus> = new Set([
  "completed",
  "cancelled",
  "failed",
]);

export function isTerminal(status: TaskStatus): boolean {
  return TERMINAL.has(status);
}

/** `inputResponses` as the SDK lifts them off a request (untrusted). */
export type TaskInputResponses = NonNullable<
  ServerContext["mcpReq"]["inputResponses"]
>;

export type TaskRecord = {
  taskId: string;
  /** The Pomerium user who created the task; only they can read it. */
  owner: string | undefined;
  status: TaskStatus;
  statusMessage?: string;
  createdAt: string;
  lastUpdatedAt: string;
  ttlMs: number;
  pollIntervalMs: number;
  inputRequests?: InputRequests;
  /** The AgentOps session behind the task, once created. */
  sessionId?: string;
  /** The Pomerium consent page for the current approval, once known. */
  approvalUrl?: string;
  result?: CallToolResult;
  /** Set by the task's driver: receives `tasks/update` inputResponses. */
  onInputResponses?: (responses: TaskInputResponses) => void;
  /** Set by the task's driver: called on `tasks/cancel`. */
  onCancel?: () => void;
};

type TaskPatch = Partial<
  Pick<
    TaskRecord,
    | "status"
    | "statusMessage"
    | "inputRequests"
    | "sessionId"
    | "approvalUrl"
    | "result"
  >
>;

const DEFAULT_TTL_MS = 60 * 60 * 1000;
const DEFAULT_POLL_INTERVAL_MS = 2000;

export class TaskStore {
  #tasks = new Map<string, TaskRecord>();
  #waiters = new Map<string, Set<() => void>>();

  create(owner: string | undefined): TaskRecord {
    this.#sweep();
    const now = new Date().toISOString();

    const record: TaskRecord = {
      taskId: randomUUID(),
      owner,
      status: "working",
      statusMessage: "Starting",
      createdAt: now,
      lastUpdatedAt: now,
      ttlMs: DEFAULT_TTL_MS,
      pollIntervalMs: DEFAULT_POLL_INTERVAL_MS,
    };

    this.#tasks.set(record.taskId, record);

    return record;
  }

  /** A task visible to `owner`. Tasks are scoped to the user who made them. */
  get(taskId: string, owner: string | undefined): TaskRecord | undefined {
    const record = this.#tasks.get(taskId);

    return record && record.owner === owner ? record : undefined;
  }

  /** The tasks visible to `owner`, newest first. */
  list(owner: string | undefined): TaskRecord[] {
    this.#sweep();

    return [...this.#tasks.values()]
      .filter((record) => record.owner === owner)
      .reverse();
  }

  update(taskId: string, patch: TaskPatch): void {
    const record = this.#tasks.get(taskId);

    // A terminal task never changes again (e.g. a late event after cancel).
    if (!record || isTerminal(record.status)) return;

    Object.assign(record, patch, { lastUpdatedAt: new Date().toISOString() });

    if (patch.status && patch.status !== "input_required") {
      delete record.inputRequests;
    }

    for (const wake of this.#waiters.get(taskId) ?? []) wake();
  }

  /** Resolves once `predicate` holds for the task, or after `timeoutMs`. */
  waitFor(
    taskId: string,
    predicate: (record: TaskRecord) => boolean,
    timeoutMs: number,
  ): Promise<TaskRecord | undefined> {
    return new Promise((resolve) => {
      const waiters = this.#waiters.get(taskId) ?? new Set();
      this.#waiters.set(taskId, waiters);

      const finish = () => {
        clearTimeout(timer);
        waiters.delete(check);
        resolve(this.#tasks.get(taskId));
      };

      const check = () => {
        const record = this.#tasks.get(taskId);

        if (!record || predicate(record)) finish();
      };

      const timer = setTimeout(finish, timeoutMs);

      waiters.add(check);
      check();
    });
  }

  #sweep(): void {
    const now = Date.now();

    for (const [id, record] of this.#tasks) {
      if (Date.parse(record.createdAt) + record.ttlMs < now) {
        this.#tasks.delete(id);
        this.#waiters.delete(id);
      }
    }
  }
}

/** The process-wide store, shared by every per-request server instance. */
export const taskStore = new TaskStore();

/** SEP-2663 `DetailedTask` on the wire. */
type WireTask = {
  taskId: string;
  status: TaskStatus;
  statusMessage?: string;
  createdAt: string;
  lastUpdatedAt: string;
  ttlMs: number;
  pollIntervalMs: number;
  inputRequests?: InputRequests;
  result?: CallToolResult;
};

/** The `Task` fields every task-shaped result carries. */
function taskFields(record: TaskRecord): WireTask {
  const task: WireTask = {
    taskId: record.taskId,
    status: record.status,
    createdAt: record.createdAt,
    lastUpdatedAt: record.lastUpdatedAt,
    ttlMs: record.ttlMs,
    pollIntervalMs: record.pollIntervalMs,
  };

  if (record.statusMessage) task.statusMessage = record.statusMessage;

  return task;
}

/** `DetailedTask`: the task plus its status-specific payload. */
export function detailedTask(record: TaskRecord): WireTask {
  const task = taskFields(record);

  if (record.status === "input_required") {
    task.inputRequests = record.inputRequests ?? {};
  }

  if (record.status === "completed" && record.result) {
    task.result = record.result;
  }

  return task;
}

/** `CreateTaskResult`, returned from `tools/call` in place of a CallToolResult. */
export function createTaskResult(record: TaskRecord) {
  return { resultType: "task" as const, ...taskFields(record) };
}

const CLIENT_INFO_META_KEY = "io.modelcontextprotocol/clientInfo";

const RequestEnvelope = Schema.Struct({
  [CLIENT_INFO_META_KEY]: Schema.optional(
    Schema.Struct({ name: Schema.optional(Schema.String) }),
  ),
  [CLIENT_CAPABILITIES_META_KEY]: Schema.optional(
    Schema.Struct({
      elicitation: Schema.optional(
        Schema.Struct({ url: Schema.optional(Schema.Unknown) }),
      ),
      extensions: Schema.optional(
        Schema.Struct({ [TASKS_EXTENSION]: Schema.optional(Schema.Unknown) }),
      ),
    }),
  ),
});

/** What the client declared on this request (2026-07-28 `_meta` envelope). */
export type ClientSupport = {
  /** `clientInfo.name`, for logs. */
  name: string;
  /** The tasks extension (`io.modelcontextprotocol/tasks`). */
  tasks: boolean;
  /** URL-mode elicitation (`elicitation.url`). */
  urlElicitation: boolean;
  /** Form-mode elicitation (any `elicitation` declaration; `{}` means form). */
  formElicitation: boolean;
};

/**
 * Reads the client's declared support from the request envelope. A request
 * without one (a 2025-era client) supports neither, so it gets plain results.
 */
export function clientSupport(ctx: ServerContext): ClientSupport {
  const envelope = Schema.decodeUnknownResult(RequestEnvelope)(
    ctx.mcpReq.envelope,
  );

  if (Result.isFailure(envelope)) {
    return {
      name: "unknown",
      tasks: false,
      urlElicitation: false,
      formElicitation: false,
    };
  }

  const capabilities = envelope.success[CLIENT_CAPABILITIES_META_KEY];

  return {
    name: envelope.success[CLIENT_INFO_META_KEY]?.name ?? "unknown",
    tasks: capabilities?.extensions?.[TASKS_EXTENSION] !== undefined,
    urlElicitation: capabilities?.elicitation?.url !== undefined,
    formElicitation: capabilities?.elicitation !== undefined,
  };
}

const taskIdParams = toMcpSchema(Schema.Struct({ taskId: Schema.String }));

function unknownTask(taskId: string): ProtocolError {
  return new ProtocolError(
    ProtocolErrorCode.InvalidParams,
    `Unknown task: ${taskId}`,
  );
}

/** Advertises the extension and serves `tasks/get|update|cancel`. */
export function registerTasks(
  server: McpServer,
  store: TaskStore = taskStore,
): void {
  server.server.registerCapabilities({
    extensions: { [TASKS_EXTENSION]: {} },
  });

  server.server.setRequestHandler(
    "tasks/get",
    { params: taskIdParams },
    async ({ taskId }, ctx) => {
      const record = store.get(taskId, userFromContext(ctx));

      if (!record) throw unknownTask(taskId);

      return { resultType: "complete", ...detailedTask(record) };
    },
  );

  server.server.setRequestHandler(
    "tasks/update",
    // The SDK lifts `inputResponses` off every request as MRTR wire material
    // before validating params, so it arrives on the context, not in params.
    { params: taskIdParams },
    async ({ taskId }, ctx) => {
      const record = store.get(taskId, userFromContext(ctx));

      if (!record) throw unknownTask(taskId);
      record.onInputResponses?.(ctx.mcpReq.inputResponses ?? {});

      return { resultType: "complete" };
    },
  );

  server.server.setRequestHandler(
    "tasks/cancel",
    { params: taskIdParams },
    async ({ taskId }, ctx) => {
      const record = store.get(taskId, userFromContext(ctx));

      if (!record) throw unknownTask(taskId);
      record.onCancel?.();

      return { resultType: "complete" };
    },
  );
}
