import { describe, expect, it } from "vite-plus/test";
import { TaskStore } from "./tasks.ts";

describe("TaskStore", () => {
  it("keeps an active task past its ttl and sweeps a finished one", () => {
    const store = new TaskStore();
    const active = store.create(undefined);
    const done = store.create(undefined);
    store.update(done.taskId, { status: "completed" });

    const longAgo = new Date(Date.now() - 2 * active.ttlMs).toISOString();
    active.createdAt = longAgo;
    done.createdAt = longAgo;

    // create() sweeps expired tasks.
    store.create(undefined);

    expect(store.get(active.taskId, undefined)).toBeDefined();
    expect(store.get(done.taskId, undefined)).toBeUndefined();
  });
});
