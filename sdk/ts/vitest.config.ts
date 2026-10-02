import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    // Streaming cases wait on real reconnect backoff and real liveness windows,
    // so they are seconds long by nature rather than by accident.
    testTimeout: 30_000,
    hookTimeout: 30_000,
    // One stub, driven by every case. The suite is deliberately not parallel:
    // the scenarios are one-shot per key and a shared stub is the point.
    fileParallelism: false,
  },
});
