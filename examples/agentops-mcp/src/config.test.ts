import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import packageJson from "../package.json" with { type: "json" };

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

async function importConfig() {
  vi.resetModules();

  return import("./config.ts");
}

describe("configuration", () => {
  it("defaults the server name and version from package.json", async () => {
    vi.stubEnv("PORT", undefined);
    vi.stubEnv("NODE_ENV", undefined);
    vi.stubEnv("SERVER_NAME", undefined);
    vi.stubEnv("SERVER_VERSION", undefined);
    vi.stubEnv("LOG_LEVEL", undefined);
    vi.stubEnv("HARNESS_API_URL", undefined);
    vi.stubEnv("HARNESS_TOKEN_FILE", undefined);
    vi.stubEnv("HARNESS_TOKEN", undefined);
    vi.stubEnv("DEMO_TEMPLATE", undefined);
    vi.stubEnv("STATE_SECRET", undefined);
    vi.stubEnv("EVENTS_STORE_FILE", undefined);

    const { getConfig } = await importConfig();

    expect(getConfig()).toEqual({
      PORT: 3000,
      NODE_ENV: "development",
      SERVER_NAME: packageJson.name,
      SERVER_VERSION: packageJson.version,
      LOG_LEVEL: "info",
      HARNESS_API_URL: "https://harness-api.agentops.example.com",
      HARNESS_TOKEN_FILE: "/var/run/secrets/agentops/token",
      HARNESS_TOKEN: "",
      DEMO_TEMPLATE: "hello",
      STATE_SECRET: "",
      EVENTS_STORE_FILE: "",
    });
  });

  it("loads and parses environment values", async () => {
    vi.stubEnv("PORT", "4311");
    vi.stubEnv("NODE_ENV", "test");
    vi.stubEnv("SERVER_NAME", "custom-server");
    vi.stubEnv("SERVER_VERSION", "2.0.0");
    vi.stubEnv("LOG_LEVEL", "debug");

    const { getConfig } = await importConfig();

    expect(getConfig()).toEqual({
      PORT: 4311,
      NODE_ENV: "test",
      SERVER_NAME: "custom-server",
      SERVER_VERSION: "2.0.0",
      LOG_LEVEL: "debug",
      HARNESS_API_URL: "https://harness-api.agentops.example.com",
      HARNESS_TOKEN_FILE: "/var/run/secrets/agentops/token",
      HARNESS_TOKEN: "",
      DEMO_TEMPLATE: "hello",
      STATE_SECRET: "",
      EVENTS_STORE_FILE: "",
    });
  });

  it("exits when environment configuration is invalid", async () => {
    vi.stubEnv("PORT", "not-a-number");
    vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    vi.spyOn(process, "exit").mockImplementation((code) => {
      throw new Error(`process.exit(${code})`);
    });

    const { getConfig } = await importConfig();

    expect(() => getConfig()).toThrow("process.exit(1)");
    expect(process.exit).toHaveBeenCalledWith(1);
    expect(process.stderr.write).toHaveBeenCalledWith(
      expect.stringContaining("Invalid environment configuration"),
    );
  });
});
