import { Config as EffectConfig, ConfigProvider, Effect } from "effect";
import packageJson from "../package.json" with { type: "json" };

const configEffect = EffectConfig.all({
  PORT: EffectConfig.Number("PORT").pipe(EffectConfig.withDefault(3000)),
  NODE_ENV: EffectConfig.Literals(
    ["development", "production", "test"],
    "NODE_ENV",
  ).pipe(EffectConfig.withDefault("development")),
  SERVER_NAME: EffectConfig.String("SERVER_NAME").pipe(
    EffectConfig.withDefault(packageJson.name),
  ),
  SERVER_VERSION: EffectConfig.String("SERVER_VERSION").pipe(
    EffectConfig.withDefault(packageJson.version),
  ),
  LOG_LEVEL: EffectConfig.Literals(
    ["error", "warn", "info", "debug"],
    "LOG_LEVEL",
  ).pipe(EffectConfig.withDefault("info")),
  // AgentOps Harness API (Connect JSON), reached through Pomerium.
  HARNESS_API_URL: EffectConfig.String("HARNESS_API_URL").pipe(
    EffectConfig.withDefault("https://harness-api.agentops.example.com"),
  ),
  // Projected ServiceAccount token (audience pomerium-agentic-as). Re-read on
  // every call because the kubelet rotates it.
  HARNESS_TOKEN_FILE: EffectConfig.String("HARNESS_TOKEN_FILE").pipe(
    EffectConfig.withDefault("/var/run/secrets/agentops/token"),
  ),
  // Static token for local development; takes precedence over the file.
  HARNESS_TOKEN: EffectConfig.String("HARNESS_TOKEN").pipe(
    EffectConfig.withDefault(""),
  ),
  DEMO_TEMPLATE: EffectConfig.String("DEMO_TEMPLATE").pipe(
    EffectConfig.withDefault("hello"),
  ),
  // HMAC key for MRTR requestState. Empty: a random per-process key.
  STATE_SECRET: EffectConfig.String("STATE_SECRET").pipe(
    EffectConfig.withDefault(""),
  ),
});

export type Config = Effect.Success<typeof configEffect>;

const loadConfig = configEffect.parse(ConfigProvider.fromEnv());

let config: Config | undefined;

function failInvalidConfig(cause: unknown): never {
  process.stderr.write(`Invalid environment configuration: ${String(cause)}\n`);
  process.exit(1);
}

export function getConfig(): Config {
  if (!config) {
    try {
      config = Effect.runSync(loadConfig);
    } catch (error) {
      return failInvalidConfig(error);
    }
  }

  return config;
}

export function isProduction(): boolean {
  return getConfig().NODE_ENV === "production";
}

export function isDevelopment(): boolean {
  return getConfig().NODE_ENV === "development";
}
