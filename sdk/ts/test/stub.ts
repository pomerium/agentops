import { createNodeClient } from "../src/node.js";
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import { createInterface } from "node:readline";
import type { Interceptor } from "@connectrpc/connect";
import {
  SentinelSchema,
  type ClientOptions,
  type Event,
  type HarnessClient,
  type Sentinel,
} from "../src/index.js";

export type StubClientOptions = Partial<Omit<ClientOptions, "baseUrl" | "interceptors">> & {
  client?: string;
  scenario?: string;
  scenarioKey?: string;
};

export class Stub {
  private constructor(
    private readonly proc: ChildProcess,
    readonly baseUrl: string,
  ) {}

  static async start(): Promise<Stub> {
    const bin = process.env.APISTUB_BIN ?? "../../bin/apistub";
    if (!existsSync(bin)) {
      throw new Error(`the conformance stub is not built at ${bin}. Run \`make apistub\` (or set APISTUB_BIN).`);
    }
    const proc = spawn(bin, ["-addr", "127.0.0.1:0"], { stdio: ["ignore", "pipe", "pipe"] });
    const lines = createInterface({ input: proc.stdout! });
    const addr = await new Promise<string>((resolve, reject) => {
      proc.once("error", reject);
      proc.once("exit", (code) => reject(new Error(`the stub exited with code ${code}`)));
      lines.once("line", (line) => {
        const match = /listening on (\S+)/.exec(line);
        if (match === null) {
          reject(new Error(`the stub said ${JSON.stringify(line)}, not an address`));
          return;
        }
        resolve(match[1]);
      });
    });
    lines.close();
    return new Stub(proc, `http://${addr}`);
  }

  stop(): void {
    this.proc.kill("SIGTERM");
  }

  client({ client, scenario, scenarioKey, ...rest }: StubClientOptions = {}): HarnessClient {
    const headers: Record<string, string> = {};
    if (client !== undefined) headers["x-apistub-client"] = client;
    if (scenario !== undefined) headers["x-apistub-scenario"] = scenario;
    if (scenarioKey !== undefined) headers["x-apistub-scenario-key"] = scenarioKey;
    const setHeaders: Interceptor = (next) => async (req) => {
      for (const [k, v] of Object.entries(headers)) req.header.set(k, v);
      return next(req);
    };
    return createNodeClient({ ...rest, baseUrl: this.baseUrl, interceptors: [setHeaders] });
  }
}

export const sentinelName = (sentinel: Sentinel) => SentinelSchema.value[sentinel].name;

export const Stubbed = {
  sentinelRef: (sentinel: Sentinel) => `sentinel/${sentinelName(sentinel)}`,
  promptPermission: "stub:permission",
  promptUnknownEvent: "stub:unknown-event",
  promptZeroEvent: "stub:zero-event",
} as const;

export async function collect(feed: AsyncIterable<Event>, budgetMs = 20_000): Promise<Event[]> {
  const out: Event[] = [];
  const deadline = Date.now() + budgetMs;
  for await (const ev of feed) {
    out.push(ev);
    if (Date.now() > deadline) {
      throw new Error(`the feed did not end within ${budgetMs}ms (got ${out.length} events)`);
    }
  }
  return out;
}

type Payloads = { [P in Event["payload"] as P["case"] & string]: P["value"] };

export function firstOf<K extends keyof Payloads>(events: readonly Event[], kind: K): Payloads[K] | undefined {
  return events.find((e) => e.payload.case === kind)?.payload.value as Payloads[K] | undefined;
}

export async function failure(call: Promise<unknown>): Promise<unknown> {
  return call.then(
    () => undefined,
    (err: unknown) => err,
  );
}
