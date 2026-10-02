import { readFile } from "node:fs/promises";
import { createClient, type Interceptor } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-node";
import { HarnessAPIService } from "./gen/harnessapi/v1/harnessapi_pb.js";
import { transportOptions, type ClientOptions, type HarnessClient } from "./client.js";

export interface NodeClientOptions extends ClientOptions {
  tokenFile?: string;
  httpVersion?: "1.1" | "2";
}

export function createNodeClient(opts: NodeClientOptions): HarnessClient {
  const bearerFile = opts.tokenFile === undefined ? [] : [bearer(opts.tokenFile)];
  return createClient(
    HarnessAPIService,
    createConnectTransport({ ...transportOptions(opts, bearerFile), httpVersion: opts.httpVersion ?? "1.1" }),
  );
}

function bearer(tokenFile: string): Interceptor {
  return (next) => async (req) => {
    req.header.set("Authorization", `Bearer ${(await readFile(tokenFile, "utf8")).trim()}`);
    return next(req);
  };
}
