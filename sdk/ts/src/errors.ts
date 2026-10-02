import { ConnectError } from "@connectrpc/connect";
import { ErrorInfoSchema, Sentinel, SentinelSchema } from "./gen/harnessapi/v1/harnessapi_pb.js";

export function sentinelOf(err: unknown): Sentinel | undefined {
  const [info] = ConnectError.from(err).findDetails(ErrorInfoSchema);
  return info !== undefined && info.sentinel !== Sentinel.UNSPECIFIED && SentinelSchema.value[info.sentinel]
    ? info.sentinel
    : undefined;
}
