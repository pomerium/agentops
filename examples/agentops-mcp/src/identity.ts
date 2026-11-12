import type { ServerContext } from "@modelcontextprotocol/server";
import { Result, Schema } from "effect";

const PomeriumClaims = Schema.Struct({
  email: Schema.optional(Schema.String),
  sub: Schema.optional(Schema.String),
});

/**
 * The signed-in user, from the `X-Pomerium-Jwt-Assertion` header that Pomerium
 * adds on the `mcp` route (`pass_identity_headers`).
 *
 * The signature is not verified here: the server is reachable only through
 * Pomerium (its Service has no other route), and the value only scopes which
 * tasks a user sees. Verify it against Pomerium's JWKS before using it for
 * authorization decisions.
 */
export function userFromContext(ctx: ServerContext): string | undefined {
  return userFromHeaders(ctx.http?.req?.headers);
}

export function userFromHeaders(
  headers: Headers | undefined,
): string | undefined {
  const assertion = headers?.get("x-pomerium-jwt-assertion");

  if (!assertion) return undefined;

  const payload = assertion.split(".")[1];

  if (!payload) return undefined;

  try {
    const claims = Schema.decodeUnknownResult(PomeriumClaims)(
      JSON.parse(Buffer.from(payload, "base64url").toString("utf8")),
    );

    if (Result.isFailure(claims)) return undefined;

    return claims.success.email || claims.success.sub || undefined;
  } catch {
    return undefined;
  }
}
