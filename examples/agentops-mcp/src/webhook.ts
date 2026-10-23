import { createHmac } from "node:crypto";
import { lookup as dnsLookup, type LookupAddress } from "node:dns";
import { request } from "node:https";
import { BlockList, isIP, type LookupFunction } from "node:net";

/**
 * Outbound webhooks for MCP Events: Standard Webhooks signing and an HTTPS
 * POST that only ever connects to public addresses.
 *
 * The callback URL comes from the client, so every connection is checked at
 * connect time (a custom `lookup`, so DNS rebinding can't swap in a private
 * address after validation), TLS still verifies the original hostname, and
 * redirects are never followed (`node:https` doesn't follow them).
 */

/** OpenAI's limit for one delivery's request body. */
export const MAX_EVENT_BYTES = 256 * 1024;

const TIMEOUT_MS = 10_000;
const MAX_RESPONSE_BYTES = 64 * 1024;

export type CallbackFailure =
  | "invalid_url"
  | "blocked_address"
  | "timeout"
  | "unreachable"
  | "http_error"
  | "challenge_failed";

/** A callback that could not be reached or answered wrongly. */
export class CallbackError extends Error {
  readonly reason: CallbackFailure;

  constructor(reason: CallbackFailure, message: string) {
    super(message);
    this.name = "CallbackError";
    this.reason = reason;
  }
}

export type WebhookResponse = { status: number; body: string };

/** Sends one POST; the default is {@link safePost}. Injectable for tests. */
export type WebhookPost = (
  url: string,
  headers: WebhookHeaders,
  body: string,
) => Promise<WebhookResponse>;

/**
 * The signing key of a `whsec_` secret: its base64 value must decode to
 * 24–64 bytes. Undefined for anything else.
 */
export function decodeSecret(secret: string): Buffer | undefined {
  if (!secret.startsWith("whsec_")) return undefined;
  const encoded = secret.slice("whsec_".length);

  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(encoded)) return undefined;
  const key = Buffer.from(encoded, "base64");

  return key.length >= 24 && key.length <= 64 ? key : undefined;
}

/**
 * The Standard Webhooks `webhook-signature` value. With more than one secret
 * (during a rotation), one signature per secret, space-separated.
 */
export function signWebhook(
  secrets: readonly string[],
  id: string,
  timestampSeconds: number,
  body: string,
): string {
  return secrets
    .flatMap((secret) => {
      const key = decodeSecret(secret);

      return key
        ? [
            `v1,${createHmac("sha256", key)
              .update(`${id}.${timestampSeconds}.${body}`)
              .digest("base64")}`,
          ]
        : [];
    })
    .join(" ");
}

export type WebhookHeaders = {
  "content-type": string;
  "webhook-id": string;
  "webhook-timestamp": string;
  "webhook-signature": string;
  "x-mcp-subscription-id": string;
};

/** Signed Standard Webhooks headers for one attempt (fresh timestamp). */
export function webhookHeaders(
  secrets: readonly string[],
  id: string,
  subscriptionId: string,
  body: string,
  now: Date = new Date(),
): WebhookHeaders {
  const timestamp = Math.floor(now.getTime() / 1000);

  return {
    "content-type": "application/json",
    "webhook-id": id,
    "webhook-timestamp": String(timestamp),
    "webhook-signature": signWebhook(secrets, id, timestamp, body),
    "x-mcp-subscription-id": subscriptionId,
  };
}

const blocked = new BlockList();

for (const [network, prefix] of [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16],
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.0.2.0", 24],
  ["192.88.99.0", 24],
  ["192.168.0.0", 16],
  ["198.18.0.0", 15],
  ["198.51.100.0", 24],
  ["203.0.113.0", 24],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4],
] as const) {
  blocked.addSubnet(network, prefix, "ipv4");
}

for (const [network, prefix] of [
  ["::", 128],
  ["::1", 128],
  ["64:ff9b::", 96],
  ["64:ff9b:1::", 48],
  ["100::", 64],
  ["2001::", 23],
  ["2001:db8::", 32],
  ["2002::", 16],
  ["fc00::", 7],
  ["fe80::", 10],
  ["fec0::", 10],
  ["ff00::", 8],
] as const) {
  blocked.addSubnet(network, prefix, "ipv6");
}

/** Whether `address` is a public unicast IP a callback may connect to. */
export function isPublicAddress(address: string): boolean {
  const mapped = /^::ffff:(\d+\.\d+\.\d+\.\d+)$/i.exec(address);
  const ip = mapped?.[1] ?? address;
  const family = isIP(ip);

  if (family === 0) return false;
  // Any other IPv4-mapped/compatible form: refuse rather than reason about it.

  if (family === 6 && /^::ffff:/i.test(ip)) return false;

  return !blocked.check(ip, family === 4 ? "ipv4" : "ipv6");
}

/** dns.lookup that fails unless every resolved address is public. */
const publicOnlyLookup: LookupFunction = (hostname, options, callback) => {
  dnsLookup(hostname, { ...options, all: true }, (error, addresses) => {
    if (error) return callback(error, "");
    const list: LookupAddress[] = addresses;
    const denied = list.find((entry) => !isPublicAddress(entry.address));

    if (list.length === 0 || denied) {
      return callback(
        new CallbackError(
          "blocked_address",
          `${hostname} resolves to a non-public address`,
        ),
        "",
      );
    }

    if (options.all) return callback(null, list);
    const first = list[0];

    return callback(null, first?.address ?? "", first?.family);
  });
};

/** Checks a callback URL before any connection: HTTPS, no private literal IP. */
export function validateCallbackUrl(target: string): URL {
  let url: URL;

  try {
    url = new URL(target);
  } catch {
    throw new CallbackError("invalid_url", "The callback URL is not a URL");
  }

  if (url.protocol !== "https:") {
    throw new CallbackError("invalid_url", "The callback URL must use https");
  }

  if (url.username || url.password) {
    throw new CallbackError("invalid_url", "The callback URL has credentials");
  }

  const host = url.hostname.replace(/^\[|\]$/g, "");

  if (isIP(host) !== 0 && !isPublicAddress(host)) {
    throw new CallbackError(
      "blocked_address",
      "The callback URL points to a non-public address",
    );
  }

  if (host === "localhost" || host.endsWith(".localhost")) {
    throw new CallbackError("blocked_address", "The callback URL is local");
  }

  return url;
}

/** POSTs `body` to a public HTTPS callback; never follows redirects. */
export const safePost: WebhookPost = (target, headers, body) =>
  new Promise((resolve, reject) => {
    const url = validateCallbackUrl(target);

    const req = request(
      url,
      {
        method: "POST",
        headers: { ...headers, "content-length": Buffer.byteLength(body) },
        lookup: publicOnlyLookup,
        timeout: TIMEOUT_MS,
      },
      (res) => {
        let text = "";
        res.setEncoding("utf8");
        res.on("data", (chunk: string) => {
          if (text.length < MAX_RESPONSE_BYTES) text += chunk;
        });
        res.on("end", () =>
          resolve({ status: res.statusCode ?? 0, body: text }),
        );
        res.on("error", (error) =>
          reject(new CallbackError("unreachable", error.message)),
        );
      },
    );

    req.on("timeout", () =>
      req.destroy(new CallbackError("timeout", "The callback timed out")),
    );
    req.on("error", (error) =>
      reject(
        error instanceof CallbackError
          ? error
          : new CallbackError("unreachable", error.message),
      ),
    );
    req.end(body);
  });
