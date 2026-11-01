// A minimal webhook receiver that verifies pgdispatch / Standard Webhooks signatures.
//
// No dependencies (Node 18+). Run it with:
//
//   WEBHOOK_SECRET=whsec_... PORT=3000 node receiver.js
//
// If you can add a dependency, the official `standardwebhooks` npm package
// performs the same verification:
//
//   import { Webhook } from "standardwebhooks";
//   const event = new Webhook(secret).verify(rawBody, headers); // throws on failure
//
// The important part is verify(); copy it into your own framework and pass it
// the *raw* request body. In Express, for example, use
// express.raw({ type: "application/json" }) on the webhook route instead of
// express.json(), because re-serialized JSON would not match the signature.

"use strict";

const crypto = require("node:crypto");
const http = require("node:http");

const TOLERANCE_SECONDS = 5 * 60;
const MAX_BODY_BYTES = 1024 * 1024;

class InvalidWebhookError extends Error {}

/**
 * Throw InvalidWebhookError unless `body` was signed with `secret`.
 *
 * @param {string} secret  The endpoint secret, "whsec_<base64>".
 * @param {import("node:http").IncomingHttpHeaders} headers  Lower-cased request headers.
 * @param {Buffer} body  The exact bytes received.
 */
function verify(secret, headers, body) {
  const msgId = headers["webhook-id"];
  const timestamp = headers["webhook-timestamp"];
  const signatures = headers["webhook-signature"];
  if (!msgId || !timestamp || !signatures) {
    throw new InvalidWebhookError("missing webhook headers");
  }

  // Replay protection: reject requests signed too far in the past or future.
  if (!/^\d+$/.test(timestamp)) {
    throw new InvalidWebhookError("invalid webhook-timestamp");
  }
  const now = Math.floor(Date.now() / 1000);
  if (Math.abs(now - Number(timestamp)) > TOLERANCE_SECONDS) {
    throw new InvalidWebhookError("timestamp outside the tolerance window");
  }

  // The key is the base64 part of the secret, after the "whsec_" prefix.
  const key = Buffer.from(secret.replace(/^whsec_/, ""), "base64");
  const expected = crypto
    .createHmac("sha256", key)
    .update(`${msgId}.${timestamp}.`)
    .update(body)
    .digest();

  // During a secret rotation the header holds several "v1,<sig>" entries
  // separated by spaces; one match is enough.
  for (const entry of signatures.split(" ")) {
    const [version, signature] = entry.split(",", 2);
    if (version !== "v1" || !signature) continue;
    const received = Buffer.from(signature, "base64");
    // timingSafeEqual throws on a length mismatch, so check that first.
    if (received.length === expected.length && crypto.timingSafeEqual(received, expected)) {
      return;
    }
  }
  throw new InvalidWebhookError("no matching signature");
}

/** Read the raw request body, refusing anything larger than `limit` bytes. */
function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > limit) {
        reject(new RangeError("body too large"));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

const secret = process.env.WEBHOOK_SECRET;
if (!secret) {
  console.error("WEBHOOK_SECRET is not set");
  process.exit(1);
}

const server = http.createServer(async (req, res) => {
  if (req.method !== "POST") {
    res.writeHead(405, { Allow: "POST" }).end();
    return;
  }

  let body;
  try {
    body = await readBody(req, MAX_BODY_BYTES);
  } catch {
    res.writeHead(413).end();
    return;
  }

  try {
    verify(secret, req.headers, body);
  } catch (err) {
    res.writeHead(401, { "Content-Type": "text/plain" }).end(err.message);
    return;
  }

  const event = JSON.parse(body.toString("utf8"));
  // Delivery is at-least-once: the same webhook-id may arrive again after a
  // timeout or a retry. Store processed ids (e.g. a unique column) and skip
  // duplicates. Keep this handler fast: enqueue the work and answer 2xx right
  // away, then process it in the background.
  console.log(`received ${event.type} (${req.headers["webhook-id"]})`);

  res.writeHead(204).end();
});

const port = Number(process.env.PORT || 3000);
server.listen(port, () => console.log(`listening on :${port}`));
