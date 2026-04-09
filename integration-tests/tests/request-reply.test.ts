import { describe, it, expect, afterAll, beforeAll } from "vitest";
import { StringCodec, type NatsConnection } from "nats";
import { connectNats, uniqueSubject, workerFetch, sleep } from "./helpers";

const sc = StringCodec();

describe("request/reply", () => {
  let nc: NatsConnection;

  beforeAll(async () => {
    nc = await connectNats();
  });

  afterAll(async () => {
    await nc?.close();
  });

  it("worker sends a request and receives a reply", async () => {
    const subject = uniqueSubject("reqrep");

    // Test-side sets up a responder via TCP
    const sub = nc.subscribe(subject);
    (async () => {
      for await (const msg of sub) {
        msg.respond(sc.encode(`reply to: ${msg.string()}`));
      }
    })();

    // Small delay to ensure responder is active
    await sleep(100);

    // Worker sends request via WebSocket
    const res = await workerFetch("request", {
      method: "POST",
      body: { subject, data: "ping" },
    });
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body.reply).toBe("reply to: ping");

    sub.unsubscribe();
  });

  it("worker request fails when no responder", async () => {
    const subject = uniqueSubject("reqrep-timeout");

    const res = await workerFetch("request", {
      method: "POST",
      body: { subject, data: "hello", timeout_ms: 2000 },
    });

    // With no_responders enabled, the server sends a 503 status HMSG
    // which the client converts to a NoResponders error
    expect(res.status).toBe(504);
    const body = await res.text();
    expect(body.toLowerCase()).toContain("no responders");
  });
});
