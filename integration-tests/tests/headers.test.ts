import { describe, it, expect, afterAll, beforeAll } from "vitest";
import { StringCodec, headers as natsHeaders, type NatsConnection } from "nats";
import { connectNats, collectMessages, uniqueSubject, workerFetch, sleep } from "./helpers";

const sc = StringCodec();

describe("headers", () => {
  let nc: NatsConnection;

  beforeAll(async () => {
    nc = await connectNats();
  });

  afterAll(async () => {
    await nc?.close();
  });

  it("worker publishes with headers, test-side receives them", async () => {
    const subject = uniqueSubject("hdr-pub");

    const collecting = collectMessages(nc, subject, 1, 5000);
    await sleep(100);

    const res = await workerFetch("publish-with-headers", {
      method: "POST",
      body: {
        subject,
        data: "with headers",
        headers: {
          "X-Custom": "test-value",
          "Content-Type": "application/json",
        },
      },
    });
    expect(res.status).toBe(200);

    const messages = await collecting;
    expect(messages).toHaveLength(1);
    expect(messages[0].string()).toBe("with headers");
    expect(messages[0].headers?.get("X-Custom")).toBe("test-value");
    expect(messages[0].headers?.get("Content-Type")).toBe("application/json");
  });

  it("worker receives headers published by test-side", async () => {
    const subject = uniqueSubject("hdr-recv");

    // Worker subscribes and collects
    const collectPromise = workerFetch("subscribe-and-collect", {
      method: "POST",
      body: { subject, count: 1, timeout_ms: 5000 },
    });

    await sleep(200);

    // Test-side publishes with headers
    const hdrs = natsHeaders();
    hdrs.set("X-Source", "test-runner");
    hdrs.set("X-Trace-Id", "abc-123");
    nc.publish(subject, sc.encode("headers from test"), { headers: hdrs });
    await nc.flush();

    const res = await collectPromise;
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.messages).toHaveLength(1);
    expect(body.messages[0].data).toBe("headers from test");
    expect(body.messages[0].headers["X-Source"]).toBe("test-runner");
    expect(body.messages[0].headers["X-Trace-Id"]).toBe("abc-123");
  });
});
