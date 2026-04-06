import { describe, it, expect, afterAll, beforeAll } from "vitest";
import type { NatsConnection } from "nats";
import { connectNats, collectMessages, uniqueSubject, workerFetch, sleep } from "./helpers";

describe("pub/sub", () => {
  let nc: NatsConnection;

  beforeAll(async () => {
    nc = await connectNats();
  });

  afterAll(async () => {
    await nc?.close();
  });

  it("worker publishes a message that the test-side subscriber receives", async () => {
    const subject = uniqueSubject("pub");

    // Test-side subscribes first via TCP
    const collecting = collectMessages(nc, subject, 1, 5000);

    // Small delay to ensure subscription is active
    await sleep(100);

    // Worker publishes via WebSocket
    const res = await workerFetch("publish", {
      method: "POST",
      body: { subject, data: "hello from worker" },
    });
    expect(res.status).toBe(200);

    const messages = await collecting;
    expect(messages).toHaveLength(1);
    expect(messages[0].string()).toBe("hello from worker");
  });

  it("worker publishes multiple messages in sequence", async () => {
    const subject = uniqueSubject("pub-multi");
    const count = 5;

    const collecting = collectMessages(nc, subject, count, 5000);
    await sleep(100);

    for (let i = 0; i < count; i++) {
      const res = await workerFetch("publish", {
        method: "POST",
        body: { subject, data: `msg-${i}` },
      });
      expect(res.status).toBe(200);
    }

    const messages = await collecting;
    expect(messages).toHaveLength(count);
    for (let i = 0; i < count; i++) {
      expect(messages[i].string()).toBe(`msg-${i}`);
    }
  });
});
