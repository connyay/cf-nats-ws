import { describe, it, expect, afterAll, beforeAll } from "vitest";
import type { NatsConnection } from "nats";
import { connectNats, collectMessages, uniqueSubject, workerFetch, sleep } from "./helpers";

describe("publish with reply", () => {
  let nc: NatsConnection;

  beforeAll(async () => {
    nc = await connectNats();
  });

  afterAll(async () => {
    await nc?.close();
  });

  it("worker publishes with explicit reply subject", async () => {
    const subject = uniqueSubject("pub-reply");
    const replySubject = uniqueSubject("reply-inbox");

    const collecting = collectMessages(nc, subject, 1, 5000);
    await sleep(100);

    const res = await workerFetch("publish-with-reply", {
      method: "POST",
      body: { subject, reply: replySubject, data: "need response" },
    });
    expect(res.status).toBe(200);

    const messages = await collecting;
    expect(messages).toHaveLength(1);
    expect(messages[0].string()).toBe("need response");
    expect(messages[0].reply).toBe(replySubject);
  });
});
