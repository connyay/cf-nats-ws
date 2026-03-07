import { describe, it, expect, afterAll, beforeAll } from "vitest";
import { StringCodec, type NatsConnection } from "nats";
import { connectNats, uniqueSubject, workerFetch, sleep } from "./helpers";

const sc = StringCodec();

describe("subscribe", () => {
  let nc: NatsConnection;

  beforeAll(async () => {
    nc = await connectNats();
  });

  afterAll(async () => {
    await nc?.close();
  });

  it("worker subscribes and collects multiple messages", async () => {
    const subject = uniqueSubject("sub-multi");

    const collectPromise = workerFetch("subscribe-and-collect", {
      method: "POST",
      body: { subject, count: 3, timeout_ms: 5000 },
    });

    await sleep(200);

    nc.publish(subject, sc.encode("msg-0"));
    nc.publish(subject, sc.encode("msg-1"));
    nc.publish(subject, sc.encode("msg-2"));
    await nc.flush();

    const res = await collectPromise;
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.messages).toHaveLength(3);
    expect(body.messages.map((m: any) => m.data)).toEqual(["msg-0", "msg-1", "msg-2"]);
  });

  it("worker subscribes to wildcard subject (*)", async () => {
    const prefix = uniqueSubject("wc");

    const collectPromise = workerFetch("subscribe-and-collect", {
      method: "POST",
      body: { subject: `${prefix}.*`, count: 2, timeout_ms: 5000 },
    });

    await sleep(200);

    nc.publish(`${prefix}.foo`, sc.encode("foo"));
    nc.publish(`${prefix}.bar`, sc.encode("bar"));
    await nc.flush();

    const res = await collectPromise;
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.messages).toHaveLength(2);

    const subjects = body.messages.map((m: any) => m.subject);
    expect(subjects).toContain(`${prefix}.foo`);
    expect(subjects).toContain(`${prefix}.bar`);
  });

  it("worker subscribes to wildcard subject (>)", async () => {
    const prefix = uniqueSubject("wcgt");

    const collectPromise = workerFetch("subscribe-and-collect", {
      method: "POST",
      body: { subject: `${prefix}.>`, count: 3, timeout_ms: 5000 },
    });

    await sleep(200);

    nc.publish(`${prefix}.a`, sc.encode("a"));
    nc.publish(`${prefix}.b.c`, sc.encode("b.c"));
    nc.publish(`${prefix}.d.e.f`, sc.encode("d.e.f"));
    await nc.flush();

    const res = await collectPromise;
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.messages).toHaveLength(3);

    const data = body.messages.map((m: any) => m.data).sort();
    expect(data).toEqual(["a", "b.c", "d.e.f"]);
  });

  it("worker subscribes with queue group distributes messages", async () => {
    const subject = uniqueSubject("queue");
    const queue = "test-workers";
    const messageCount = 6;

    // Two workers subscribe with the same queue group
    const collect1 = workerFetch("subscribe-and-collect", {
      method: "POST",
      body: { subject, count: messageCount, timeout_ms: 3000, queue },
    });
    const collect2 = workerFetch("subscribe-and-collect", {
      method: "POST",
      body: { subject, count: messageCount, timeout_ms: 3000, queue },
    });

    await sleep(200);

    for (let i = 0; i < messageCount; i++) {
      nc.publish(subject, sc.encode(`qmsg-${i}`));
    }
    await nc.flush();

    const [res1, res2] = await Promise.all([collect1, collect2]);
    const body1 = await res1.json();
    const body2 = await res2.json();

    // Together they should have received all messages
    const total = body1.messages.length + body2.messages.length;
    expect(total).toBe(messageCount);

    // Each should have received at least some (queue distributes)
    // In practice with 2 consumers and 6 messages, each gets some
    // but we can't guarantee exact split, so just verify total
  });

  it("worker auto-unsubscribes after N messages", async () => {
    const subject = uniqueSubject("autounsub");
    const maxMsgs = 3;

    const collectPromise = workerFetch("subscribe-auto-unsub", {
      method: "POST",
      body: { subject, max_msgs: maxMsgs, timeout_ms: 3000 },
    });

    await sleep(200);

    // Publish more than max_msgs
    for (let i = 0; i < 6; i++) {
      nc.publish(subject, sc.encode(`auto-${i}`));
    }
    await nc.flush();

    const res = await collectPromise;
    expect(res.status).toBe(200);
    const body = await res.json();

    // Should have received at most max_msgs
    expect(body.messages.length).toBeLessThanOrEqual(maxMsgs);
    expect(body.messages.length).toBeGreaterThan(0);
  });

  it("worker receives reply subject on published messages", async () => {
    const subject = uniqueSubject("reply-sub");
    // Use a simple inbox-style reply subject
    const replySubject = uniqueSubject("inbox");

    const collectPromise = workerFetch("subscribe-and-collect", {
      method: "POST",
      body: { subject, count: 1, timeout_ms: 5000 },
    });

    await sleep(200);

    // Publish with reply subject
    nc.publish(subject, sc.encode("need-reply"), { reply: replySubject });
    await nc.flush();

    const res = await collectPromise;
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.messages).toHaveLength(1);
    expect(body.messages[0].reply).toBe(replySubject);
  });

  it("subscribe times out gracefully when no messages arrive", async () => {
    const subject = uniqueSubject("sub-timeout");

    const res = await workerFetch("subscribe-and-collect", {
      method: "POST",
      body: { subject, count: 1, timeout_ms: 500 },
    });

    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.messages).toHaveLength(0);
  });
});
