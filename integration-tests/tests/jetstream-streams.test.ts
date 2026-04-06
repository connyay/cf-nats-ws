import { describe, it, expect, afterAll, beforeAll } from "vitest";
import type { NatsConnection, JetStreamManager } from "nats";
import { connectNats, uniqueName, workerFetch } from "./helpers";

describe("jetstream streams", () => {
  let nc: NatsConnection;
  let jsm: JetStreamManager;

  beforeAll(async () => {
    nc = await connectNats();
    jsm = await nc.jetstreamManager();
  });

  afterAll(async () => {
    await nc?.close();
  });

  it("worker creates a stream", async () => {
    const streamName = uniqueName("WORKER_CREATE");

    const res = await workerFetch("jetstream/stream/create", {
      method: "POST",
      body: {
        name: streamName,
        subjects: [`${streamName}.>`],
      },
    });
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body.name).toBe(streamName);
    expect(body.subjects).toContain(`${streamName}.>`);

    // Verify via test-side
    const info = await jsm.streams.info(streamName);
    expect(info.config.name).toBe(streamName);

    // Cleanup
    await jsm.streams.delete(streamName);
  });

  it("worker gets stream info", async () => {
    const streamName = uniqueName("WORKER_INFO");

    // Create stream via test-side
    await jsm.streams.add({
      name: streamName,
      subjects: [`${streamName}.>`],
    });

    // Publish some messages via test-side
    const js = nc.jetstream();
    await js.publish(`${streamName}.a`, new TextEncoder().encode("one"));
    await js.publish(`${streamName}.b`, new TextEncoder().encode("two"));

    const res = await workerFetch("jetstream/stream/info", {
      method: "POST",
      body: { name: streamName },
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.name).toBe(streamName);
    expect(body.state.messages).toBe(2);
    expect(body.state.first_seq).toBe(1);
    expect(body.state.last_seq).toBe(2);

    // Cleanup
    await jsm.streams.delete(streamName);
  });

  it("worker deletes a stream", async () => {
    const streamName = uniqueName("WORKER_DEL");

    // Create stream via test-side
    await jsm.streams.add({
      name: streamName,
      subjects: [`${streamName}.>`],
    });

    const res = await workerFetch("jetstream/stream/delete", {
      method: "POST",
      body: { name: streamName },
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.deleted).toBe(true);

    // Verify it's gone
    await expect(jsm.streams.info(streamName)).rejects.toThrow();
  });

  it("worker publishes with headers to JetStream", async () => {
    const streamName = uniqueName("WORKER_HDR");
    const subject = `${streamName}.events`;

    // Create stream via test-side
    await jsm.streams.add({
      name: streamName,
      subjects: [`${streamName}.>`],
    });

    const res = await workerFetch("jetstream/publish-with-headers", {
      method: "POST",
      body: {
        subject,
        data: "event with headers",
        headers: { "X-Event-Type": "test", "X-Priority": "high" },
      },
    });
    expect(res.status).toBe(200);
    const ack = await res.json();
    expect(ack.stream).toBe(streamName);
    expect(ack.seq).toBeGreaterThan(0);

    // Verify message + headers via test-side
    const js = nc.jetstream();
    const consumer = await js.consumers.get(streamName, {
      filterSubjects: [subject],
    });
    const msg = await consumer.next();
    expect(msg).toBeDefined();
    expect(msg!.string()).toBe("event with headers");
    expect(msg!.headers?.get("X-Event-Type")).toBe("test");
    expect(msg!.headers?.get("X-Priority")).toBe("high");
    msg!.ack();

    // Cleanup
    await jsm.streams.delete(streamName);
  });

  it("worker publishes return sequential sequence numbers", async () => {
    const streamName = uniqueName("WORKER_SEQ");
    const subject = `${streamName}.seq`;

    await jsm.streams.add({
      name: streamName,
      subjects: [`${streamName}.>`],
    });

    const seqs: number[] = [];
    for (let i = 0; i < 3; i++) {
      const res = await workerFetch("jetstream/publish", {
        method: "POST",
        body: { subject, data: `seq-${i}` },
      });
      expect(res.status).toBe(200);
      const ack = await res.json();
      seqs.push(ack.seq);
    }

    // Sequences should be monotonically increasing
    expect(seqs[1]).toBeGreaterThan(seqs[0]);
    expect(seqs[2]).toBeGreaterThan(seqs[1]);

    // Cleanup
    await jsm.streams.delete(streamName);
  });
});
