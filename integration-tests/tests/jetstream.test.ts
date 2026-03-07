import { describe, it, expect, afterAll, beforeAll } from "vitest";
import type { NatsConnection, JetStreamManager } from "nats";
import { connectNats, uniqueName, workerFetch } from "./helpers";

describe("jetstream", () => {
  let nc: NatsConnection;
  let jsm: JetStreamManager;

  beforeAll(async () => {
    nc = await connectNats();
    jsm = await nc.jetstreamManager();
  });

  afterAll(async () => {
    await nc?.close();
  });

  it("worker publishes to a JetStream stream", async () => {
    const streamName = uniqueName("TEST");
    const subject = `${streamName}.events`;

    // Create stream via test-side
    await jsm.streams.add({
      name: streamName,
      subjects: [`${streamName}.>`],
    });

    // Worker publishes via WebSocket
    const res = await workerFetch("jetstream/publish", {
      method: "POST",
      body: { subject, data: "js event" },
    });
    expect(res.status).toBe(200);
    const ack = await res.json();
    expect(ack.stream).toBe(streamName);
    expect(ack.seq).toBeGreaterThan(0);

    // Verify message landed in the stream
    const js = nc.jetstream();
    const consumer = await js.consumers.get(streamName, {
      filterSubjects: [subject],
    });
    const msg = await consumer.next();
    expect(msg).toBeDefined();
    expect(msg!.string()).toBe("js event");
    msg!.ack();

    // Cleanup
    await jsm.streams.delete(streamName);
  });

  it("worker reads from KV store", async () => {
    const bucketName = uniqueName("kv_test");

    // Create KV bucket and seed a value via test-side
    const js = nc.jetstream();
    const kv = await js.views.kv(bucketName);
    await kv.put("greeting", new TextEncoder().encode("hello"));

    // Worker reads the value via WebSocket
    const res = await workerFetch("kv/get", {
      method: "POST",
      body: { bucket: bucketName, key: "greeting" },
    });
    expect(res.status).toBe(200);

    const body = await res.json();
    expect(body.value).toBe("hello");

    // Cleanup
    await jsm.streams.delete(`KV_${bucketName}`);
  });

  it("worker writes to KV store", async () => {
    const bucketName = uniqueName("kv_test");

    // Worker creates bucket + puts a value via WebSocket
    const res = await workerFetch("kv/put", {
      method: "POST",
      body: { bucket: bucketName, key: "color", value: "blue" },
    });
    expect(res.status).toBe(200);

    // Verify via test-side
    const js = nc.jetstream();
    const kv = await js.views.kv(bucketName);
    const entry = await kv.get("color");
    expect(entry).toBeDefined();
    expect(new TextDecoder().decode(entry!.value)).toBe("blue");

    // Cleanup
    await jsm.streams.delete(`KV_${bucketName}`);
  });
});
