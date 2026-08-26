import { describe, it, expect, afterAll, beforeAll } from "vitest";
import type { NatsConnection } from "nats";
import { connectNats, collectMessages, uniqueSubject, workerFetch, sleep } from "./helpers";

/**
 * Sanity check for NatsClient::from_websocket: the worker establishes the
 * WebSocket itself via fetch() with an Upgrade header (the same way a
 * binding-provided socket would be obtained) and hands it to the NATS client.
 */
describe("fetch()-established websocket", () => {
  let nc: NatsConnection;

  beforeAll(async () => {
    nc = await connectNats();
  });

  afterAll(async () => {
    await nc?.close();
  });

  it("completes the NATS handshake and publishes over a fetched websocket", async () => {
    const subject = uniqueSubject("fetchws");

    const collecting = collectMessages(nc, subject, 1, 5000);
    await sleep(100);

    const res = await workerFetch("fetch-websocket/publish", {
      method: "POST",
      body: { subject, data: "hello via fetched socket" },
    });
    expect(res.status).toBe(200);
    const json = (await res.json()) as { server_id: string };
    expect(json.server_id).toBeTruthy();

    const messages = await collecting;
    expect(messages).toHaveLength(1);
    expect(messages[0].string()).toBe("hello via fetched socket");
  });
});
