import { describe, it, expect } from "vitest";
import { workerFetch } from "./helpers";

describe("connection", () => {
  it("server info returns valid NATS server details", async () => {
    const res = await workerFetch("server-info");
    expect(res.status).toBe(200);

    const info = await res.json();
    expect(info.server_id).toBeDefined();
    expect(info.server_id.length).toBeGreaterThan(0);
    expect(info.version).toBeDefined();
    expect(info.port).toBe(8443); // WebSocket port
    expect(info.max_payload).toBeGreaterThan(0);
    expect(info.proto).toBeGreaterThanOrEqual(1);
    expect(info.headers).toBe(true);
    expect(info.jetstream).toBe(true);
  });

  it("flush completes successfully", async () => {
    const res = await workerFetch("flush", { method: "POST" });
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("flushed");
  });
});
