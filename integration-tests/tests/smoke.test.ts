import { describe, it, expect, afterAll } from "vitest";
import { connectNats, workerFetch } from "./helpers";

describe("smoke", () => {
  it("NATS is reachable from test runner (TCP)", async () => {
    const nc = await connectNats();
    expect(nc).toBeDefined();
    await nc.close();
  });

  it("worker health endpoint responds", async () => {
    const res = await workerFetch("health");
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("ok");
  });
});
