import { describe, it, expect, afterAll, beforeAll } from "vitest";
import type { NatsConnection, JetStreamManager } from "nats";
import { connectNats, uniqueName, workerFetch } from "./helpers";

describe("kv advanced", () => {
  let nc: NatsConnection;
  let jsm: JetStreamManager;

  beforeAll(async () => {
    nc = await connectNats();
    jsm = await nc.jetstreamManager();
  });

  afterAll(async () => {
    await nc?.close();
  });

  it("kv put returns revision numbers", async () => {
    const bucket = uniqueName("kvrev");

    const res1 = await workerFetch("kv/put", {
      method: "POST",
      body: { bucket, key: "counter", value: "1" },
    });
    expect(res1.status).toBe(200);
    const body1 = await res1.json();
    expect(body1.revision).toBeGreaterThan(0);

    const res2 = await workerFetch("kv/put", {
      method: "POST",
      body: { bucket, key: "counter", value: "2" },
    });
    expect(res2.status).toBe(200);
    const body2 = await res2.json();
    expect(body2.revision).toBeGreaterThan(body1.revision);

    // Cleanup
    await jsm.streams.delete(`KV_${bucket}`);
  });

  it("kv get returns entry metadata", async () => {
    const bucket = uniqueName("kvmeta");

    // Create and seed via worker
    await workerFetch("kv/put", {
      method: "POST",
      body: { bucket, key: "meta-test", value: "hello" },
    });

    const res = await workerFetch("kv/get", {
      method: "POST",
      body: { bucket, key: "meta-test" },
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.value).toBe("hello");
    expect(body.key).toBe("meta-test");
    expect(body.revision).toBeGreaterThan(0);
    // created is not yet populated by the library (always 0)
    expect(body.created).toBeDefined();

    // Cleanup
    await jsm.streams.delete(`KV_${bucket}`);
  });

  it("kv get returns 404 for missing key", async () => {
    const bucket = uniqueName("kvmiss");

    // Create bucket with a dummy key so bucket exists
    await workerFetch("kv/put", {
      method: "POST",
      body: { bucket, key: "exists", value: "yes" },
    });

    const res = await workerFetch("kv/get", {
      method: "POST",
      body: { bucket, key: "does-not-exist" },
    });
    expect(res.status).toBe(404);

    // Cleanup
    await jsm.streams.delete(`KV_${bucket}`);
  });

  it("kv delete marks key as deleted", async () => {
    const bucket = uniqueName("kvdel");

    // Create and seed
    await workerFetch("kv/put", {
      method: "POST",
      body: { bucket, key: "ephemeral", value: "temp" },
    });

    // Verify it exists
    const getRes = await workerFetch("kv/get", {
      method: "POST",
      body: { bucket, key: "ephemeral" },
    });
    expect(getRes.status).toBe(200);

    // Delete it
    const delRes = await workerFetch("kv/delete", {
      method: "POST",
      body: { bucket, key: "ephemeral" },
    });
    expect(delRes.status).toBe(200);
    const delBody = await delRes.json();
    expect(delBody.revision).toBeGreaterThan(0);

    // Should be gone now
    const getAfter = await workerFetch("kv/get", {
      method: "POST",
      body: { bucket, key: "ephemeral" },
    });
    expect(getAfter.status).toBe(404);

    // Cleanup
    await jsm.streams.delete(`KV_${bucket}`);
  });

  it("kv purge removes key permanently", async () => {
    const bucket = uniqueName("kvpurge");

    await workerFetch("kv/put", {
      method: "POST",
      body: { bucket, key: "purgeable", value: "v1" },
    });
    await workerFetch("kv/put", {
      method: "POST",
      body: { bucket, key: "purgeable", value: "v2" },
    });

    const purgeRes = await workerFetch("kv/purge", {
      method: "POST",
      body: { bucket, key: "purgeable" },
    });
    expect(purgeRes.status).toBe(200);
    const purgeBody = await purgeRes.json();
    expect(purgeBody.revision).toBeGreaterThan(0);

    // Key should be gone
    const getRes = await workerFetch("kv/get", {
      method: "POST",
      body: { bucket, key: "purgeable" },
    });
    expect(getRes.status).toBe(404);

    // Cleanup
    await jsm.streams.delete(`KV_${bucket}`);
  });

  it("kv keys lists all keys in bucket", async () => {
    const bucket = uniqueName("kvkeys");

    // Seed multiple keys
    for (const key of ["alpha", "beta", "gamma"]) {
      const res = await workerFetch("kv/put", {
        method: "POST",
        body: { bucket, key, value: `val-${key}` },
      });
      expect(res.status).toBe(200);
    }

    // kv.keys() uses push consumers which may not work in all environments
    const res = await Promise.race([
      workerFetch("kv/keys", { method: "POST", body: { bucket } }),
      new Promise<Response>((_, reject) =>
        setTimeout(() => reject(new Error("kv/keys timed out")), 10000),
      ),
    ]);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.keys.sort()).toEqual(["alpha", "beta", "gamma"]);

    // Cleanup
    await jsm.streams.delete(`KV_${bucket}`);
  });

  it("kv keys excludes deleted keys", async () => {
    const bucket = uniqueName("kvkeysdel");

    for (const key of ["keep", "remove"]) {
      await workerFetch("kv/put", {
        method: "POST",
        body: { bucket, key, value: "x" },
      });
    }

    await workerFetch("kv/delete", {
      method: "POST",
      body: { bucket, key: "remove" },
    });

    const res = await Promise.race([
      workerFetch("kv/keys", { method: "POST", body: { bucket } }),
      new Promise<Response>((_, reject) =>
        setTimeout(() => reject(new Error("kv/keys timed out")), 10000),
      ),
    ]);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.keys).toEqual(["keep"]);

    // Cleanup
    await jsm.streams.delete(`KV_${bucket}`);
  });
});
