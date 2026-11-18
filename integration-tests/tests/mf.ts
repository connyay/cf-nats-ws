import { Miniflare } from "miniflare";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const buildDir = resolve(__dirname, "../test-worker/build");

const NATS_WS_URL = process.env.NATS_WS_URL ?? "ws://localhost:8443";
const NATS_TOKEN = process.env.NATS_TOKEN ?? "dev-nats-token";

const mf_instance = new Miniflare({
  workers: [
    {
      config: {
        name: "test-worker",
        compatibilityDate: "2024-08-31",
        manifest: {
          mainModule: "index.js",
          modules: {
            "index.js": {
              type: "esm",
              contents: readFileSync(resolve(buildDir, "index.js"), "utf8"),
            },
            "index_bg.wasm": {
              type: "wasm",
              contents: new Uint8Array(readFileSync(resolve(buildDir, "index_bg.wasm"))),
            },
          },
        },
        env: {
          NATS_URL: { type: "text", value: NATS_WS_URL },
          NATS_TOKEN: { type: "text", value: NATS_TOKEN },
        },
      },
    },
  ],
});

export const mfUrl = await mf_instance.ready;
export const mf = mf_instance;
