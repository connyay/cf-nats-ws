import { Miniflare } from "miniflare";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const buildDir = resolve(__dirname, "../test-worker/build");

const NATS_WS_URL = process.env.NATS_WS_URL ?? "ws://localhost:8443";
const NATS_TOKEN = process.env.NATS_TOKEN ?? "dev-nats-token";

const mf_instance = new Miniflare({
  workers: [
    {
      name: "test-worker",
      rootPath: buildDir,
      scriptPath: "./index.js",
      compatibilityDate: "2024-08-31",
      modules: true,
      modulesRules: [{ type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true }],
      bindings: {
        NATS_URL: NATS_WS_URL,
        NATS_TOKEN,
      },
    },
  ],
});

export const mfUrl = await mf_instance.ready;
export const mf = mf_instance;
