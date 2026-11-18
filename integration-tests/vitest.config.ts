import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    testTimeout: 30000,
    globalSetup: "./globalSetup.ts",
    pool: "forks",
    fileParallelism: false,
    isolate: false,
  },
});
