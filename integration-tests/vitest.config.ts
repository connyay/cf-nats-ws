import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    testTimeout: 30000,
    globalSetup: "./globalSetup.ts",
    pool: "forks",
    poolOptions: {
      forks: { singleFork: true },
    },
  },
});
