import { connect } from "nats";

export async function setup() {
  const natsUrl = process.env.NATS_URL ?? "localhost:4222";
  const natsToken = process.env.NATS_TOKEN ?? "dev-nats-token";
  const maxRetries = 30;
  let lastError: Error | null = null;

  for (let i = 0; i < maxRetries; i++) {
    try {
      const nc = await connect({ servers: natsUrl, token: natsToken });
      await nc.close();
      console.log(`NATS is ready at ${natsUrl}`);
      return;
    } catch (err) {
      lastError = err as Error;
      await new Promise((r) => setTimeout(r, 1000));
    }
  }

  throw new Error(`NATS not ready after ${maxRetries}s: ${lastError?.message}`);
}
