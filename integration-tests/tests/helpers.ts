import { connect, type NatsConnection, type Msg } from "nats";
import { mf, mfUrl } from "./mf";

// --- NATS helpers (test-side, TCP connection) ---

const NATS_URL = process.env.NATS_URL ?? "localhost:4222";
const NATS_TOKEN = process.env.NATS_TOKEN ?? "dev-nats-token";

export async function connectNats(): Promise<NatsConnection> {
  return connect({ servers: NATS_URL, token: NATS_TOKEN });
}

/**
 * Subscribe and collect up to `count` messages, or until `timeoutMs` elapses.
 */
export async function collectMessages(
  nc: NatsConnection,
  subject: string,
  count: number,
  timeoutMs: number = 5000,
): Promise<Msg[]> {
  const messages: Msg[] = [];
  const sub = nc.subscribe(subject);

  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      sub.unsubscribe();
      resolve(messages);
    }, timeoutMs);

    (async () => {
      for await (const msg of sub) {
        messages.push(msg);
        if (messages.length >= count) {
          clearTimeout(timer);
          sub.unsubscribe();
          resolve(messages);
          return;
        }
      }
    })();
  });
}

/**
 * Generate a unique subject to isolate test traffic.
 */
export function uniqueSubject(prefix: string): string {
  const ts = Date.now().toString().slice(-6);
  const rand = Math.random().toString(36).slice(2, 6);
  return `test.${prefix}.${ts}.${rand}`;
}

export function uniqueName(prefix: string): string {
  const ts = Date.now().toString().slice(-6);
  const rand = Math.random().toString(36).slice(2, 6);
  return `${prefix}_${ts}_${rand}`;
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// --- Worker helpers (Miniflare dispatch) ---

export async function workerFetch(
  path: string,
  options: {
    method?: string;
    headers?: Record<string, string>;
    body?: unknown;
  } = {},
) {
  const { body, ...rest } = options;
  return mf.dispatchFetch(`${mfUrl}${path}`, {
    ...rest,
    headers: {
      "Content-Type": "application/json",
      ...rest.headers,
    },
    body: body ? JSON.stringify(body) : undefined,
  });
}
