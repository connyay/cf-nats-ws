# Integration Tests

End-to-end tests that verify NATS messages flow through a Cloudflare Worker (using `cf-nats-ws`) to a real NATS server.

## Prerequisites

- Docker (for the NATS server)
- Node.js
- Rust + `wasm32-unknown-unknown` target (`rustup target add wasm32-unknown-unknown`)

## Architecture

Tests use two independent NATS connections to verify real message flow:

```
┌───────────────────────┐         ┌──────────────┐
│  Vitest test runner   │◄─TCP───►│              │
│  (nats.js :4222)      │         │  NATS Server │
│                       │         │  (Docker)    │
│  Miniflare            │         │              │
│  └─ test-worker       │◄─WS────►│              │
│     (cf-nats-ws :8443)│         └──────────────┘
└───────────────────────┘
```

- **Test-side** connects via TCP (port 4222) using `nats.js` to subscribe, publish, and verify messages
- **Test worker** connects via WebSocket (port 8443) using `cf-nats-ws` through Miniflare
- Both hit the same NATS server, proving messages flow end-to-end

## Quick start

```bash
cd integration-tests
npm install
npm run docker:up    # start NATS
npm run test         # build worker + run tests
npm run docker:down  # tear down
```

## Scripts

| Script                        | Description                             |
| ----------------------------- | --------------------------------------- |
| `npm run docker:up`           | Start NATS server (waits until healthy) |
| `npm run docker:down`         | Stop and remove NATS container          |
| `npm run test`                | Build test worker, then run all tests   |
| `npm run test:no-build`       | Run tests without rebuilding the worker |
| `npm run test:watch`          | Build + watch mode                      |
| `npm run test:no-build:watch` | Watch mode without rebuild              |

## Test worker

The test worker (`test-worker/`) is a minimal Cloudflare Worker that exposes HTTP endpoints exercising `cf-nats-ws`:

| Endpoint                          | Method | Description                                                              |
| --------------------------------- | ------ | ------------------------------------------------------------------------ |
| `/health`                         | GET    | Health check                                                             |
| `/publish`                        | POST   | Publish a message (`{ subject, data }`)                                  |
| `/publish-with-reply`             | POST   | Publish with explicit reply subject (`{ subject, reply, data }`)         |
| `/publish-with-headers`           | POST   | Publish with headers (`{ subject, data, headers }`)                      |
| `/request`                        | POST   | Request/reply (`{ subject, data, timeout_ms? }`)                         |
| `/subscribe-and-collect`          | POST   | Subscribe, collect N messages (`{ subject, count, timeout_ms, queue? }`) |
| `/subscribe-auto-unsub`           | POST   | Subscribe with auto-unsubscribe (`{ subject, max_msgs, timeout_ms }`)    |
| `/server-info`                    | GET    | Return NATS server info                                                  |
| `/flush`                          | POST   | Flush pending messages                                                   |
| `/jetstream/publish`              | POST   | Publish to JetStream (`{ subject, data }`)                               |
| `/jetstream/publish-with-headers` | POST   | Publish to JetStream with headers (`{ subject, data, headers }`)         |
| `/jetstream/stream/create`        | POST   | Create a stream (`{ name, subjects }`)                                   |
| `/jetstream/stream/info`          | POST   | Get stream info (`{ name }`)                                             |
| `/jetstream/stream/delete`        | POST   | Delete a stream (`{ name }`)                                             |
| `/kv/get`                         | POST   | Get a KV entry (`{ bucket, key }`)                                       |
| `/kv/put`                         | POST   | Put a KV entry (`{ bucket, key, value }`)                                |
| `/kv/delete`                      | POST   | Delete a KV entry (`{ bucket, key }`)                                    |
| `/kv/purge`                       | POST   | Purge a KV entry (`{ bucket, key }`)                                     |
| `/kv/keys`                        | POST   | List all keys in a bucket (`{ bucket }`)                                 |

## Test suites

| File                        | Coverage                                                                  |
| --------------------------- | ------------------------------------------------------------------------- |
| `smoke.test.ts`             | NATS connectivity, worker health                                          |
| `connection.test.ts`        | Server info, flush                                                        |
| `pubsub.test.ts`            | Worker publishes, test-side subscribes and verifies                       |
| `publish-reply.test.ts`     | Publish with explicit reply subject                                       |
| `headers.test.ts`           | Publish/receive with NATS headers (HPUB/HMSG)                             |
| `subscribe.test.ts`         | Subscribe & collect, wildcards (`*`, `>`), queue groups, auto-unsubscribe |
| `request-reply.test.ts`     | Request/reply, no-responder handling                                      |
| `jetstream.test.ts`         | JetStream publish with ack, KV get/put                                    |
| `jetstream-streams.test.ts` | Stream create/info/delete, publish with headers, sequence numbers         |
| `kv-advanced.test.ts`       | KV revisions, metadata, delete, purge, keys listing                       |

## Environment variables

| Variable      | Default               | Description                             |
| ------------- | --------------------- | --------------------------------------- |
| `NATS_URL`    | `localhost:4222`      | NATS TCP address (for test-side client) |
| `NATS_WS_URL` | `ws://localhost:8443` | NATS WebSocket address (for worker)     |
| `NATS_TOKEN`  | `dev-nats-token`      | Auth token                              |

## NATS server

Docker Compose runs a single NATS 2.11 node with:

- **TCP** on port 4222 (client connections)
- **WebSocket** on port 8443 (no TLS)
- **HTTP monitoring** on port 8222
- **JetStream** enabled (persistent storage)
- **Token auth** (`dev-nats-token` by default)
