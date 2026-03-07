# cf-nats-ws

A Wasm-compatible NATS client built specifically for Cloudflare Workers environments, using WebSocket transport.

## Features

- **Wasm-first design** — Built for Cloudflare Workers, no tokio or OS dependencies
- **WebSocket transport** — Connects to NATS servers via WebSocket protocol
- **Core NATS protocol** — PUB, SUB, request/reply patterns
- **Headers support** — NATS 2.2+ message headers (HPUB/HMSG)
- **Request timeouts** — Wasm-compatible timeout implementation
- **JetStream support** — Basic JetStream client and Key-Value store

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
cf-nats-ws = { git = "https://github.com/connyay/cf-nats-ws" }
```

## Usage

### Basic Connection

```rust
use cf_nats_ws::NatsClient;

let client = NatsClient::connect("wss://demo.nats.io:443").await?;
```

### Publishing

```rust
// Simple publish
client.publish("subject", b"Hello NATS")?;

// Publish with reply subject
client.publish_with_reply("subject", "reply.inbox", b"Request")?;

// Publish with headers
use cf_nats_ws::Headers;
let mut headers = Headers::new();
headers.set("Content-Type", "application/json");
client.publish_with_headers("subject", &headers, b"{}")?;
```

### Subscribing

```rust
let mut sub = client.subscribe("subject.*").await?;

while let Some(msg) = sub.next().await {
    println!("Received: {}", String::from_utf8_lossy(&msg.data));
    if let Some(reply) = &msg.reply {
        client.publish(reply, b"ACK")?;
    }
}
```

### Request/Reply

```rust
// With default 5s timeout
let response = client.request("service.endpoint", b"request data").await?;

// With custom timeout
let response = client.request_with_timeout("service.endpoint", b"data", 10000).await?;
```

### Queue Groups

```rust
let sub = client.subscribe_with_queue("work.queue", Some("workers")).await?;
```

### Auto-unsubscribe

```rust
let mut sub = client.subscribe("subject").await?;
sub.unsubscribe_after(10)?; // Unsubscribe after 10 messages
```

## Connection Options

```rust
use cf_nats_ws::{NatsClient, ClientOptions};

let options = ClientOptions {
    name: Some("my-worker".to_string()),
    verbose: false,
    pedantic: false,
    echo: true,
    headers: true,
    auth_token: Some("my-token".to_string()),
    ..Default::default()
};

let client = NatsClient::connect_with_options("wss://nats.example.com", options).await?;
```

## JetStream

```rust
use cf_nats_ws::JetStreamClient;
use std::rc::Rc;

let client = Rc::new(NatsClient::connect("wss://nats.example.com").await?);
let js = JetStreamClient::new(client);

// Publish to stream
js.publish("ORDERS.new", b"order data").await?;

// Key-Value store
let kv = js.kv("my-kv");
kv.put("key", b"value").await?;
let entry = kv.get("key").await?;
```

## Architecture Notes

### Wasm Compatibility

This client is designed for single-threaded Wasm environments:

- Uses `Rc<RefCell<>>` instead of `Arc<Mutex<>>`
- Uses `worker::wasm_bindgen_futures::spawn_local` for async tasks
- Timeouts use `web_sys::WorkerGlobalScope::set_timeout`
- Random IDs use `crypto.getRandomValues` via `web_sys::Crypto`

### Protocol Implementation

Implements the [NATS client protocol](https://docs.nats.io/reference/reference-protocols/nats-protocol):

- INFO, CONNECT, PUB, SUB, UNSUB, MSG, PING, PONG, +OK, -ERR
- HPUB, HMSG (headers support)
- Protocol v1 with headers and no_responders
- Automatic PING/PONG handling
- Streaming parser for partial WebSocket frames

### Memory Management

- Automatic subscription cleanup on unsubscribe
- Auto-cleanup for `unsubscribe_after()` requests
- Proper cleanup of timeout handlers

## URL Formats

Automatically converts between URL schemes:

- `nats://host:port` → `ws://host:port`
- `tls://host:port` → `wss://host:port`
- `nats+tls://host:port` → `wss://host:port`
- `ws://host:port` → used as-is
- `wss://host:port` → used as-is

## Example: Cloudflare Worker

```rust
use worker::*;
use cf_nats_ws::NatsClient;

#[event(fetch)]
async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    let nats_url = env.var("NATS_URL")?.to_string();
    let client = NatsClient::connect(&nats_url).await
        .map_err(|e| Error::RustError(format!("NATS connection failed: {}", e)))?;

    client.publish("worker.requests", b"Hello from Worker")?;

    let response = client.request("service.hello", b"ping").await
        .map_err(|e| Error::RustError(format!("Request failed: {}", e)))?;

    Response::ok(String::from_utf8_lossy(&response.data))
}
```

## Error Handling

```rust
use cf_nats_ws::NatsError;

match client.request("subject", b"data").await {
    Ok(msg) => println!("Got response: {:?}", msg),
    Err(NatsError::Timeout) => println!("Request timed out"),
    Err(NatsError::Connection(e)) => println!("Connection error: {}", e),
    Err(e) => println!("Other error: {}", e),
}
```

## Limitations

- No TLS certificate validation (relies on Workers runtime)
- No reconnection logic (designed for serverless, stateless workers)
- No cluster failover (single connection only)
- No NKey signature generation (auth_token, JWT, or user/pass only)

## License

MIT

## References

Based on implementations from:

- [nats.deno](https://github.com/nats-io/nats.deno)
- [nats.ws](https://github.com/nats-io/nats.ws)

Protocol specification:

- [NATS Protocol](https://docs.nats.io/reference/reference-protocols/nats-protocol)
