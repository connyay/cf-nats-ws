use cf_nats_ws::{
    ClientOptions, Headers, NatsClient,
    jetstream::{JetStreamClient, KvBucketConfig, KvStore},
};
use std::rc::Rc;
use worker::*;

const NATS_URL: &str = "wss://demo.nats.io:8443";
const KV_BUCKET: &str = "my-bucket";

#[event(fetch)]
pub async fn main(req: Request, _env: Env, _ctx: Context) -> Result<Response> {
    console_log!("NATS WebSocket Worker Example");

    let url = req.url()?;
    let path = url.path();

    match path {
        "/publish" => handle_publish().await,
        "/subscribe" => handle_subscribe().await,
        "/request" => handle_request().await,
        "/headers" => handle_headers_example().await,
        "/kv/create" => handle_kv_create_bucket().await,
        "/kv/put" => handle_kv_put().await,
        "/kv/get" => handle_kv_get().await,
        "/kv/delete" => handle_kv_delete().await,
        "/kv/demo" => handle_kv_demo().await,
        _ => Response::ok(
            "NATS WebSocket Worker - KV Store Examples:\n\
            Basic Operations:\n\
            /publish - Basic publish\n\
            /subscribe - Subscribe example\n\
            /request - Request/reply pattern\n\
            /headers - Publish with headers\n\
            \n\
            KV Store Operations:\n\
            /kv/create - Create KV bucket\n\
            /kv/put - Store key-value pair\n\
            /kv/get - Retrieve value by key\n\
            /kv/delete - Delete a key\n\
            /kv/demo - Full KV demonstration",
        ),
    }
}

async fn connect() -> Result<NatsClient> {
    NatsClient::connect(NATS_URL)
        .await
        .map_err(|e| Error::RustError(format!("Connection failed: {e:?}")))
}

async fn connect_kv(bucket: &str) -> Result<KvStore> {
    let client = Rc::new(connect().await?);
    let js = JetStreamClient::new(client);
    Ok(js.kv(bucket))
}

async fn handle_publish() -> Result<Response> {
    let client = connect().await?;

    client
        .publish("test.subject", b"Hello from Cloudflare Worker!")
        .map_err(|e| Error::RustError(format!("Publish failed: {e:?}")))?;

    client
        .flush()
        .await
        .map_err(|e| Error::RustError(format!("Flush failed: {e:?}")))?;

    Response::ok("Message published to test.subject")
}

async fn handle_subscribe() -> Result<Response> {
    let options = ClientOptions {
        name: Some("cf-worker-subscriber".to_string()),
        verbose: true,
        ..Default::default()
    };

    let client = NatsClient::connect_with_options(NATS_URL, options)
        .await
        .map_err(|e| Error::RustError(format!("Connection failed: {e:?}")))?;

    let mut subscription = client
        .subscribe("test.>")
        .await
        .map_err(|e| Error::RustError(format!("Subscribe failed: {e:?}")))?;

    console_log!("Subscribed to test.>, waiting for messages...");

    client
        .publish("test.worker", b"Self-test message")
        .map_err(|e| Error::RustError(format!("Publish failed: {e:?}")))?;

    let message = match subscription.next().await {
        Some(msg) => {
            let data_str = msg.as_str().unwrap_or("<binary data>");
            format!("Received message on '{}': {}", msg.subject, data_str)
        }
        None => "No message received".to_string(),
    };

    Response::ok(message)
}

async fn handle_request() -> Result<Response> {
    let client = connect().await?;

    client
        .flush()
        .await
        .map_err(|e| Error::RustError(format!("Flush failed: {e:?}")))?;

    // May timeout if no responder is listening
    match client.request("echo.service", b"Hello, NATS!").await {
        Ok(response) => {
            let response_text = response.as_str().unwrap_or("<binary response>").to_string();
            Response::ok(format!("Request/Reply result: {response_text}"))
        }
        Err(e) => Response::ok(format!("Request failed or timed out: {e:?}")),
    }
}

async fn handle_headers_example() -> Result<Response> {
    let client = connect().await?;

    let mut headers = Headers::new();
    headers.set("X-Request-Id", "cf-worker-123");
    headers.set("Content-Type", "application/json");
    headers.append("X-Trace", "worker-trace-1");

    client
        .publish_with_headers(
            "test.headers",
            &headers,
            b"{\"message\": \"Hello with headers!\"}",
        )
        .map_err(|e| Error::RustError(format!("Publish with headers failed: {e:?}")))?;

    client
        .flush()
        .await
        .map_err(|e| Error::RustError(format!("Flush failed: {e:?}")))?;

    Response::ok("Message published with headers to test.headers")
}

async fn handle_kv_create_bucket() -> Result<Response> {
    let kv = connect_kv(KV_BUCKET).await?;

    let config = KvBucketConfig {
        max_history: Some(5),
        max_bytes: Some(1024 * 1024), // 1MB
        max_value_size: Some(1024),   // 1KB per value
        ..Default::default()
    };

    match kv.create_bucket(config).await {
        Ok(_) => Response::ok("KV bucket 'my-bucket' created successfully!"),
        Err(e) => Response::ok(format!("Failed to create bucket: {e:?}")),
    }
}

async fn handle_kv_put() -> Result<Response> {
    let kv = connect_kv(KV_BUCKET).await?;

    let pairs = vec![
        ("user.1.name", "Alice"),
        ("user.1.email", "alice@example.com"),
        ("user.2.name", "Bob"),
        ("user.2.email", "bob@example.com"),
        ("config.timeout", "30"),
        ("config.retries", "3"),
    ];

    let mut results = Vec::new();
    for (key, value) in pairs {
        match kv.put(key, value.as_bytes()).await {
            Ok(seq) => results.push(format!("  {key}: {value} (seq: {seq})")),
            Err(e) => results.push(format!("x {key}: error - {e}")),
        }
    }

    Response::ok(format!("KV PUT results:\n{}", results.join("\n")))
}

async fn handle_kv_get() -> Result<Response> {
    let kv = connect_kv(KV_BUCKET).await?;

    let keys = vec![
        "user.1.name",
        "user.1.email",
        "user.2.name",
        "config.timeout",
        "nonexistent.key",
    ];

    let mut results = Vec::new();
    for key in keys {
        match kv.get(key).await {
            Ok(Some(entry)) => {
                let value = String::from_utf8_lossy(&entry.value);
                results.push(format!("  {}: {} (rev: {})", key, value, entry.revision));
            }
            Ok(None) => results.push(format!("x {key}: not found")),
            Err(e) => results.push(format!("x {key}: error - {e}")),
        }
    }

    Response::ok(format!("KV GET results:\n{}", results.join("\n")))
}

async fn handle_kv_delete() -> Result<Response> {
    let kv = connect_kv(KV_BUCKET).await?;

    let key = "user.2.email";
    match kv.delete(key).await {
        Ok(seq) => {
            match kv.get(key).await {
                Ok(Some(_)) => Response::ok(format!(
                    "Key {key} marked for deletion (seq: {seq}), but still retrievable"
                )),
                Ok(None) => Response::ok(format!("Key {key} successfully deleted (seq: {seq})")),
                Err(e) => Response::ok(format!("Key {key} deleted (seq: {seq}), get error: {e}")),
            }
        }
        Err(e) => Response::ok(format!("Failed to delete key {key}: {e}")),
    }
}

async fn handle_kv_demo() -> Result<Response> {
    let kv = connect_kv("demo-bucket").await?;
    let mut demo_log = Vec::new();

    demo_log.push("KV Store Demo Starting...".to_string());

    demo_log.push("\nStep 1: Creating KV bucket".to_string());
    let config = KvBucketConfig {
        max_history: Some(3),
        max_bytes: Some(512 * 1024), // 512KB
        ..Default::default()
    };

    match kv.create_bucket(config).await {
        Ok(_) => demo_log.push("  Bucket 'demo-bucket' created".to_string()),
        Err(e) => demo_log.push(format!("  Bucket exists or error: {e}")),
    }

    demo_log.push("\nStep 2: Storing key-value pairs".to_string());
    let data = vec![
        ("app.name", "CloudFlare NATS Demo"),
        ("app.version", "1.0.0"),
        ("app.author", "CF Worker"),
        ("settings.debug", "true"),
        ("settings.region", "us-west-2"),
    ];

    for (key, value) in &data {
        match kv.put(key, value.as_bytes()).await {
            Ok(seq) => demo_log.push(format!("  PUT {key}: {value} (seq: {seq})")),
            Err(e) => demo_log.push(format!("x PUT {key}: {e}")),
        }
    }

    demo_log.push("\nStep 3: Reading values back".to_string());
    for (key, _) in &data {
        match kv.get(key).await {
            Ok(Some(entry)) => {
                let value = String::from_utf8_lossy(&entry.value);
                demo_log.push(format!(
                    "  GET {}: {} (rev: {})",
                    key, value, entry.revision
                ));
            }
            Ok(None) => demo_log.push(format!("x GET {key}: not found")),
            Err(e) => demo_log.push(format!("x GET {key}: {e}")),
        }
    }

    demo_log.push("\nStep 4: Updating a value".to_string());
    match kv.put("app.version", b"2.0.0").await {
        Ok(seq) => {
            demo_log.push(format!("  Updated app.version to 2.0.0 (seq: {seq})"));
            if let Ok(Some(entry)) = kv.get("app.version").await {
                let value = String::from_utf8_lossy(&entry.value);
                demo_log.push(format!(
                    "  Confirmed: app.version = {} (rev: {})",
                    value, entry.revision
                ));
            }
        }
        Err(e) => demo_log.push(format!("x Update failed: {e}")),
    }

    demo_log.push("\nStep 5: Deleting a key".to_string());
    match kv.delete("settings.debug").await {
        Ok(seq) => {
            demo_log.push(format!("  Deleted settings.debug (seq: {seq})"));
            match kv.get("settings.debug").await {
                Ok(Some(_)) => demo_log.push("  Key still exists (soft delete)".to_string()),
                Ok(None) => demo_log.push("  Key successfully removed".to_string()),
                Err(_) => demo_log.push("  Key is deleted".to_string()),
            }
        }
        Err(e) => demo_log.push(format!("x Delete failed: {e}")),
    }

    demo_log.push("\nKV Store Demo Complete!".to_string());
    demo_log.push("\nThis demo shows:".to_string());
    demo_log.push("- Creating KV buckets with custom config".to_string());
    demo_log.push("- Storing and retrieving key-value pairs".to_string());
    demo_log.push("- Updating existing values".to_string());
    demo_log.push("- Deleting keys".to_string());
    demo_log.push("- Handling errors gracefully".to_string());

    Response::ok(demo_log.join("\n"))
}
