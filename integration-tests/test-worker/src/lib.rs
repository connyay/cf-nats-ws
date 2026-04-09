use cf_nats_ws::jetstream::StreamConfig;
use cf_nats_ws::{ClientOptions, Headers, JetStreamClient, NatsClient};
use serde::Deserialize;
use std::collections::HashMap;
use std::rc::Rc;
use worker::*;

#[derive(Deserialize)]
struct PublishRequest {
    subject: String,
    data: String,
}

#[derive(Deserialize)]
struct PublishWithReplyRequest {
    subject: String,
    reply: String,
    data: String,
}

#[derive(Deserialize)]
struct PublishWithHeadersRequest {
    subject: String,
    data: String,
    headers: HashMap<String, String>,
}

#[derive(Deserialize)]
struct RequestRequest {
    subject: String,
    data: String,
    #[serde(default = "default_timeout")]
    timeout_ms: u32,
}

#[derive(Deserialize)]
struct SubscribeCollectRequest {
    subject: String,
    #[serde(default = "default_count")]
    count: usize,
    #[serde(default = "default_timeout")]
    timeout_ms: u32,
    queue: Option<String>,
}

#[derive(Deserialize)]
struct SubscribeAutoUnsubRequest {
    subject: String,
    max_msgs: u64,
    #[serde(default = "default_timeout")]
    timeout_ms: u32,
}

#[derive(Deserialize)]
struct StreamCreateRequest {
    name: String,
    subjects: Vec<String>,
}

#[derive(Deserialize)]
struct StreamNameRequest {
    name: String,
}

#[derive(Deserialize)]
struct KvGetRequest {
    bucket: String,
    key: String,
}

#[derive(Deserialize)]
struct KvPutRequest {
    bucket: String,
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct KvKeyRequest {
    bucket: String,
    key: String,
}

#[derive(Deserialize)]
struct KvKeysRequest {
    bucket: String,
}

fn default_timeout() -> u32 {
    5000
}

fn default_count() -> usize {
    1
}

fn nats_err(e: impl std::fmt::Display) -> Error {
    Error::RustError(e.to_string())
}

async fn connect_nats(env: &Env) -> Result<NatsClient> {
    let nats_url = env.var("NATS_URL")?.to_string();
    let token = env.var("NATS_TOKEN").ok().map(|v| v.to_string());

    let options = ClientOptions {
        auth_token: token,
        ..Default::default()
    };

    NatsClient::connect_with_options(&nats_url, options)
        .await
        .map_err(|e| Error::RustError(format!("NATS connect: {e}")))
}

async fn connect_jetstream(env: &Env) -> Result<JetStreamClient> {
    let client = connect_nats(env).await?;
    Ok(JetStreamClient::new(Rc::new(client)))
}

fn build_headers(map: &HashMap<String, String>) -> Headers {
    let mut headers = Headers::new();
    for (k, v) in map {
        headers.set(k, v);
    }
    headers
}

#[event(fetch)]
async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_error_panic_hook::set_once();

    let router = Router::new();

    router
        .get("/health", |_, _| Response::ok("ok"))
        // --- Core publish ---
        .post_async("/publish", |mut req, ctx| async move {
            let body: PublishRequest = req.json().await?;
            let client = connect_nats(&ctx.env).await?;
            client
                .publish(&body.subject, body.data.as_bytes())
                .map_err(nats_err)?;
            Response::ok("published")
        })
        .post_async("/publish-with-reply", |mut req, ctx| async move {
            let body: PublishWithReplyRequest = req.json().await?;
            let client = connect_nats(&ctx.env).await?;
            client
                .publish_with_reply(&body.subject, &body.reply, body.data.as_bytes())
                .map_err(nats_err)?;
            Response::ok("published")
        })
        .post_async("/publish-with-headers", |mut req, ctx| async move {
            let body: PublishWithHeadersRequest = req.json().await?;
            let client = connect_nats(&ctx.env).await?;
            let headers = build_headers(&body.headers);
            client
                .publish_with_headers(&body.subject, &headers, body.data.as_bytes())
                .map_err(nats_err)?;
            Response::ok("published")
        })
        // --- Request/reply ---
        .post_async("/request", |mut req, ctx| async move {
            let body: RequestRequest = req.json().await?;
            let client = connect_nats(&ctx.env).await?;
            match client
                .request_with_timeout(&body.subject, body.data.as_bytes(), body.timeout_ms)
                .await
            {
                Ok(msg) => Response::from_json(&serde_json::json!({
                    "reply": String::from_utf8_lossy(&msg.data),
                })),
                Err(e) => Response::error(format!("request failed: {e}"), 504),
            }
        })
        // --- Subscribe & collect ---
        .post_async("/subscribe-and-collect", |mut req, ctx| async move {
            let body: SubscribeCollectRequest = req.json().await?;
            let client = connect_nats(&ctx.env).await?;
            let mut sub = if let Some(queue) = &body.queue {
                client
                    .subscribe_with_queue(&body.subject, Some(queue))
                    .await
            } else {
                client.subscribe(&body.subject).await
            }
            .map_err(nats_err)?;

            let mut messages: Vec<serde_json::Value> = Vec::new();
            let deadline = js_sys::Date::now() + body.timeout_ms as f64;

            while messages.len() < body.count {
                let remaining = deadline - js_sys::Date::now();
                if remaining <= 0.0 {
                    break;
                }
                // Race between next message and timeout
                let timeout = gloo_timers::future::TimeoutFuture::new(remaining as u32);
                let next = sub.next();
                futures::pin_mut!(timeout);
                futures::pin_mut!(next);
                match futures::future::select(next, timeout).await {
                    futures::future::Either::Left((Some(msg), _)) => {
                        let mut entry = serde_json::json!({
                            "subject": msg.subject,
                            "data": String::from_utf8_lossy(&msg.data),
                        });
                        if let Some(reply) = &msg.reply {
                            entry["reply"] = serde_json::json!(reply);
                        }
                        if let Some(headers) = &msg.headers {
                            let mut hmap: HashMap<String, String> = HashMap::new();
                            for (k, values) in headers.iter() {
                                if let Some(first) = values.first() {
                                    hmap.insert(k.clone(), first.clone());
                                }
                            }
                            entry["headers"] = serde_json::json!(hmap);
                        }
                        messages.push(entry);
                    }
                    _ => break,
                }
            }
            let _ = sub.unsubscribe();
            Response::from_json(&serde_json::json!({ "messages": messages }))
        })
        // --- Subscribe with auto-unsubscribe ---
        .post_async("/subscribe-auto-unsub", |mut req, ctx| async move {
            let body: SubscribeAutoUnsubRequest = req.json().await?;
            let client = connect_nats(&ctx.env).await?;
            let mut sub = client
                .subscribe(&body.subject)
                .await
                .map_err(nats_err)?;
            sub.unsubscribe_after(body.max_msgs)
                .map_err(nats_err)?;

            let mut messages: Vec<String> = Vec::new();
            let deadline = js_sys::Date::now() + body.timeout_ms as f64;

            while js_sys::Date::now() < deadline {
                let remaining = deadline - js_sys::Date::now();
                let timeout = gloo_timers::future::TimeoutFuture::new(remaining as u32);
                let next = sub.next();
                futures::pin_mut!(timeout);
                futures::pin_mut!(next);
                match futures::future::select(next, timeout).await {
                    futures::future::Either::Left((Some(msg), _)) => {
                        messages.push(String::from_utf8_lossy(&msg.data).to_string());
                    }
                    _ => break,
                }
            }
            Response::from_json(&serde_json::json!({ "messages": messages }))
        })
        // --- Connection ---
        .get_async("/server-info", |_, ctx| async move {
            let client = connect_nats(&ctx.env).await?;
            let info = client.server_info();
            Response::from_json(&serde_json::json!({
                "server_id": info.server_id,
                "server_name": info.server_name,
                "version": info.version,
                "host": info.host,
                "port": info.port,
                "max_payload": info.max_payload,
                "proto": info.proto,
                "headers": info.headers,
                "jetstream": info.jetstream,
                "connect_urls": info.connect_urls,
                "domain": info.domain,
            }))
        })
        .post_async("/flush", |_, ctx| async move {
            let client = connect_nats(&ctx.env).await?;
            client
                .flush()
                .await
                .map_err(nats_err)?;
            Response::ok("flushed")
        })
        // --- JetStream ---
        .post_async("/jetstream/publish", |mut req, ctx| async move {
            let body: PublishRequest = req.json().await?;
            let js = connect_jetstream(&ctx.env).await?;
            let ack = js
                .publish(&body.subject, body.data.as_bytes())
                .await
                .map_err(nats_err)?;
            Response::from_json(&serde_json::json!({
                "stream": ack.stream,
                "seq": ack.seq,
            }))
        })
        .post_async("/jetstream/publish-with-headers", |mut req, ctx| async move {
            let body: PublishWithHeadersRequest = req.json().await?;
            let js = connect_jetstream(&ctx.env).await?;
            let headers = build_headers(&body.headers);
            let ack = js
                .publish_with_headers(&body.subject, &headers, body.data.as_bytes())
                .await
                .map_err(nats_err)?;
            Response::from_json(&serde_json::json!({
                "stream": ack.stream,
                "seq": ack.seq,
            }))
        })
        .post_async("/jetstream/stream/create", |mut req, ctx| async move {
            let body: StreamCreateRequest = req.json().await?;
            let js = connect_jetstream(&ctx.env).await?;
            let config = StreamConfig {
                name: body.name,
                subjects: body.subjects,
                ..Default::default()
            };
            let info = js
                .create_stream(&config)
                .await
                .map_err(nats_err)?;
            Response::from_json(&serde_json::json!({
                "name": info.config.name,
                "subjects": info.config.subjects,
                "state": {
                    "messages": info.state.messages,
                    "bytes": info.state.bytes,
                },
            }))
        })
        .post_async("/jetstream/stream/info", |mut req, ctx| async move {
            let body: StreamNameRequest = req.json().await?;
            let js = connect_jetstream(&ctx.env).await?;
            let info = js
                .stream_info(&body.name)
                .await
                .map_err(nats_err)?;
            Response::from_json(&serde_json::json!({
                "name": info.config.name,
                "subjects": info.config.subjects,
                "state": {
                    "messages": info.state.messages,
                    "bytes": info.state.bytes,
                    "first_seq": info.state.first_seq,
                    "last_seq": info.state.last_seq,
                    "consumer_count": info.state.consumer_count,
                },
            }))
        })
        .post_async("/jetstream/stream/delete", |mut req, ctx| async move {
            let body: StreamNameRequest = req.json().await?;
            let js = connect_jetstream(&ctx.env).await?;
            let deleted = js
                .delete_stream(&body.name)
                .await
                .map_err(nats_err)?;
            Response::from_json(&serde_json::json!({ "deleted": deleted }))
        })
        // --- KV ---
        .post_async("/kv/get", |mut req, ctx| async move {
            let body: KvGetRequest = req.json().await?;
            let js = connect_jetstream(&ctx.env).await?;
            let kv = js.kv(&body.bucket);
            let entry = kv
                .get(&body.key)
                .await
                .map_err(nats_err)?;
            match entry {
                Some(val) => Response::from_json(&serde_json::json!({
                    "value": String::from_utf8_lossy(&val.value),
                    "key": val.key,
                    "revision": val.revision,
                    "created": val.created,
                })),
                None => Response::error("key not found", 404),
            }
        })
        .post_async("/kv/put", |mut req, ctx| async move {
            let body: KvPutRequest = req.json().await?;
            let js = connect_jetstream(&ctx.env).await?;
            let kv = js.kv(&body.bucket);
            kv.create_bucket(Default::default())
                .await
                .map_err(nats_err)?;
            let revision = kv
                .put(&body.key, body.value.as_bytes())
                .await
                .map_err(nats_err)?;
            Response::from_json(&serde_json::json!({ "revision": revision }))
        })
        .post_async("/kv/delete", |mut req, ctx| async move {
            let body: KvKeyRequest = req.json().await?;
            let js = connect_jetstream(&ctx.env).await?;
            let kv = js.kv(&body.bucket);
            let revision = kv
                .delete(&body.key)
                .await
                .map_err(nats_err)?;
            Response::from_json(&serde_json::json!({ "revision": revision }))
        })
        .post_async("/kv/purge", |mut req, ctx| async move {
            let body: KvKeyRequest = req.json().await?;
            let js = connect_jetstream(&ctx.env).await?;
            let kv = js.kv(&body.bucket);
            let revision = kv
                .purge(&body.key)
                .await
                .map_err(nats_err)?;
            Response::from_json(&serde_json::json!({ "revision": revision }))
        })
        .post_async("/kv/keys", |mut req, ctx| async move {
            let body: KvKeysRequest = req.json().await?;
            let js = connect_jetstream(&ctx.env).await?;
            let kv = js.kv(&body.bucket);
            let keys = kv
                .keys()
                .await
                .map_err(nats_err)?;
            Response::from_json(&serde_json::json!({ "keys": keys }))
        })
        .run(req, env)
        .await
}
