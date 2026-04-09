use crate::client::NatsClient;
use crate::error::{NatsError, Result};
use crate::headers::Headers;
use std::rc::Rc;
use worker::console_log;

const KV_OPERATION_HDR: &str = "KV-Operation";
const KV_PREFIX: &str = "$KV";
const NATS_ROLLUP_HDR: &str = "Nats-Rollup";

/// JetStream KV Store
pub struct KvStore {
    client: Rc<NatsClient>,
    js: super::JetStreamClient,
    bucket: String,
    stream: String,
}

impl KvStore {
    /// Create a new KV store instance for a bucket
    pub fn new(client: Rc<NatsClient>, bucket: &str) -> Self {
        let js = super::JetStreamClient::new(client.clone());
        Self {
            client,
            js,
            bucket: bucket.to_string(),
            stream: format!("KV_{bucket}"),
        }
    }

    /// Create a KV bucket (creates the underlying stream)
    pub async fn create_bucket(&self, opts: KvBucketConfig) -> Result<()> {
        console_log!("KV: Creating bucket {}", self.bucket);

        let stream_config = StreamConfig {
            name: self.stream.clone(),
            subjects: vec![format!("{}.{}.>", KV_PREFIX, self.bucket)],
            retention: Some(RetentionPolicy::Limits),
            max_consumers: Some(-1),
            max_msgs: Some(opts.max_msgs.unwrap_or(-1)),
            max_bytes: Some(opts.max_bytes.unwrap_or(-1)),
            max_age: opts.max_age_ms.map(|ms| ms * 1_000_000), // Convert to nanos
            max_msg_size: Some(opts.max_value_size.unwrap_or(-1)),
            storage: Some(opts.storage.unwrap_or(StorageType::File)),
            num_replicas: Some(opts.replicas.unwrap_or(1) as i32),
            discard: Some(DiscardPolicy::New),
            deny_delete: Some(true),
            allow_rollup_hdrs: Some(true),
            allow_direct: Some(true),
            max_msgs_per_subject: Some(opts.max_history.unwrap_or(1)),
        };

        let subject = format!("{}.STREAM.CREATE.{}", self.js.prefix, self.stream);
        let data = serde_json::to_vec(&stream_config)
            .map_err(|e| NatsError::Parse(format!("Failed to serialize stream config: {e}")))?;

        let response = self.client.request(&subject, &data).await?;

        // Ignore "already exists" errors
        match super::parse_api_response::<StreamInfo>(&response.data) {
            Ok(_) => {}
            Err(NatsError::Server(msg))
                if msg.contains("already exists") || msg.contains("already in use") => {}
            Err(e) => return Err(e),
        }

        console_log!("KV: Bucket {} created/exists", self.bucket);
        Ok(())
    }

    /// Put a key-value pair
    pub async fn put(&self, key: &str, value: &[u8]) -> Result<u64> {
        validate_key(key)?;
        debug_log!("KV: PUT {} in bucket {}", key, self.bucket);

        let subject = format!("{}.{}.{}", KV_PREFIX, self.bucket, key);

        let ack = self.js.publish(&subject, value).await?;
        debug_log!("KV: PUT {} successful, seq: {}", key, ack.seq);
        Ok(ack.seq)
    }

    /// Get a value by key
    pub async fn get(&self, key: &str) -> Result<Option<KvEntry>> {
        validate_key(key)?;
        debug_log!("KV: GET {} from bucket {}", key, self.bucket);

        // Use direct get for efficiency
        let subject = format!("{}.DIRECT.GET.{}", self.js.prefix, self.stream);
        let kv_subject = format!("{}.{}.{}", KV_PREFIX, self.bucket, key);

        // Create the query payload for last_by_subj
        let query = serde_json::json!({
            "last_by_subj": kv_subject
        });
        let query_data = serde_json::to_vec(&query)
            .map_err(|e| NatsError::Parse(format!("Failed to serialize query: {e}")))?;

        match self.client.request(&subject, &query_data).await {
            Ok(msg) => {
                // Check for 404 status (key not found)
                if let Some(headers) = &msg.headers
                    && let Some(status) = headers.status_code()
                    && status == 404
                {
                    debug_log!("KV: Key {} not found (status 404)", key);
                    return Ok(None);
                }

                // Check if it's a delete marker
                if let Some(headers) = &msg.headers
                    && let Some(op) = headers.get(KV_OPERATION_HDR)
                    && (op == "DEL" || op == "PURGE")
                {
                    debug_log!("KV: Key {} is deleted", key);
                    return Ok(None);
                }

                // Extract sequence from headers
                let seq = msg
                    .headers
                    .as_ref()
                    .and_then(|h| h.get("Nats-Sequence"))
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                // Extract creation timestamp from headers
                let created = msg
                    .headers
                    .as_ref()
                    .and_then(|h| h.get("Nats-Time-Stamp"))
                    .map(|ts| {
                        let ms = js_sys::Date::parse(ts);
                        if ms.is_nan() { 0 } else { ms as u64 }
                    })
                    .unwrap_or(0);

                Ok(Some(KvEntry {
                    key: key.to_string(),
                    value: msg.data,
                    revision: seq,
                    created,
                    operation: Operation::Put,
                }))
            }
            Err(e) => Err(e),
        }
    }

    /// Delete a key
    pub async fn delete(&self, key: &str) -> Result<u64> {
        validate_key(key)?;
        debug_log!("KV: DELETE {} from bucket {}", key, self.bucket);

        let subject = format!("{}.{}.{}", KV_PREFIX, self.bucket, key);

        // Create delete header
        let mut headers = Headers::new();
        headers.set(KV_OPERATION_HDR, "DEL");

        let ack = self
            .js
            .publish_with_headers(&subject, &headers, b"")
            .await?;
        debug_log!("KV: DELETE {} successful, seq: {}", key, ack.seq);
        Ok(ack.seq)
    }

    /// Purge (permanently delete) a key and its history
    pub async fn purge(&self, key: &str) -> Result<u64> {
        validate_key(key)?;
        debug_log!("KV: PURGE {} from bucket {}", key, self.bucket);

        let subject = format!("{}.{}.{}", KV_PREFIX, self.bucket, key);

        // Create purge headers
        let mut headers = Headers::new();
        headers.set(KV_OPERATION_HDR, "PURGE");
        headers.set(NATS_ROLLUP_HDR, "sub");

        let ack = self
            .js
            .publish_with_headers(&subject, &headers, b"")
            .await?;
        debug_log!("KV: PURGE {} successful, seq: {}", key, ack.seq);
        Ok(ack.seq)
    }

    /// List all keys in the bucket
    pub async fn keys(&self) -> Result<Vec<String>> {
        debug_log!("KV: Listing keys in bucket {}", self.bucket);

        // Check if stream has any messages
        let info_subject = format!("{}.STREAM.INFO.{}", self.js.prefix, self.stream);
        let response = self.client.request(&info_subject, b"").await?;
        let stream_info: StreamInfo = super::parse_api_response(&response.data)?;
        if stream_info.state.messages == 0 {
            return Ok(Vec::new());
        }

        // Create an ephemeral ordered consumer with last_per_subject delivery
        let inbox = format!("_INBOX.{}", crate::client::generate_inbox_id());
        let filter_subject = format!("{}.{}.>", KV_PREFIX, self.bucket);

        let consumer_config = serde_json::json!({
            "stream_name": self.stream,
            "config": {
                "deliver_subject": inbox,
                "deliver_policy": "last_per_subject",
                "ack_policy": "none",
                "filter_subject": filter_subject,
                "headers_only": true,
                "flow_control": true,
                "idle_heartbeat": 5_000_000_000i64,
                "max_deliver": 1,
                "mem_storage": true,
                "num_replicas": 1
            }
        });
        let consumer_data = serde_json::to_vec(&consumer_config)
            .map_err(|e| NatsError::Parse(format!("Failed to serialize consumer config: {e}")))?;

        // Subscribe to deliver subject BEFORE creating consumer
        let mut sub = self.client.subscribe(&inbox).await?;

        // Create the consumer
        let create_subject = format!("{}.CONSUMER.CREATE.{}", self.js.prefix, self.stream);
        let create_response = self.client.request(&create_subject, &consumer_data).await?;

        let resp: serde_json::Value = serde_json::from_slice(&create_response.data)
            .map_err(|e| NatsError::Parse(format!("Failed to parse consumer response: {e}")))?;
        if let Some(err) = resp.get("error") {
            let code = err.get("code").and_then(|c| c.as_u64()).unwrap_or(0);
            let desc = err
                .get("description")
                .and_then(|d| d.as_str())
                .unwrap_or("unknown");
            return Err(NatsError::Server(format!("{code}: {desc}")));
        }
        let num_pending = resp
            .get("num_pending")
            .and_then(|n| n.as_u64())
            .unwrap_or(0);
        if num_pending == 0 {
            let _ = sub.unsubscribe();
            return Ok(Vec::new());
        }

        // Collect keys from delivered messages
        let prefix = format!("{}.{}.", KV_PREFIX, self.bucket);
        let mut keys = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut received = 0u64;

        while let Some(msg) = sub.next().await {
            // Skip flow control / heartbeat messages (status 100)
            if let Some(headers) = &msg.headers
                && headers.status_code() == Some(100)
            {
                if let Some(reply) = &msg.reply {
                    let _ = self.client.publish(reply, b"");
                }
                continue;
            }

            received += 1;

            // Skip deleted/purged keys
            let is_deleted = msg
                .headers
                .as_ref()
                .and_then(|h| h.get(KV_OPERATION_HDR))
                .is_some_and(|op| op == "DEL" || op == "PURGE");

            if !is_deleted
                && let Some(key) = msg.subject.strip_prefix(&prefix)
                && seen.insert(key.to_string())
            {
                keys.push(key.to_string());
            }

            // Break when we've received all expected messages
            if received >= num_pending {
                break;
            }

            // Also check pending count from reply subject as a fallback
            if let Some(reply) = &msg.reply
                && parse_pending_from_reply(reply) == Some(0)
            {
                break;
            }
        }

        let _ = sub.unsubscribe();
        debug_log!("KV: Found {} unique keys", keys.len());
        Ok(keys)
    }
}

/// Parse the pending message count from a JetStream ACK reply subject.
/// Format (9 tokens):  $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>
/// Format (12 tokens): $JS.ACK.<domain>.<acct>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>.<token>
fn parse_pending_from_reply(reply: &str) -> Option<u64> {
    let tokens: Vec<&str> = reply.split('.').collect();
    let idx = match tokens.len() {
        9 => 8,
        12 => 10,
        _ => return None,
    };
    tokens[idx].parse::<u64>().ok()
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(NatsError::Parse("Key cannot be empty".to_string()));
    }
    if key.starts_with('.') || key.ends_with('.') {
        return Err(NatsError::Parse(format!("Invalid key: {key}")));
    }
    if key.contains(' ') {
        return Err(NatsError::Parse(format!(
            "Key cannot contain spaces: {key}"
        )));
    }
    if key.contains('*') || key.contains('>') {
        return Err(NatsError::Parse(format!(
            "Key cannot contain wildcards: {key}"
        )));
    }
    if key.contains('\r') || key.contains('\n') || key.contains('\0') {
        return Err(NatsError::Parse(
            "Key cannot contain \\r, \\n, or \\0".to_string(),
        ));
    }
    Ok(())
}

// Types
#[derive(Debug, Clone)]
pub struct KvEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub revision: u64,
    pub created: u64,
    pub operation: Operation,
}

#[derive(Debug, Clone)]
pub enum Operation {
    Put,
    Delete,
    Purge,
}

#[derive(Debug, Clone)]
pub struct KvBucketConfig {
    pub max_history: Option<i64>,
    pub max_msgs: Option<i64>,
    pub max_bytes: Option<i64>,
    pub max_age_ms: Option<i64>,
    pub max_value_size: Option<i32>,
    pub storage: Option<StorageType>,
    pub replicas: Option<u32>,
}

impl Default for KvBucketConfig {
    fn default() -> Self {
        Self {
            max_history: Some(1),
            max_msgs: Some(-1),
            max_bytes: Some(-1),
            max_age_ms: None,
            max_value_size: Some(-1),
            storage: Some(StorageType::File),
            replicas: Some(1),
        }
    }
}

// Re-use types from parent module
use super::{DiscardPolicy, RetentionPolicy, StorageType, StreamConfig, StreamInfo};
