pub mod kv;

use crate::client::NatsClient;
use crate::error::{NatsError, Result};
use crate::headers::Headers;
use crate::types::Message;
use serde::{Deserialize, Serialize};
use std::rc::Rc;

pub use kv::{KvBucketConfig, KvEntry, KvStore, Operation};

/// JetStream client wrapper
pub struct JetStreamClient {
    client: Rc<NatsClient>,
    pub(crate) prefix: String,
}

impl JetStreamClient {
    /// Create a new JetStream client
    pub fn new(client: Rc<NatsClient>) -> Self {
        Self {
            client,
            prefix: "$JS.API".to_string(),
        }
    }

    /// Publish a message to a stream with acknowledgment
    pub async fn publish(&self, subject: &str, data: &[u8]) -> Result<PubAck> {
        self.publish_inner(subject, None, data).await
    }

    /// Publish with headers
    pub async fn publish_with_headers(
        &self,
        subject: &str,
        headers: &Headers,
        data: &[u8],
    ) -> Result<PubAck> {
        self.publish_inner(subject, Some(headers), data).await
    }

    async fn publish_inner(
        &self,
        subject: &str,
        headers: Option<&Headers>,
        data: &[u8],
    ) -> Result<PubAck> {
        debug_log!("JetStream: Publishing to {}", subject);

        let inbox = format!("_INBOX.{}", crate::client::generate_inbox_id());
        let mut sub = self.client.subscribe(&inbox).await?;

        if let Some(headers) = headers {
            self.client
                .publish_with_headers_and_reply(subject, &inbox, headers, data)?;
        } else {
            self.client.publish_with_reply(subject, &inbox, data)?;
        }

        match sub.next().await {
            Some(msg) => {
                let ack: PubAck = serde_json::from_slice(&msg.data)
                    .map_err(|e| NatsError::Parse(format!("Failed to parse PubAck: {e}")))?;
                Ok(ack)
            }
            None => Err(NatsError::Timeout),
        }
    }

    /// Get stream info
    pub async fn stream_info(&self, stream: &str) -> Result<StreamInfo> {
        let subject = format!("{}.STREAM.INFO.{}", self.prefix, stream);
        let response = self.api_request(&subject, b"").await?;
        parse_api_response(&response.data)
    }

    /// Create a stream
    pub async fn create_stream(&self, config: &StreamConfig) -> Result<StreamInfo> {
        let subject = format!("{}.STREAM.CREATE.{}", self.prefix, config.name);
        let data = serde_json::to_vec(config)
            .map_err(|e| NatsError::Parse(format!("Failed to serialize stream config: {e}")))?;
        let response = self.api_request(&subject, &data).await?;
        parse_api_response(&response.data)
    }

    /// Delete a stream
    pub async fn delete_stream(&self, stream: &str) -> Result<bool> {
        let subject = format!("{}.STREAM.DELETE.{}", self.prefix, stream);
        let response = self.api_request(&subject, b"").await?;
        let result: DeleteResponse = parse_api_response(&response.data)?;
        Ok(result.success)
    }

    /// Create or get a KV store
    pub fn kv(&self, bucket: &str) -> KvStore {
        KvStore::new(self.client.clone(), bucket)
    }

    /// Make an API request
    async fn api_request(&self, subject: &str, data: &[u8]) -> Result<Message> {
        debug_log!("JetStream API request to {}", subject);
        self.client.request(subject, data).await
    }
}

fn parse_api_response<T: for<'a> Deserialize<'a>>(data: &[u8]) -> Result<T> {
    let resp: ApiResponse<T> = serde_json::from_slice(data)
        .map_err(|e| NatsError::Parse(format!("Failed to parse API response: {e}")))?;
    if let Some(error) = resp.error {
        return Err(NatsError::Server(format!(
            "{}: {}",
            error.code, error.description
        )));
    }
    resp.response
        .ok_or_else(|| NatsError::Parse("Empty API response".to_string()))
}

// JetStream types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubAck {
    pub stream: String,
    pub seq: u64,
    pub domain: Option<String>,
    pub duplicate: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub name: String,
    pub subjects: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_consumers: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_msgs: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_age: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_msg_size: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_replicas: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discard: Option<DiscardPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deny_delete: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_rollup_hdrs: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_direct: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_msgs_per_subject: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RetentionPolicy {
    Limits,
    Interest,
    WorkQueue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageType {
    File,
    Memory,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiscardPolicy {
    Old,
    New,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    pub config: StreamConfig,
    pub created: String,
    pub state: StreamState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamState {
    pub messages: u64,
    pub bytes: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    pub consumer_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiResponse<T> {
    #[serde(rename = "type")]
    response_type: Option<String>,
    error: Option<ApiError>,
    #[serde(flatten)]
    response: Option<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiError {
    code: u16,
    description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeleteResponse {
    success: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::NatsError;

    #[test]
    fn test_parse_api_response_success() {
        let data = br#"{"success": true}"#;
        let result: DeleteResponse = parse_api_response(data).unwrap();
        assert!(result.success);
    }

    #[test]
    fn test_parse_api_response_success_complex_type() {
        let data = br#"{
            "config": {"name": "test", "subjects": ["foo.>"]},
            "created": "2024-01-01T00:00:00Z",
            "state": {"messages": 10, "bytes": 1024, "first_seq": 1, "last_seq": 10, "consumer_count": 2}
        }"#;
        let info: StreamInfo = parse_api_response(data).unwrap();
        assert_eq!(info.config.name, "test");
        assert_eq!(info.state.messages, 10);
        assert_eq!(info.state.last_seq, 10);
    }

    #[test]
    fn test_parse_api_response_api_error() {
        let data = br#"{"error": {"code": 503, "description": "no suitable peers for placement"}}"#;
        let result = parse_api_response::<DeleteResponse>(data);
        match &result {
            Err(NatsError::Server(msg)) => {
                assert!(msg.contains("503"));
                assert!(msg.contains("no suitable peers for placement"));
            }
            other => panic!("Expected NatsError::Server, got: {other:?}"),
        }
    }

    #[test]
    fn test_parse_api_response_malformed_json() {
        let result = parse_api_response::<DeleteResponse>(b"not json");
        assert!(
            matches!(result, Err(NatsError::Parse(ref msg)) if msg.contains("Failed to parse API response"))
        );
    }

    #[test]
    fn test_parse_api_response_already_exists_matches_guard() {
        // kv.rs create_bucket ignores errors matching: msg.contains("already exists")
        let data = br#"{"error": {"code": 400, "description": "stream already exists"}}"#;
        let result = parse_api_response::<StreamInfo>(data);
        match result {
            Err(NatsError::Server(msg)) if msg.contains("already exists") => {}
            other => {
                panic!("Expected NatsError::Server matching 'already exists' guard, got: {other:?}")
            }
        }
    }

    #[test]
    fn test_parse_api_response_already_in_use_matches_guard() {
        // NATS may also return "stream name already in use" (code 10058).
        // kv.rs create_bucket also matches this variant.
        let data = br#"{"error": {"code": 400, "description": "stream name already in use"}}"#;
        let result = parse_api_response::<StreamInfo>(data);
        match result {
            Err(NatsError::Server(msg)) if msg.contains("already in use") => {}
            other => {
                panic!("Expected NatsError::Server matching 'already in use' guard, got: {other:?}")
            }
        }
    }

    #[test]
    fn test_parse_api_response_error_format() {
        let data = br#"{"error": {"code": 404, "description": "stream not found"}}"#;
        let result = parse_api_response::<StreamInfo>(data);
        match result {
            Err(NatsError::Server(msg)) => {
                assert_eq!(msg, "404: stream not found");
            }
            other => panic!("Expected NatsError::Server, got: {other:?}"),
        }
    }
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            subjects: Vec::new(),
            retention: Some(RetentionPolicy::Limits),
            max_consumers: Some(-1),
            max_msgs: Some(-1),
            max_bytes: Some(-1),
            max_age: None,
            max_msg_size: Some(-1),
            storage: Some(StorageType::Memory),
            num_replicas: Some(1),
            discard: None,
            deny_delete: None,
            allow_rollup_hdrs: None,
            allow_direct: None,
            max_msgs_per_subject: None,
        }
    }
}
