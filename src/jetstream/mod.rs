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

        let inbox = format!("_INBOX.{}", generate_inbox());
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

        let info: ApiResponse<StreamInfo> = serde_json::from_slice(&response.data)
            .map_err(|e| NatsError::Parse(format!("Failed to parse stream info: {e}")))?;

        if let Some(error) = info.error {
            return Err(NatsError::Server(format!(
                "{}: {}",
                error.code, error.description
            )));
        }

        info.response
            .ok_or_else(|| NatsError::Parse("No stream info in response".to_string()))
    }

    /// Create a stream
    pub async fn create_stream(&self, config: &StreamConfig) -> Result<StreamInfo> {
        let subject = format!("{}.STREAM.CREATE.{}", self.prefix, config.name);
        let data = serde_json::to_vec(config)
            .map_err(|e| NatsError::Parse(format!("Failed to serialize stream config: {e}")))?;

        let response = self.api_request(&subject, &data).await?;

        let info: ApiResponse<StreamInfo> =
            serde_json::from_slice(&response.data).map_err(|e| {
                NatsError::Parse(format!("Failed to parse stream create response: {e}"))
            })?;

        if let Some(error) = info.error {
            return Err(NatsError::Server(format!(
                "{}: {}",
                error.code, error.description
            )));
        }

        info.response
            .ok_or_else(|| NatsError::Parse("No stream info in response".to_string()))
    }

    /// Delete a stream
    pub async fn delete_stream(&self, stream: &str) -> Result<bool> {
        let subject = format!("{}.STREAM.DELETE.{}", self.prefix, stream);
        let response = self.api_request(&subject, b"").await?;

        let result: ApiResponse<DeleteResponse> = serde_json::from_slice(&response.data)
            .map_err(|e| NatsError::Parse(format!("Failed to parse delete response: {e}")))?;

        if let Some(error) = result.error {
            return Err(NatsError::Server(format!(
                "{}: {}",
                error.code, error.description
            )));
        }

        Ok(result.response.map(|r| r.success).unwrap_or(false))
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

fn generate_inbox() -> String {
    crate::client::generate_inbox_id()
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
