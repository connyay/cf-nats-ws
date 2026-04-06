use crate::headers::Headers;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfo {
    pub server_id: String,
    pub server_name: Option<String>,
    pub version: String,
    pub go: Option<String>,
    pub host: String,
    pub port: u16,
    pub auth_required: Option<bool>,
    pub tls_required: Option<bool>,
    pub max_payload: i64,
    pub client_id: Option<u64>,
    pub client_ip: Option<String>,
    pub nonce: Option<String>,
    pub cluster: Option<String>,
    pub proto: u8,
    pub headers: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub subject: String,
    pub reply: Option<String>,
    pub data: Vec<u8>,
    pub headers: Option<Headers>,
}

impl Message {
    pub fn new(subject: String, data: Vec<u8>) -> Self {
        Self {
            subject,
            reply: None,
            data,
            headers: None,
        }
    }

    pub fn with_reply(mut self, reply: String) -> Self {
        self.reply = Some(reply);
        self
    }

    pub fn as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.data)
    }

    pub fn as_json<T: for<'a> Deserialize<'a>>(&self) -> serde_json::Result<T> {
        serde_json::from_slice(&self.data)
    }
}

#[derive(Debug, Clone)]
pub struct Subscription {
    pub id: u64,
    pub subject: String,
    pub queue: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectInfo {
    pub verbose: bool,
    pub pedantic: bool,
    pub tls_required: bool,
    pub name: Option<String>,
    pub lang: String,
    pub version: String,
    pub protocol: u8,
    pub echo: bool,
    pub headers: bool,
    pub no_responders: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pass: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nkey: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sig: Option<String>,
}

impl Default for ConnectInfo {
    fn default() -> Self {
        Self {
            verbose: false,
            pedantic: false,
            tls_required: false,
            name: None,
            lang: "rust".to_string(),
            version: "0.1.0".to_string(),
            protocol: 1,
            echo: true,
            headers: true,
            no_responders: true,
            auth_token: None,
            user: None,
            pass: None,
            jwt: None,
            nkey: None,
            sig: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub name: Option<String>,
    pub verbose: bool,
    pub pedantic: bool,
    pub echo: bool,
    pub headers: bool,
    pub auth_token: Option<String>,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub jwt: Option<String>,
    pub nkey: Option<String>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            name: None,
            verbose: false,
            pedantic: false,
            echo: true,
            headers: true,
            auth_token: None,
            user: None,
            pass: None,
            jwt: None,
            nkey: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_new() {
        let msg = Message::new("test.subject".to_string(), b"hello".to_vec());
        assert_eq!(msg.subject, "test.subject");
        assert_eq!(msg.data, b"hello");
        assert!(msg.reply.is_none());
        assert!(msg.headers.is_none());
    }

    #[test]
    fn test_message_with_reply() {
        let msg = Message::new("test.subject".to_string(), b"hello".to_vec())
            .with_reply("_INBOX.123".to_string());
        assert_eq!(msg.reply, Some("_INBOX.123".to_string()));
    }

    #[test]
    fn test_message_as_str() {
        let msg = Message::new("test".to_string(), b"hello world".to_vec());
        assert_eq!(msg.as_str().unwrap(), "hello world");
    }

    #[test]
    fn test_message_as_str_invalid_utf8() {
        let msg = Message::new("test".to_string(), vec![0xff, 0xfe]);
        assert!(msg.as_str().is_err());
    }

    #[test]
    fn test_message_as_json() {
        let msg = Message::new("test".to_string(), br#"{"key":"value"}"#.to_vec());
        let parsed: serde_json::Value = msg.as_json().unwrap();
        assert_eq!(parsed["key"], "value");
    }

    #[test]
    fn test_message_as_json_invalid() {
        let msg = Message::new("test".to_string(), b"not json".to_vec());
        let result: serde_json::Result<serde_json::Value> = msg.as_json();
        assert!(result.is_err());
    }

    #[test]
    fn test_connect_info_default() {
        let info = ConnectInfo::default();
        assert!(!info.verbose);
        assert!(!info.pedantic);
        assert!(!info.tls_required);
        assert!(info.name.is_none());
        assert_eq!(info.lang, "rust");
        assert_eq!(info.protocol, 1);
        assert!(info.echo);
        assert!(info.headers);
        assert!(info.no_responders);
        assert!(info.auth_token.is_none());
        assert!(info.user.is_none());
        assert!(info.pass.is_none());
    }

    #[test]
    fn test_connect_info_serialization() {
        let info = ConnectInfo::default();
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"lang\":\"rust\""));
        // Optional fields should not appear
        assert!(!json.contains("auth_token"));
        assert!(!json.contains("user"));
        assert!(!json.contains("pass"));
    }

    #[test]
    fn test_connect_info_with_auth() {
        let mut info = ConnectInfo::default();
        info.auth_token = Some("test-token".to_string());
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"auth_token\":\"test-token\""));
    }

    #[test]
    fn test_client_options_default() {
        let opts = ClientOptions::default();
        assert!(opts.name.is_none());
        assert!(!opts.verbose);
        assert!(!opts.pedantic);
        assert!(opts.echo);
        assert!(opts.headers);
        assert!(opts.auth_token.is_none());
        assert!(opts.user.is_none());
        assert!(opts.pass.is_none());
        assert!(opts.jwt.is_none());
        assert!(opts.nkey.is_none());
    }

    #[test]
    fn test_subscription() {
        let sub = Subscription {
            id: 1,
            subject: "test.>".to_string(),
            queue: Some("workers".to_string()),
        };
        assert_eq!(sub.id, 1);
        assert_eq!(sub.subject, "test.>");
        assert_eq!(sub.queue, Some("workers".to_string()));
    }

    #[test]
    fn test_subscription_without_queue() {
        let sub = Subscription {
            id: 42,
            subject: "events.*".to_string(),
            queue: None,
        };
        assert_eq!(sub.id, 42);
        assert_eq!(sub.subject, "events.*");
        assert!(sub.queue.is_none());
    }

    #[test]
    fn test_server_info_deserialization() {
        let json = r#"{
            "server_id": "NCPUYHXMDXBSQCKHK4KT4LXQSXVDWJZQJ2RRKH5QK",
            "version": "2.10.0",
            "host": "localhost",
            "port": 4222,
            "max_payload": 1048576,
            "proto": 1
        }"#;
        let info: ServerInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.server_id, "NCPUYHXMDXBSQCKHK4KT4LXQSXVDWJZQJ2RRKH5QK");
        assert_eq!(info.version, "2.10.0");
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, 4222);
        assert_eq!(info.max_payload, 1048576);
        assert_eq!(info.proto, 1);
    }

    #[test]
    fn test_server_info_with_optional_fields() {
        let json = r#"{
            "server_id": "test",
            "server_name": "my-server",
            "version": "2.10.0",
            "go": "go1.21",
            "host": "0.0.0.0",
            "port": 4222,
            "auth_required": true,
            "tls_required": false,
            "max_payload": 1048576,
            "client_id": 12345,
            "client_ip": "192.168.1.100",
            "nonce": "abc123",
            "cluster": "my-cluster",
            "proto": 1,
            "headers": true
        }"#;
        let info: ServerInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.server_name, Some("my-server".to_string()));
        assert_eq!(info.go, Some("go1.21".to_string()));
        assert_eq!(info.auth_required, Some(true));
        assert_eq!(info.tls_required, Some(false));
        assert_eq!(info.client_id, Some(12345));
        assert_eq!(info.client_ip, Some("192.168.1.100".to_string()));
        assert_eq!(info.nonce, Some("abc123".to_string()));
        assert_eq!(info.cluster, Some("my-cluster".to_string()));
        assert_eq!(info.headers, Some(true));
    }
}
