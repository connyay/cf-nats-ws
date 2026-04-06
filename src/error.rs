use thiserror::Error;

#[derive(Error, Debug)]
pub enum NatsError {
    #[error("WebSocket error: {0}")]
    WebSocket(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Timeout")]
    Timeout,

    #[error("Server error: {0}")]
    Server(String),

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("Invalid subject: {0}")]
    InvalidSubject(String),

    #[error("Subscription not found")]
    SubscriptionNotFound,

    #[error("Worker error: {0}")]
    Worker(#[from] worker::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, NatsError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_websocket_error_display() {
        let err = NatsError::WebSocket("connection failed".to_string());
        assert_eq!(err.to_string(), "WebSocket error: connection failed");
    }

    #[test]
    fn test_protocol_error_display() {
        let err = NatsError::Protocol("invalid command".to_string());
        assert_eq!(err.to_string(), "Protocol error: invalid command");
    }

    #[test]
    fn test_connection_error_display() {
        let err = NatsError::Connection("refused".to_string());
        assert_eq!(err.to_string(), "Connection error: refused");
    }

    #[test]
    fn test_parse_error_display() {
        let err = NatsError::Parse("unexpected token".to_string());
        assert_eq!(err.to_string(), "Parse error: unexpected token");
    }

    #[test]
    fn test_timeout_error_display() {
        let err = NatsError::Timeout;
        assert_eq!(err.to_string(), "Timeout");
    }

    #[test]
    fn test_server_error_display() {
        let err = NatsError::Server("internal error".to_string());
        assert_eq!(err.to_string(), "Server error: internal error");
    }

    #[test]
    fn test_invalid_state_error_display() {
        let err = NatsError::InvalidState("not connected".to_string());
        assert_eq!(err.to_string(), "Invalid state: not connected");
    }

    #[test]
    fn test_subscription_not_found_display() {
        let err = NatsError::SubscriptionNotFound;
        assert_eq!(err.to_string(), "Subscription not found");
    }

    #[test]
    fn test_json_error_from() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let nats_err = NatsError::from(json_err);
        assert!(matches!(nats_err, NatsError::Json(_)));
        assert!(nats_err.to_string().contains("JSON error"));
    }

    #[test]
    fn test_result_type_ok() {
        let result: Result<i32> = Ok(42);
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_result_type_err() {
        let result: Result<i32> = Err(NatsError::Timeout);
        assert!(result.is_err());
    }

    #[test]
    fn test_error_debug() {
        let err = NatsError::WebSocket("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("WebSocket"));
    }
}
