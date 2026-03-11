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

    #[error("No responders available for request")]
    NoResponders,

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

/// Returns true if the server error message indicates a connection-terminating error
/// per the NATS protocol specification.
pub fn is_fatal_server_error(msg: &str) -> bool {
    let normalized = msg
        .trim()
        .trim_start_matches('\'')
        .trim_end_matches('\'')
        .to_lowercase();
    matches!(
        normalized.as_str(),
        "unknown protocol operation"
            | "attempted to connect to route port"
            | "authorization violation"
            | "authorization timeout"
            | "invalid client protocol"
            | "maximum control line exceeded"
            | "parser error"
            | "secure connection - tls required"
            | "stale connection"
            | "maximum connections exceeded"
            | "slow consumer"
            | "maximum payload violation"
    )
}

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
    fn test_no_responders_display() {
        let err = NatsError::NoResponders;
        assert_eq!(err.to_string(), "No responders available for request");
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

    #[test]
    fn test_is_fatal_server_error() {
        // Fatal errors (connection-terminating)
        assert!(is_fatal_server_error("'Authorization Violation'"));
        assert!(is_fatal_server_error("'Stale Connection'"));
        assert!(is_fatal_server_error("'Maximum Payload Violation'"));
        assert!(is_fatal_server_error("'Unknown Protocol Operation'"));
        assert!(is_fatal_server_error("'Slow Consumer'"));
        assert!(is_fatal_server_error("'Maximum Connections Exceeded'"));
        assert!(is_fatal_server_error("'Parser Error'"));
        assert!(is_fatal_server_error("'Secure Connection - TLS Required'"));
        assert!(is_fatal_server_error("'Authorization Timeout'"));
        assert!(is_fatal_server_error("'Invalid Client Protocol'"));
        assert!(is_fatal_server_error("'Maximum Control Line Exceeded'"));
        assert!(is_fatal_server_error(
            "'Attempted To Connect To Route Port'"
        ));

        // Non-fatal errors
        assert!(!is_fatal_server_error("'Invalid Subject'"));
        assert!(!is_fatal_server_error(
            "'Permissions Violation for Subscription to foo.bar'"
        ));
        assert!(!is_fatal_server_error(
            "'Permissions Violation for Publish to foo.bar'"
        ));
    }

    #[test]
    fn test_is_fatal_server_error_case_insensitive() {
        assert!(is_fatal_server_error("'authorization violation'"));
        assert!(is_fatal_server_error("'STALE CONNECTION'"));
        assert!(is_fatal_server_error("'Maximum Payload Violation'"));
    }

    #[test]
    fn test_is_fatal_server_error_whitespace() {
        assert!(is_fatal_server_error("  'Stale Connection'  "));
        assert!(is_fatal_server_error("'Stale Connection'"));
    }
}
