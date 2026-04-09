use crate::error::{NatsError, Result};
use std::collections::HashMap;

const HEADER_VERSION: &str = "NATS/1.0";

/// Message headers for NATS 2.2+
#[derive(Debug, Clone, Default)]
pub struct Headers {
    inner: HashMap<String, Vec<String>>,
    status_code: Option<u16>,
    status_description: Option<String>,
}

impl Headers {
    /// Create a new empty Headers instance
    pub fn new() -> Self {
        Self::default()
    }

    /// Create Headers with a status code and description
    pub fn with_status(code: u16, description: impl Into<String>) -> Self {
        Self {
            inner: HashMap::new(),
            status_code: Some(code),
            status_description: Some(description.into()),
        }
    }

    /// Set a header value, replacing any existing values
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = canonicalize_header_key(key.into());
        self.inner.insert(key, vec![value.into()]);
    }

    /// Append a header value
    pub fn append(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = canonicalize_header_key(key.into());
        self.inner.entry(key).or_default().push(value.into());
    }

    /// Get the first value for a header
    pub fn get(&self, key: &str) -> Option<&str> {
        let key = canonicalize_header_key(key.to_string());
        self.inner
            .get(&key)
            .and_then(|v| v.first().map(|s| s.as_str()))
    }

    /// Get all values for a header
    pub fn get_all(&self, key: &str) -> Vec<&str> {
        let key = canonicalize_header_key(key.to_string());
        self.inner
            .get(&key)
            .map(|v| v.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    /// Remove a header
    pub fn remove(&mut self, key: &str) -> Option<Vec<String>> {
        let key = canonicalize_header_key(key.to_string());
        self.inner.remove(&key)
    }

    /// Check if headers are empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty() && self.status_code.is_none()
    }

    /// Get the number of headers
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Get status code if set
    pub fn status_code(&self) -> Option<u16> {
        self.status_code
    }

    /// Get status description if set
    pub fn status_description(&self) -> Option<&str> {
        self.status_description.as_deref()
    }

    /// Encode headers to NATS wire format
    pub fn encode(&self) -> Vec<u8> {
        use std::io::Write;
        let mut result = Vec::with_capacity(128);

        result.extend_from_slice(HEADER_VERSION.as_bytes());
        if let (Some(code), Some(desc)) = (self.status_code, &self.status_description) {
            let _ = write!(result, " {code} {desc}");
        }
        result.extend_from_slice(b"\r\n");

        for (key, values) in &self.inner {
            for value in values {
                let _ = write!(result, "{key}: {value}\r\n");
            }
        }

        result.extend_from_slice(b"\r\n");
        result
    }

    /// Decode headers from NATS wire format
    pub fn decode(data: &[u8]) -> Result<Self> {
        let s = std::str::from_utf8(data)
            .map_err(|e| NatsError::Parse(format!("Invalid UTF-8 in headers: {e}")))?;

        let lines: Vec<&str> = s.split("\r\n").collect();
        if lines.is_empty() {
            return Ok(Self::default());
        }

        let mut headers = Self::default();

        // Parse version line
        let version_line = lines[0];
        if let Some(rest) = version_line.strip_prefix(HEADER_VERSION) {
            let rest = rest.trim();
            if !rest.is_empty() {
                // Parse status code and description
                let parts: Vec<&str> = rest.splitn(2, ' ').collect();
                if !parts.is_empty()
                    && let Ok(code) = parts[0].parse::<u16>()
                {
                    headers.status_code = Some(code);
                    if parts.len() > 1 {
                        headers.status_description = Some(parts[1].to_string());
                    }
                }
            }
        }

        // Parse headers
        for line in lines.iter().skip(1) {
            if line.is_empty() {
                break;
            }
            if let Some(colon_idx) = line.find(':') {
                let key = line[..colon_idx].trim().to_string();
                let value = line[colon_idx + 1..].trim().to_string();
                headers.append(key, value);
            }
        }

        Ok(headers)
    }

    /// Iterate over all headers
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Vec<String>)> {
        self.inner.iter()
    }
}

/// Canonicalize header key to Title-Case in a single pass.
fn canonicalize_header_key(key: String) -> String {
    let mut result = String::with_capacity(key.len());
    let mut capitalize_next = true;
    for c in key.chars() {
        if c == '-' {
            result.push('-');
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c.to_ascii_lowercase());
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canonicalize_header_key() {
        assert_eq!(
            canonicalize_header_key("content-type".to_string()),
            "Content-Type"
        );
        assert_eq!(
            canonicalize_header_key("CONTENT-TYPE".to_string()),
            "Content-Type"
        );
        assert_eq!(
            canonicalize_header_key("x-nats-stream".to_string()),
            "X-Nats-Stream"
        );
    }

    #[test]
    fn test_canonicalize_header_key_edge_cases() {
        // Empty string
        assert_eq!(canonicalize_header_key("".to_string()), "");

        // Single character
        assert_eq!(canonicalize_header_key("x".to_string()), "X");

        // Multiple dashes
        assert_eq!(canonicalize_header_key("a-b-c-d".to_string()), "A-B-C-D");

        // Already canonical
        assert_eq!(
            canonicalize_header_key("Content-Type".to_string()),
            "Content-Type"
        );

        // Mixed case
        assert_eq!(
            canonicalize_header_key("cOnTeNt-TyPe".to_string()),
            "Content-Type"
        );
    }

    #[test]
    fn test_headers_basic() {
        let mut headers = Headers::new();
        headers.set("Content-Type", "application/json");
        assert_eq!(headers.get("content-type"), Some("application/json"));
        assert_eq!(headers.len(), 1);
    }

    #[test]
    fn test_headers_set_replaces_values() {
        let mut headers = Headers::new();
        headers.set("Content-Type", "text/plain");
        headers.set("Content-Type", "application/json");
        assert_eq!(headers.get("Content-Type"), Some("application/json"));
        assert_eq!(headers.len(), 1);
    }

    #[test]
    fn test_headers_append() {
        let mut headers = Headers::new();
        headers.append("Accept", "text/html");
        headers.append("Accept", "application/json");

        let values = headers.get_all("Accept");
        assert_eq!(values.len(), 2);
        assert!(values.contains(&"text/html"));
        assert!(values.contains(&"application/json"));
    }

    #[test]
    fn test_headers_get_nonexistent() {
        let headers = Headers::new();
        assert_eq!(headers.get("X-Nonexistent"), None);
        assert!(headers.get_all("X-Nonexistent").is_empty());
    }

    #[test]
    fn test_headers_remove() {
        let mut headers = Headers::new();
        headers.set("Content-Type", "application/json");
        headers.set("X-Custom", "value");

        let removed = headers.remove("Content-Type");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap(), vec!["application/json".to_string()]);
        assert!(headers.get("Content-Type").is_none());
        assert_eq!(headers.len(), 1);
    }

    #[test]
    fn test_headers_remove_nonexistent() {
        let mut headers = Headers::new();
        let removed = headers.remove("X-Nonexistent");
        assert!(removed.is_none());
    }

    #[test]
    fn test_headers_is_empty() {
        let headers = Headers::new();
        assert!(headers.is_empty());

        let mut headers = Headers::new();
        headers.set("Content-Type", "application/json");
        assert!(!headers.is_empty());

        // Headers with status but no headers
        let headers = Headers::with_status(200, "OK");
        assert!(!headers.is_empty());
    }

    #[test]
    fn test_headers_encode_decode() {
        let mut headers = Headers::new();
        headers.set("Content-Type", "application/json");
        headers.set("X-Custom", "value");

        let encoded = headers.encode();
        let decoded = Headers::decode(&encoded).unwrap();

        assert_eq!(decoded.get("Content-Type"), Some("application/json"));
        assert_eq!(decoded.get("X-Custom"), Some("value"));
    }

    #[test]
    fn test_headers_encode_format() {
        let mut headers = Headers::new();
        headers.set("Content-Type", "application/json");

        let encoded = headers.encode();
        let encoded_str = std::str::from_utf8(&encoded).unwrap();

        assert!(encoded_str.starts_with("NATS/1.0\r\n"));
        assert!(encoded_str.contains("Content-Type: application/json\r\n"));
        assert!(encoded_str.ends_with("\r\n\r\n"));
    }

    #[test]
    fn test_headers_with_status() {
        let headers = Headers::with_status(503, "No Responders");
        let encoded = headers.encode();
        let decoded = Headers::decode(&encoded).unwrap();

        assert_eq!(decoded.status_code(), Some(503));
        assert_eq!(decoded.status_description(), Some("No Responders"));
    }

    #[test]
    fn test_headers_encode_with_status() {
        let headers = Headers::with_status(404, "Not Found");
        let encoded = headers.encode();
        let encoded_str = std::str::from_utf8(&encoded).unwrap();

        assert!(encoded_str.starts_with("NATS/1.0 404 Not Found\r\n"));
    }

    #[test]
    fn test_headers_decode_empty() {
        let empty: &[u8] = b"";
        let headers = Headers::decode(empty).unwrap();
        assert!(headers.is_empty());
    }

    #[test]
    fn test_headers_decode_version_only() {
        let data = b"NATS/1.0\r\n\r\n";
        let headers = Headers::decode(data).unwrap();
        assert!(headers.status_code().is_none());
        assert_eq!(headers.len(), 0);
    }

    #[test]
    fn test_headers_decode_with_headers() {
        let data = b"NATS/1.0\r\nX-Test: value\r\nContent-Type: text/plain\r\n\r\n";
        let headers = Headers::decode(data).unwrap();
        assert_eq!(headers.get("X-Test"), Some("value"));
        assert_eq!(headers.get("Content-Type"), Some("text/plain"));
    }

    #[test]
    fn test_headers_decode_status_code_only() {
        let data = b"NATS/1.0 408\r\n\r\n";
        let headers = Headers::decode(data).unwrap();
        assert_eq!(headers.status_code(), Some(408));
        // No description
        assert!(headers.status_description().is_none());
    }

    #[test]
    fn test_headers_iter() {
        let mut headers = Headers::new();
        headers.set("Content-Type", "application/json");
        headers.set("X-Custom", "value");

        let items: Vec<_> = headers.iter().collect();
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_headers_case_insensitive_get() {
        let mut headers = Headers::new();
        headers.set("Content-Type", "application/json");

        assert_eq!(headers.get("content-type"), Some("application/json"));
        assert_eq!(headers.get("CONTENT-TYPE"), Some("application/json"));
        assert_eq!(headers.get("Content-Type"), Some("application/json"));
    }

    #[test]
    fn test_headers_default() {
        let headers = Headers::default();
        assert!(headers.is_empty());
        assert!(headers.status_code().is_none());
        assert!(headers.status_description().is_none());
    }

    #[test]
    fn test_headers_clone() {
        let mut headers = Headers::new();
        headers.set("Content-Type", "application/json");

        let cloned = headers.clone();
        assert_eq!(cloned.get("Content-Type"), Some("application/json"));
    }

    #[test]
    fn test_headers_roundtrip_with_multiple_values() {
        let mut headers = Headers::new();
        headers.append("Accept", "text/html");
        headers.append("Accept", "application/json");
        headers.set("Content-Type", "text/plain");

        let encoded = headers.encode();
        let decoded = Headers::decode(&encoded).unwrap();

        let accept_values = decoded.get_all("Accept");
        assert!(
            accept_values.contains(&"text/html") || accept_values.contains(&"application/json")
        );
        assert_eq!(decoded.get("Content-Type"), Some("text/plain"));
    }

    #[test]
    fn test_headers_various_status_codes() {
        let test_cases = [
            (200, "OK"),
            (404, "Not Found"),
            (500, "Internal Server Error"),
            (503, "No Responders"),
            (408, "Request Timeout"),
        ];

        for (code, desc) in test_cases {
            let headers = Headers::with_status(code, desc);
            let encoded = headers.encode();
            let decoded = Headers::decode(&encoded).unwrap();

            assert_eq!(decoded.status_code(), Some(code));
            assert_eq!(decoded.status_description(), Some(desc));
        }
    }
}
