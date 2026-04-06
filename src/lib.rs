macro_rules! debug_log {
    ($($arg:tt)*) => {
        #[cfg(feature = "debug")]
        ::worker::console_log!($($arg)*);
    };
}
#[allow(unused_imports)]
pub(crate) use debug_log;

pub mod client;
pub mod error;
pub mod headers;
pub mod jetstream;
pub mod parser;
pub mod protocol;
pub mod transport;
pub mod types;

pub use client::{NatsClient, SubscriptionHandle};
pub use error::{NatsError, Result};
pub use headers::Headers;
pub use jetstream::JetStreamClient;
pub use types::{ClientOptions, Message, ServerInfo, Subscription};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_commands() {
        let pub_cmd = protocol::build_pub_cmd("test.subject", None, b"hello").unwrap();
        assert_eq!(pub_cmd, b"PUB test.subject 5\r\nhello\r\n");

        let sub_cmd = protocol::build_sub_cmd("test.*", None, 1).unwrap();
        assert_eq!(sub_cmd, b"SUB test.* 1\r\n");
    }
}
