use crate::error::{NatsError, Result};
use crate::parser::{Op, Parser};
use crate::protocol;
use crate::transport::WsTransport;
use crate::types::{ClientOptions, ConnectInfo, Message, ServerInfo};
use futures::channel::{mpsc, oneshot};
use futures::{Stream, StreamExt};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use worker::{WebSocket, console_error, console_log, console_warn};

pub struct NatsClient {
    transport: Rc<WsTransport>,
    server_info: Rc<RefCell<ServerInfo>>,
    subscriptions: Rc<RefCell<HashMap<u64, mpsc::UnboundedSender<Message>>>>,
    next_sid: Rc<RefCell<u64>>,
    pongs: Rc<RefCell<VecDeque<oneshot::Sender<()>>>>,
    // Request/reply multiplexing: one wildcard inbox subscription shared by
    // all requests instead of a SUB/UNSUB round trip per request.
    inbox_prefix: String,
    resp_map: Rc<RefCell<HashMap<u64, oneshot::Sender<Message>>>>,
    next_resp_token: RefCell<u64>,
    resp_mux_started: RefCell<bool>,
}

impl NatsClient {
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_options(url, ClientOptions::default()).await
    }

    pub async fn connect_with_options(url: &str, options: ClientOptions) -> Result<Self> {
        console_log!("NatsClient: Starting connection to {}", redact_url(url));

        // Convert URL to WebSocket URL if needed. Everything except an
        // explicit ws:// maps to TLS — plaintext must be opted into.
        let ws_url = if url.starts_with("ws://") || url.starts_with("wss://") {
            url.to_string()
        } else if url.starts_with("nats://") {
            url.replace("nats://", "wss://")
        } else if url.starts_with("nats+tls://") {
            url.replace("nats+tls://", "wss://")
        } else if url.starts_with("tls://") {
            url.replace("tls://", "wss://")
        } else {
            format!("wss://{url}")
        };

        debug_log!(
            "NatsClient: Connecting to WebSocket URL: {}",
            redact_url(&ws_url)
        );
        let transport = WsTransport::connect(&ws_url).await?;
        Self::from_transport(transport, options).await
    }

    /// Connect NATS over an already-connected Workers WebSocket.
    ///
    /// The caller owns how the WebSocket is acquired. This supports sockets
    /// returned by Worker bindings without coupling this crate to `Fetcher`,
    /// `Env`, or any particular Cloudflare networking product.
    ///
    /// The socket must not have been accepted yet and its `events()` stream
    /// must not have been taken — `accept()` is called internally, and fails
    /// on a socket the caller already accepted.
    pub async fn from_websocket(ws: WebSocket, options: ClientOptions) -> Result<Self> {
        console_log!("NatsClient: Starting connection over supplied WebSocket");
        let transport = WsTransport::from_websocket(ws)?;
        Self::from_transport(transport, options).await
    }

    async fn from_transport(transport: WsTransport, options: ClientOptions) -> Result<Self> {
        let mut parser = Parser::new();

        debug_log!("NatsClient: Waiting for INFO message");
        // Wait for INFO message
        let mut server_info = None;
        let mut attempts = 0;
        while server_info.is_none() && attempts < 10 {
            attempts += 1;
            debug_log!("NatsClient: Attempt {} to get INFO message", attempts);

            let Some(data) = transport.next_message().await? else {
                return Err(NatsError::Connection(
                    "connection closed before INFO received".to_string(),
                ));
            };
            debug_log!("NatsClient: Received data: {} bytes", data.len());
            for op in parser.parse(&data)? {
                match op {
                    Op::Info(json) => {
                        debug_log!("NatsClient: Got INFO: {}", json);
                        server_info = Some(serde_json::from_str::<ServerInfo>(&json)?);
                    }
                    Op::Ping => transport.send(protocol::PONG)?,
                    Op::Err(e) => return Err(NatsError::Server(e)),
                    op => {
                        console_warn!("NatsClient: Got unexpected op: {:?}", op);
                    }
                }
            }
        }

        let mut server_info = server_info.ok_or_else(|| {
            NatsError::Connection("No INFO message received after 10 attempts".to_string())
        })?;
        debug_log!("NatsClient: Got server info: {:?}", server_info.server_id);

        // Send CONNECT
        let connect_cmd = protocol::build_connect_cmd(&ConnectInfo::from(options))?;
        debug_log!("NatsClient: Sending CONNECT command");
        transport.send(&connect_cmd)?;

        // Send initial PING to complete handshake
        debug_log!("NatsClient: Sending initial PING");
        transport.send(protocol::PING)?;

        // Wait for the PONG (or -ERR, e.g. auth failure) so connect() reports
        // handshake failures instead of returning a client that appears
        // connected. This also consumes the handshake PONG, so it can't
        // complete an unrelated flush() early.
        let handshake = async {
            loop {
                let Some(data) = transport.next_message().await? else {
                    return Err(NatsError::Connection(
                        "connection closed during handshake".to_string(),
                    ));
                };
                let mut pong = false;
                for op in parser.parse(&data)? {
                    match op {
                        Op::Pong => pong = true,
                        Op::Err(e) => return Err(NatsError::Server(e)),
                        Op::Ping => transport.send(protocol::PONG)?,
                        Op::Info(json) => match serde_json::from_str::<ServerInfo>(&json) {
                            Ok(info) => server_info = info,
                            Err(e) => {
                                console_warn!("NatsClient: Failed to parse INFO update: {}", e);
                            }
                        },
                        _ => {}
                    }
                }
                if pong {
                    return Ok(());
                }
            }
        };
        wasm_timeout(5000, std::pin::pin!(handshake))
            .await
            .map_err(|_| NatsError::Connection("handshake timed out".to_string()))??;
        debug_log!("NatsClient: Handshake complete");

        let transport = Rc::new(transport);
        let subscriptions = Rc::new(RefCell::new(HashMap::new()));
        let next_sid = Rc::new(RefCell::new(1));
        let pongs = Rc::new(RefCell::new(VecDeque::new()));
        let server_info = Rc::new(RefCell::new(server_info));

        let client = Self {
            transport: transport.clone(),
            server_info: server_info.clone(),
            subscriptions: subscriptions.clone(),
            next_sid,
            pongs: pongs.clone(),
            inbox_prefix: format!("_INBOX.{}", generate_inbox_id()),
            resp_map: Rc::new(RefCell::new(HashMap::new())),
            next_resp_token: RefCell::new(1),
            resp_mux_started: RefCell::new(false),
        };

        // Start message processing task
        let transport_clone = transport.clone();
        let subs_clone = subscriptions.clone();
        let pongs_clone = pongs.clone();
        let info_clone = server_info.clone();
        worker::wasm_bindgen_futures::spawn_local(async move {
            debug_log!("NatsClient: Starting message processor");
            if let Err(e) = Self::process_messages(
                transport_clone,
                parser,
                subs_clone.clone(),
                pongs_clone.clone(),
                info_clone,
            )
            .await
            {
                console_log!("NatsClient: Message processing error: {:?}", e);
            }
            // The connection is gone. Drop all subscription senders so pending
            // sub.next() calls resolve to None, and drop pending flush waiters
            // so flush() fails immediately instead of waiting out its timeout.
            subs_clone.borrow_mut().clear();
            pongs_clone.borrow_mut().clear();
        });

        Ok(client)
    }

    async fn process_messages(
        transport: Rc<WsTransport>,
        mut parser: Parser,
        subscriptions: Rc<RefCell<HashMap<u64, mpsc::UnboundedSender<Message>>>>,
        pongs: Rc<RefCell<VecDeque<oneshot::Sender<()>>>>,
        server_info: Rc<RefCell<ServerInfo>>,
    ) -> Result<()> {
        debug_log!("NatsClient: Message processor started");

        loop {
            let data = transport.next_message().await?;

            if let Some(data) = data {
                debug_log!("NatsClient: Processing {} bytes", data.len());
                let ops = parser.parse(&data)?;
                debug_log!("NatsClient: Parsed {} operations", ops.len());

                for op in ops {
                    match op {
                        Op::Msg(msg, sid) | Op::HMsg(msg, sid) => {
                            debug_log!("NatsClient: Got MSG/HMSG for sid {}", sid);
                            let subs = subscriptions.borrow();
                            if let Some(sender) = subs.get(&sid) {
                                debug_log!("NatsClient: Delivering message to subscriber");
                                let _ = sender.unbounded_send(msg);
                            } else {
                                debug_log!("NatsClient: No subscriber for sid {}", sid);
                            }
                        }
                        Op::Ping => {
                            debug_log!("NatsClient: Got PING, sending PONG");
                            transport.send(protocol::PONG)?;
                        }
                        Op::Pong => {
                            debug_log!("NatsClient: Got PONG");
                            // Signal waiting flush() calls
                            let mut pongs_queue = pongs.borrow_mut();
                            if let Some(sender) = pongs_queue.pop_front() {
                                debug_log!("NatsClient: Signaling flush completion");
                                let _ = sender.send(());
                            }
                        }
                        Op::Ok => {
                            debug_log!("NatsClient: Got OK");
                        }
                        Op::Err(e) => {
                            if crate::error::is_fatal_server_error(&e) {
                                console_error!(
                                    "NatsClient: Fatal server error, closing connection: {}",
                                    e
                                );
                                let _ = transport.close();
                                return Err(NatsError::Server(e));
                            }
                            console_warn!("NatsClient: Server error: {}", e);
                        }
                        Op::Info(json) => {
                            debug_log!("NatsClient: Got async INFO update: {}", json);
                            match serde_json::from_str::<ServerInfo>(&json) {
                                Ok(new_info) => {
                                    *server_info.borrow_mut() = new_info;
                                    debug_log!("NatsClient: Server info updated");
                                }
                                Err(e) => {
                                    console_warn!("NatsClient: Failed to parse async INFO: {}", e);
                                }
                            }
                        }
                    }
                }
            } else {
                console_log!("NatsClient: Connection closed");
                break;
            }
        }
        Ok(())
    }

    fn check_payload_size(&self, size: usize) -> Result<()> {
        let max_payload = self.server_info.borrow().max_payload;
        if max_payload > 0 && size as i64 > max_payload {
            return Err(NatsError::Protocol(format!(
                "payload size {} exceeds server max_payload {}",
                size, max_payload
            )));
        }
        Ok(())
    }

    pub fn publish(&self, subject: &str, data: &[u8]) -> Result<()> {
        self.publish_inner(subject, None, None, data)
    }

    pub fn publish_with_reply(&self, subject: &str, reply: &str, data: &[u8]) -> Result<()> {
        self.publish_inner(subject, Some(reply), None, data)
    }

    pub fn publish_with_headers(
        &self,
        subject: &str,
        headers: &crate::headers::Headers,
        data: &[u8],
    ) -> Result<()> {
        self.publish_inner(subject, None, Some(headers), data)
    }

    pub fn publish_with_headers_and_reply(
        &self,
        subject: &str,
        reply: &str,
        headers: &crate::headers::Headers,
        data: &[u8],
    ) -> Result<()> {
        self.publish_inner(subject, Some(reply), Some(headers), data)
    }

    fn publish_inner(
        &self,
        subject: &str,
        reply: Option<&str>,
        headers: Option<&crate::headers::Headers>,
        data: &[u8],
    ) -> Result<()> {
        debug_log!("NatsClient: Publishing to {}", subject);
        let cmd = if let Some(headers) = headers {
            let encoded = headers.encode()?;
            self.check_payload_size(encoded.len() + data.len())?;
            protocol::build_hpub_cmd(subject, reply, &encoded, data)?
        } else {
            self.check_payload_size(data.len())?;
            protocol::build_pub_cmd(subject, reply, data)?
        };
        self.transport.send(&cmd)
    }

    pub async fn subscribe(&self, subject: &str) -> Result<SubscriptionHandle> {
        self.subscribe_with_queue(subject, None).await
    }

    pub async fn subscribe_with_queue(
        &self,
        subject: &str,
        queue: Option<&str>,
    ) -> Result<SubscriptionHandle> {
        debug_log!("NatsClient: Subscribing to {}", subject);

        let sid = {
            let mut next_sid = self.next_sid.borrow_mut();
            let sid = *next_sid;
            *next_sid += 1;
            sid
        };
        debug_log!("NatsClient: Assigned sid {}", sid);

        let (tx, rx) = mpsc::unbounded();

        {
            let mut subs = self.subscriptions.borrow_mut();
            subs.insert(sid, tx);
            debug_log!(
                "NatsClient: Registered subscription handler for sid {}",
                sid
            );
        }

        let cmd = protocol::build_sub_cmd(subject, queue, sid)?;
        debug_log!("NatsClient: Sending SUB command");
        self.transport.send(&cmd)?;

        Ok(SubscriptionHandle {
            sid,
            subject: subject.to_string(),
            queue: queue.map(|s| s.to_string()),
            receiver: rx,
            transport: self.transport.clone(),
            subscriptions: self.subscriptions.clone(),
            max_msgs: None,
            msg_count: 0,
            unsubscribed: false,
        })
    }

    pub async fn request(&self, subject: &str, data: &[u8]) -> Result<Message> {
        self.request_with_timeout(subject, data, 5000).await
    }

    pub async fn request_with_timeout(
        &self,
        subject: &str,
        data: &[u8],
        timeout_ms: u32,
    ) -> Result<Message> {
        self.request_inner(subject, None, data, timeout_ms).await
    }

    pub(crate) async fn request_inner(
        &self,
        subject: &str,
        headers: Option<&crate::headers::Headers>,
        data: &[u8],
        timeout_ms: u32,
    ) -> Result<Message> {
        debug_log!("NatsClient: Making request to {}", subject);
        self.ensure_response_mux().await?;

        let token = {
            let mut next = self.next_resp_token.borrow_mut();
            let token = *next;
            *next += 1;
            token
        };
        let reply = format!("{}.{}", self.inbox_prefix, token);

        let (tx, rx) = oneshot::channel();
        self.resp_map.borrow_mut().insert(token, tx);

        let published = match headers {
            Some(headers) => self.publish_with_headers_and_reply(subject, &reply, headers, data),
            None => self.publish_with_reply(subject, &reply, data),
        };
        if let Err(e) = published {
            self.resp_map.borrow_mut().remove(&token);
            return Err(e);
        }

        let timeout_ms = timeout_ms.min(i32::MAX as u32) as i32;
        let msg = match wasm_timeout(timeout_ms, rx).await {
            Ok(Ok(msg)) => msg,
            // Sender dropped: the mux pump exited because the connection closed
            Ok(Err(_)) => return Err(NatsError::Connection("connection closed".to_string())),
            Err(e) => {
                self.resp_map.borrow_mut().remove(&token);
                return Err(e);
            }
        };

        // Check for No Responders (503 status)
        if let Some(headers) = &msg.headers
            && headers.status_code() == Some(503)
        {
            return Err(NatsError::NoResponders);
        }

        Ok(msg)
    }

    /// Subscribe once to `<inbox_prefix>.*` and spawn a pump that routes
    /// responses to their per-request oneshot channels.
    async fn ensure_response_mux(&self) -> Result<()> {
        if *self.resp_mux_started.borrow() {
            return Ok(());
        }
        // Set before the await below so a concurrent request can't double-subscribe
        *self.resp_mux_started.borrow_mut() = true;

        let mux_subject = format!("{}.*", self.inbox_prefix);
        let mut sub = match self.subscribe(&mux_subject).await {
            Ok(sub) => sub,
            Err(e) => {
                *self.resp_mux_started.borrow_mut() = false;
                return Err(e);
            }
        };

        let resp_map = self.resp_map.clone();
        let token_start = self.inbox_prefix.len() + 1;
        worker::wasm_bindgen_futures::spawn_local(async move {
            while let Some(msg) = sub.next().await {
                let token = msg
                    .subject
                    .get(token_start..)
                    .and_then(|t| t.parse::<u64>().ok());
                if let Some(tx) = token.and_then(|t| resp_map.borrow_mut().remove(&t)) {
                    let _ = tx.send(msg);
                } else {
                    debug_log!(
                        "NatsClient: Dropping response with no pending request: {}",
                        msg.subject
                    );
                }
            }
            // Connection closed: fail all in-flight requests immediately
            resp_map.borrow_mut().clear();
        });

        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        debug_log!("NatsClient: Flushing - sending PING and waiting for PONG");

        let (tx, rx) = oneshot::channel();
        {
            self.pongs.borrow_mut().push_back(tx);
        }
        if let Err(e) = self.transport.send(protocol::PING) {
            // Remove the waiter we just queued, or the next flush's PONG
            // would pop it and desync the FIFO.
            self.pongs.borrow_mut().pop_back();
            return Err(e);
        }

        wasm_timeout(5000, rx)
            .await?
            .map_err(|_| NatsError::InvalidState("Flush cancelled".to_string()))?;

        debug_log!("NatsClient: Flush complete - PONG received");
        Ok(())
    }

    pub fn server_info(&self) -> ServerInfo {
        self.server_info.borrow().clone()
    }

    pub fn close(&self) -> Result<()> {
        if self.transport.is_closed() {
            return Ok(());
        }
        console_log!("NatsClient: Closing connection");
        // Wake pending subscribers/flushes immediately rather than waiting
        // for the close event to reach the message processor.
        self.subscriptions.borrow_mut().clear();
        self.pongs.borrow_mut().clear();
        self.transport.close()
    }
}

impl Drop for NatsClient {
    fn drop(&mut self) {
        // The message-processor task holds its own Rc<WsTransport> and is
        // parked in next_message() until the socket closes, so dropping the
        // client without closing would leave the socket and task alive
        // forever. Closing here unwinds both.
        let _ = self.close();
    }
}

pub struct SubscriptionHandle {
    sid: u64,
    subject: String,
    queue: Option<String>,
    receiver: mpsc::UnboundedReceiver<Message>,
    transport: Rc<WsTransport>,
    subscriptions: Rc<RefCell<HashMap<u64, mpsc::UnboundedSender<Message>>>>,
    max_msgs: Option<u64>,
    msg_count: u64,
    unsubscribed: bool,
}

impl SubscriptionHandle {
    pub async fn next(&mut self) -> Option<Message> {
        StreamExt::next(self).await
    }

    pub fn unsubscribe(&mut self) -> Result<()> {
        if self.unsubscribed {
            return Ok(());
        }
        self.unsubscribed = true;
        self.subscriptions.borrow_mut().remove(&self.sid);
        let cmd = protocol::build_unsub_cmd(self.sid, None);
        self.transport.send(&cmd)
    }

    pub fn unsubscribe_after(&mut self, max_msgs: u64) -> Result<()> {
        if max_msgs == 0 {
            return self.unsubscribe();
        }

        let cmd = protocol::build_unsub_cmd(self.sid, Some(max_msgs));
        self.transport.send(&cmd)?;
        self.max_msgs = Some(max_msgs);

        Ok(())
    }

    pub fn sid(&self) -> u64 {
        self.sid
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub fn queue(&self) -> Option<&str> {
        self.queue.as_deref()
    }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        if self.unsubscribed {
            return;
        }
        // Send UNSUB to server (ignore errors — we may already be disconnected)
        let cmd = protocol::build_unsub_cmd(self.sid, None);
        let _ = self.transport.send(&cmd);
        // Remove from subscriptions map to prevent leaking the entry
        self.subscriptions.borrow_mut().remove(&self.sid);
    }
}

impl Stream for SubscriptionHandle {
    type Item = Message;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let poll = this.receiver.poll_next_unpin(cx);

        if let std::task::Poll::Ready(Some(_)) = &poll {
            this.msg_count += 1;

            if let Some(max) = this.max_msgs
                && this.msg_count >= max
            {
                debug_log!(
                    "NatsClient: Auto-unsubscribing sid {} after {} messages",
                    this.sid,
                    this.msg_count
                );
                // The server already removed the subscription at max_msgs, so
                // no UNSUB is needed on drop.
                this.unsubscribed = true;
                this.subscriptions.borrow_mut().remove(&this.sid);
            }
        }

        poll
    }
}

pub(crate) fn generate_inbox_id() -> String {
    use wasm_bindgen::JsCast;

    let buf = js_sys::Uint8Array::new_with_length(16);
    let crypto =
        js_sys::Reflect::get(&js_sys::global(), &"crypto".into()).expect("crypto not available");
    let crypto: web_sys::Crypto = crypto.unchecked_into();
    crypto
        .get_random_values_with_array_buffer_view(&buf)
        .expect("getRandomValues failed");

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes: Vec<u8> = buf.to_vec();
    let mut out = String::with_capacity(32);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

/// Strip credentials from a URL for logging: drops the query string (tokens
/// often ride there) and masks any userinfo in the authority.
fn redact_url(url: &str) -> String {
    let base = url.split('?').next().unwrap_or(url);
    if let Some((scheme, rest)) = base.split_once("://") {
        let (authority, path) = match rest.split_once('/') {
            Some((authority, path)) => (authority, Some(path)),
            None => (rest, None),
        };
        if let Some((_credentials, host)) = authority.rsplit_once('@') {
            return match path {
                Some(path) => format!("{scheme}://***@{host}/{path}"),
                None => format!("{scheme}://***@{host}"),
            };
        }
    }
    base.to_string()
}

/// Race a future against a WASM setTimeout. Returns Err(Timeout) if the timer fires first.
pub(crate) async fn wasm_timeout<F: std::future::Future + Unpin>(
    timeout_ms: i32,
    future: F,
) -> Result<F::Output> {
    use futures::future::{Either, select};

    let timeout = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::new(&mut |resolve, _| {
        let global = js_sys::global();
        let set_timeout: js_sys::Function = js_sys::Reflect::get(&global, &"setTimeout".into())
            .unwrap()
            .into();
        let _ = set_timeout.call2(
            &wasm_bindgen::JsValue::NULL,
            &resolve,
            &wasm_bindgen::JsValue::from(timeout_ms),
        );
    }));

    match select(std::pin::pin!(future), std::pin::pin!(timeout)).await {
        Either::Left((result, _)) => Ok(result),
        Either::Right(_) => Err(NatsError::Timeout),
    }
}

#[cfg(test)]
mod tests {
    use super::redact_url;

    #[test]
    fn test_redact_url_plain() {
        assert_eq!(redact_url("wss://host:443"), "wss://host:443");
        assert_eq!(redact_url("wss://host/path"), "wss://host/path");
    }

    #[test]
    fn test_redact_url_strips_query() {
        assert_eq!(
            redact_url("wss://host/path?token=secret"),
            "wss://host/path"
        );
        assert_eq!(redact_url("wss://host?token=secret"), "wss://host");
    }

    #[test]
    fn test_redact_url_masks_userinfo() {
        assert_eq!(
            redact_url("wss://user:pass@host:8443/sub"),
            "wss://***@host:8443/sub"
        );
        assert_eq!(redact_url("nats://user:pass@host"), "nats://***@host");
        assert_eq!(redact_url("wss://user:p@ss@host?token=x"), "wss://***@host");
    }
}
