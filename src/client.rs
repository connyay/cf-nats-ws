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
use worker::{console_error, console_log, console_warn};

pub struct NatsClient {
    transport: Rc<WsTransport>,
    server_info: Rc<RefCell<ServerInfo>>,
    subscriptions: Rc<RefCell<HashMap<u64, mpsc::UnboundedSender<Message>>>>,
    next_sid: Rc<RefCell<u64>>,
    pongs: Rc<RefCell<VecDeque<oneshot::Sender<()>>>>,
}

impl NatsClient {
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_options(url, ClientOptions::default()).await
    }

    pub async fn connect_with_options(url: &str, options: ClientOptions) -> Result<Self> {
        console_log!("NatsClient: Starting connection to {}", url);

        // Convert URL to WebSocket URL if needed
        let ws_url = if url.starts_with("ws://") || url.starts_with("wss://") {
            url.to_string()
        } else if url.starts_with("nats://") {
            url.replace("nats://", "ws://")
        } else if url.starts_with("nats+tls://") {
            url.replace("nats+tls://", "wss://")
        } else if url.starts_with("tls://") {
            url.replace("tls://", "wss://")
        } else {
            format!("wss://{url}")
        };

        debug_log!(
            "NatsClient: Connecting to WebSocket URL: {}",
            ws_url.split('?').next().unwrap_or(&ws_url)
        );
        let transport = WsTransport::connect(&ws_url).await?;
        let mut parser = Parser::new();

        debug_log!("NatsClient: Waiting for INFO message");
        // Wait for INFO message
        let mut server_info = None;
        let mut attempts = 0;
        while server_info.is_none() && attempts < 10 {
            attempts += 1;
            debug_log!("NatsClient: Attempt {} to get INFO message", attempts);

            if let Some(data) = transport.next_message().await? {
                debug_log!("NatsClient: Received data: {} bytes", data.len());
                let ops = parser.parse(&data)?;
                debug_log!("NatsClient: Parsed {} operations", ops.len());

                for op in ops {
                    match &op {
                        Op::Info(json) => {
                            debug_log!("NatsClient: Got INFO: {}", json);
                            let info: ServerInfo = serde_json::from_str(json)?;
                            server_info = Some(info);
                            break;
                        }
                        _ => {
                            console_warn!("NatsClient: Got unexpected op: {:?}", op);
                        }
                    }
                }
            } else {
                // Small delay before retry
                debug_log!("NatsClient: No message yet, waiting...");
            }
        }

        let server_info = server_info.ok_or_else(|| {
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
        };

        // Start message processing task
        let transport_clone = transport.clone();
        let subs_clone = subscriptions.clone();
        let pongs_clone = pongs.clone();
        let info_clone = server_info.clone();
        worker::wasm_bindgen_futures::spawn_local(async move {
            debug_log!("NatsClient: Starting message processor");
            if let Err(e) =
                Self::process_messages(transport_clone, subs_clone, pongs_clone, info_clone).await
            {
                console_log!("NatsClient: Message processing error: {:?}", e);
            }
        });

        Ok(client)
    }

    async fn process_messages(
        transport: Rc<WsTransport>,
        subscriptions: Rc<RefCell<HashMap<u64, mpsc::UnboundedSender<Message>>>>,
        pongs: Rc<RefCell<VecDeque<oneshot::Sender<()>>>>,
        server_info: Rc<RefCell<ServerInfo>>,
    ) -> Result<()> {
        let mut parser = Parser::new();
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
            let encoded = headers.encode();
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
            msg_count: Rc::new(RefCell::new(0)),
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
        debug_log!("NatsClient: Making request to {}", subject);
        let inbox = format!("_INBOX.{}", generate_inbox_id());

        let mut sub = self.subscribe(&inbox).await?;
        self.publish_with_reply(subject, &inbox, data)?;

        let msg = wasm_timeout(timeout_ms as i32, Box::pin(sub.next()))
            .await?
            .ok_or(NatsError::Timeout)?;

        // Check for No Responders (503 status)
        if let Some(headers) = &msg.headers
            && headers.status_code() == Some(503)
        {
            return Err(NatsError::NoResponders);
        }

        Ok(msg)
    }

    pub async fn flush(&self) -> Result<()> {
        debug_log!("NatsClient: Flushing - sending PING and waiting for PONG");

        let (tx, rx) = oneshot::channel();
        {
            self.pongs.borrow_mut().push_back(tx);
        }
        self.transport.send(protocol::PING)?;

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
        console_log!("NatsClient: Closing connection");
        self.transport.close()
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
    msg_count: Rc<RefCell<u64>>,
}

impl SubscriptionHandle {
    pub async fn next(&mut self) -> Option<Message> {
        StreamExt::next(self).await
    }

    pub fn unsubscribe(&mut self) -> Result<()> {
        let cmd = protocol::build_unsub_cmd(self.sid, None);
        self.transport.send(&cmd)?;
        let mut subs = self.subscriptions.borrow_mut();
        subs.remove(&self.sid);
        Ok(())
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
        // Send UNSUB to server (ignore errors — we may already be disconnected)
        let cmd = protocol::build_unsub_cmd(self.sid, None);
        let _ = self.transport.send(&cmd);
        // Remove from subscriptions map to prevent leaking the entry
        let mut subs = self.subscriptions.borrow_mut();
        subs.remove(&self.sid);
    }
}

impl Stream for SubscriptionHandle {
    type Item = Message;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let poll = self.receiver.poll_next_unpin(cx);

        if let std::task::Poll::Ready(Some(_)) = &poll {
            let mut count = self.msg_count.borrow_mut();
            *count += 1;

            if let Some(max) = self.max_msgs
                && *count >= max
            {
                debug_log!(
                    "NatsClient: Auto-unsubscribing sid {} after {} messages",
                    self.sid,
                    *count
                );
                let mut subs = self.subscriptions.borrow_mut();
                subs.remove(&self.sid);
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

/// Race a future against a WASM setTimeout. Returns Err(Timeout) if the timer fires first.
async fn wasm_timeout<F: std::future::Future + Unpin>(
    timeout_ms: i32,
    future: F,
) -> Result<F::Output> {
    use futures::future::{Either, select};
    use wasm_bindgen::JsCast;
    use wasm_bindgen::prelude::*;

    let (timeout_tx, timeout_rx) = futures::channel::oneshot::channel::<()>();
    let timeout_tx = Rc::new(RefCell::new(Some(timeout_tx)));

    let closure = Closure::once(move || {
        if let Some(tx) = timeout_tx.borrow_mut().take() {
            let _ = tx.send(());
        }
    });

    let timeout_id = worker::js_sys::global()
        .unchecked_into::<web_sys::WorkerGlobalScope>()
        .set_timeout_with_callback_and_timeout_and_arguments_0(
            closure.as_ref().unchecked_ref(),
            timeout_ms,
        )
        .map_err(|_| NatsError::InvalidState("Failed to set timeout".to_string()))?;
    // Closure::once is consumed when the timeout fires, but if the future resolves first,
    // JS still holds a reference — forget() prevents invalidation in WASM.
    closure.forget();

    match select(std::pin::pin!(future), timeout_rx).await {
        Either::Left((result, _)) => {
            worker::js_sys::global()
                .unchecked_into::<web_sys::WorkerGlobalScope>()
                .clear_timeout_with_handle(timeout_id);
            Ok(result)
        }
        Either::Right(_) => Err(NatsError::Timeout),
    }
}
