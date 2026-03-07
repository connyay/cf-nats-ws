use crate::error::{NatsError, Result};
use futures::StreamExt;
use futures::channel::mpsc;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use worker::{WebSocket, WebsocketEvent, console_error};

pub struct WsTransport {
    ws: Arc<WebSocket>,
    receiver: Rc<RefCell<mpsc::UnboundedReceiver<Vec<u8>>>>,
}

impl WsTransport {
    pub async fn connect(url: &str) -> Result<Self> {
        debug_log!("WsTransport: Connecting to {}", url);
        let parsed_url = url
            .parse()
            .map_err(|_| NatsError::Connection(format!("Invalid URL: {url}")))?;

        let ws = WebSocket::connect(parsed_url)
            .await
            .map_err(|e| NatsError::WebSocket(format!("Failed to connect: {e}")))?;

        // Clone for the event handler BEFORE accepting
        let ws_events = ws.clone();
        let ws = Arc::new(ws);

        // Create a channel for messages
        let (tx, rx) = mpsc::unbounded();

        // Set up event handler BEFORE accepting connection
        worker::wasm_bindgen_futures::spawn_local(async move {
            debug_log!("WsTransport: Setting up event handler");
            let mut events = match ws_events.events() {
                Ok(e) => e,
                Err(e) => {
                    console_error!("WsTransport: Failed to get event stream: {:?}", e);
                    return;
                }
            };

            debug_log!("WsTransport: Event handler ready, listening for events");
            while let Some(event) = events.next().await {
                match event {
                    Ok(WebsocketEvent::Message(msg)) => {
                        if let Some(bytes) = msg.bytes() {
                            debug_log!("WsTransport: Received {} bytes", bytes.len());
                            if tx.unbounded_send(bytes).is_err() {
                                console_error!("WsTransport: Failed to send message to channel");
                                break;
                            }
                        }
                    }
                    Ok(WebsocketEvent::Close(e)) => {
                        console_error!("WsTransport: WebSocket closed: {:?}", e);
                        break;
                    }
                    Err(e) => {
                        console_error!("WsTransport: Event error: {:?}", e);
                        break;
                    }
                }
            }
            debug_log!("WsTransport: Event handler exiting");
        });

        // NOW accept the connection
        ws.accept()
            .map_err(|e| NatsError::WebSocket(format!("Failed to accept connection: {e}")))?;
        debug_log!("WsTransport: Connection accepted");

        Ok(Self {
            ws,
            receiver: Rc::new(RefCell::new(rx)),
        })
    }

    pub fn send(&self, data: &[u8]) -> Result<()> {
        debug_log!("WsTransport: Sending {} bytes", data.len());
        self.ws
            .send_with_bytes(data)
            .map_err(|e| NatsError::WebSocket(format!("Send failed: {e}")))
    }

    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn next_message(&self) -> Result<Option<Vec<u8>>> {
        // In WASM, we're single-threaded, so RefCell is safe
        let mut receiver = self.receiver.borrow_mut();
        let msg = receiver.next().await;
        if let Some(_data) = &msg {
            debug_log!("WsTransport: next_message returning {} bytes", _data.len());
        }
        Ok(msg)
    }

    pub fn close(&self) -> Result<()> {
        debug_log!("WsTransport: Closing WebSocket");
        self.ws
            .close::<String>(None, None)
            .map_err(|e| NatsError::WebSocket(format!("Close failed: {e}")))
    }
}
