use crate::error::{NatsError, Result};
use futures::StreamExt;
use futures::channel::mpsc;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use worker::{WebSocket, WebsocketEvent, console_error};

pub struct WsTransport {
    ws: Arc<WebSocket>,
    receiver: Rc<RefCell<mpsc::UnboundedReceiver<Vec<u8>>>>,
    closed_locally: Rc<Cell<bool>>,
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

        Self::from_websocket(ws)
    }

    /// Build a NATS transport around an already-connected Workers WebSocket.
    ///
    /// This is useful when the connection must be established through a
    /// binding (for example, a Workers VPC Service) instead of the global
    /// WebSocket constructor.
    ///
    /// The socket must not have been accepted yet and its `events()` stream
    /// must not have been taken — `accept()` is called internally, and fails
    /// on a socket the caller already accepted.
    pub fn from_websocket(ws: WebSocket) -> Result<Self> {
        // Clone for the event handler BEFORE accepting
        let ws_events = ws.clone();
        let ws = Arc::new(ws);

        // Create a channel for messages
        let (tx, rx) = mpsc::unbounded();

        let closed_locally = Rc::new(Cell::new(false));
        let closed_locally_events = closed_locally.clone();

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
                    Ok(WebsocketEvent::Message(msg)) => match message_data(&msg).await {
                        Ok(Some(data)) => {
                            debug_log!("WsTransport: Received {} bytes", data.len());
                            if tx.unbounded_send(data).is_err() {
                                console_error!("WsTransport: Failed to send message to channel");
                                break;
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            console_error!("WsTransport: Failed to read message: {:?}", e);
                            break;
                        }
                    },
                    Ok(WebsocketEvent::Close(e)) => {
                        if closed_locally_events.get() {
                            debug_log!("WsTransport: WebSocket closed: {:?}", e);
                        } else {
                            console_error!("WsTransport: WebSocket closed: {:?}", e);
                        }
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
            closed_locally,
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
        if self.closed_locally.replace(true) {
            return Ok(());
        }
        debug_log!("WsTransport: Closing WebSocket");
        self.ws
            .close::<String>(None, None)
            .map_err(|e| NatsError::WebSocket(format!("Close failed: {e}")))
    }

    /// Whether close() has been called locally. A close initiated by the
    /// server is not reflected here.
    pub fn is_closed(&self) -> bool {
        self.closed_locally.get()
    }
}

impl Drop for WsTransport {
    fn drop(&mut self) {
        // Close the socket so the event-handler task exits instead of
        // keeping the WebSocket (and the Worker invocation) alive.
        let _ = self.close();
    }
}

async fn message_data(msg: &worker::MessageEvent) -> Result<Option<Vec<u8>>> {
    let value = msg.as_ref().data();

    if value.is_instance_of::<js_sys::ArrayBuffer>() {
        return Ok(Some(js_sys::Uint8Array::new(&value).to_vec()));
    }

    if let Some(text) = value.as_string() {
        return Ok(Some(text.into_bytes()));
    }

    if value.is_instance_of::<web_sys::Blob>() {
        let blob: web_sys::Blob = value.unchecked_into();
        let buffer = JsFuture::from(blob.array_buffer())
            .await
            .map_err(|e| NatsError::WebSocket(format!("Failed to read Blob: {e:?}")))?;
        return Ok(Some(js_sys::Uint8Array::new(&buffer).to_vec()));
    }

    Ok(None)
}
