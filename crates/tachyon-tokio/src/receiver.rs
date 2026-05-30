use tokio::sync::mpsc;

use crate::bus::{AsyncBusError, OwnedMessage};

/// Error returned by [`BusReceiver::try_recv_buffered`].
///
/// Distinguishes between "no message queued yet" and "driver has stopped".
#[derive(Debug, PartialEq, Eq)]
pub enum TryRecvBufferedError {
    /// No message is currently buffered; the driver thread is still running.
    Empty,
    /// The driver thread has stopped; no more messages will be received.
    Disconnected,
}

impl std::fmt::Display for TryRecvBufferedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "no message buffered"),
            Self::Disconnected => write!(f, "bus receiver driver has stopped"),
        }
    }
}

impl std::error::Error for TryRecvBufferedError {}

/// A dedicated receive stream backed by an internal driver thread.
///
/// # Phase 4 low-overhead receive path
///
/// Created via [`crate::AsyncBus::into_receiver`]. The driver thread takes exclusive
/// ownership of the upstream `tachyon_ipc::Bus` handle and continuously calls
/// the blocking `acquire_rx(spin_threshold)` in a loop, forwarding each received
/// message as an [`OwnedMessage`] into an internal `tokio::sync::mpsc` channel.
///
/// ## Why this is lower-overhead than the Phase 3 path
///
/// The Phase 3 [`crate::AsyncBus::recv`] path spawns a new `tokio::task::spawn_blocking`
/// task for **every** message received. Each spawn carries task-creation and
/// thread-handoff overhead. `BusReceiver` amortizes that cost across many messages
/// because the driver thread runs continuously and never re-spawns.
///
/// ## Why this is not true zero-copy
///
/// The upstream `tachyon-ipc` receive API (as of 0.5.1) couples message acquisition
/// and consumption in a single blocking call (`acquire_rx`). There is no Rust API
/// for separate readiness probing followed by a zero-copy consume. The driver thread
/// therefore still copies the payload out of the upstream guard into an owned
/// `Vec<u8>` before releasing the upstream slot and forwarding to the channel.
///
/// See `docs/tokio-low-overhead.md` for a full explanation of the compromise and
/// what upstream additions would enable true zero-copy in a future phase.
///
/// ## Recommended usage pattern
///
/// ```rust,no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # use tachyon_tokio::AsyncBus;
/// let bus = AsyncBus::listen("/tmp/my.sock", 1 << 16).await?;
/// let mut receiver = bus.into_receiver(10_000, 64);
///
/// // Suspend until the first message arrives (replaces `readable().await`).
/// while let Some(result) = receiver.recv().await {
///     let msg = result?;
///     println!("type={} len={}", msg.type_id, msg.payload.len());
///
///     // After the first message wakes us, drain any additional buffered messages
///     // synchronously before yielding back to the executor.
///     loop {
///         match receiver.try_recv_buffered() {
///             Ok(extra) => println!("  buffered: type={}", extra.type_id),
///             Err(tachyon_tokio::TryRecvBufferedError::Empty) => break,
///             Err(tachyon_tokio::TryRecvBufferedError::Disconnected) => return Ok(()),
///         }
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// ## Cleanup
///
/// When this value is dropped, the internal channel closes. The driver thread exits
/// on its next iteration when it detects the channel is closed (i.e., after it
/// successfully receives one more message from upstream and tries to send it).
/// Because the upstream `acquire_rx` API has no cancellation mechanism, the driver
/// may remain blocked in `acquire_rx` until the next message arrives; it is a plain
/// OS thread and does not prevent the Tokio runtime from shutting down.
pub struct BusReceiver {
    rx: mpsc::Receiver<Result<OwnedMessage, AsyncBusError>>,
    // Plain OS thread; not joined on drop. Does not block Tokio runtime shutdown.
    _driver: std::thread::JoinHandle<()>,
}

impl BusReceiver {
    pub(crate) fn new(
        rx: mpsc::Receiver<Result<OwnedMessage, AsyncBusError>>,
        driver: std::thread::JoinHandle<()>,
    ) -> Self {
        Self {
            rx,
            _driver: driver,
        }
    }

    /// Receive the next message, suspending the current task until one is available.
    ///
    /// This is the primary async receive entry-point. It corresponds to the
    /// `readable().await` + consume step of the desired low-overhead pattern:
    /// because the driver runs in the background, the channel is already populated
    /// as soon as data arrives upstream, so the suspension time is minimal.
    ///
    /// Returns `None` when the driver has stopped and all buffered messages have
    /// been consumed.
    pub async fn recv(&mut self) -> Option<Result<OwnedMessage, AsyncBusError>> {
        self.rx.recv().await
    }

    /// Try to consume a buffered message without suspending.
    ///
    /// Use this in a tight loop after [`recv`][Self::recv] returns to drain
    /// any additional messages that accumulated in the channel buffer while the
    /// task was processing the previous message. This avoids extra `.await`
    /// suspension overhead when bursts of messages arrive close together.
    ///
    /// Returns:
    /// - `Ok(msg)` — a message was available in the buffer.
    /// - `Err(Empty)` — no message is currently buffered; call [`recv`][Self::recv]
    ///   to wait for the next one.
    /// - `Err(Disconnected)` — the driver has stopped; no further messages will arrive.
    pub fn try_recv_buffered(&mut self) -> Result<OwnedMessage, TryRecvBufferedError> {
        match self.rx.try_recv() {
            Ok(Ok(msg)) => Ok(msg),
            Ok(Err(_bus_err)) => {
                // The driver forwarded a transport error; treat as disconnected.
                Err(TryRecvBufferedError::Disconnected)
            }
            Err(mpsc::error::TryRecvError::Empty) => Err(TryRecvBufferedError::Empty),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(TryRecvBufferedError::Disconnected),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BusReceiver, TryRecvBufferedError};
    use crate::bus::AsyncBus;
    use std::time::{SystemTime, UNIX_EPOCH};

    const CONNECT_RETRY_ATTEMPTS: usize = 200;
    const CONNECT_RETRY_DELAY_MS: u64 = 1;
    const CONNECT_RETRY_DELAY: std::time::Duration =
        std::time::Duration::from_millis(CONNECT_RETRY_DELAY_MS);
    const CONNECT_TOTAL_WAIT: std::time::Duration =
        std::time::Duration::from_millis((CONNECT_RETRY_ATTEMPTS as u64) * CONNECT_RETRY_DELAY_MS);

    fn unique_socket(name: &str) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before epoch")
            .as_nanos();
        format!("/tmp/tachyon_receiver_{name}_{ts}.sock")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bus_receiver_recv_owned_message() {
        let socket_path = unique_socket("recv");
        let _ = std::fs::remove_file(&socket_path);

        let server_path = socket_path.clone();
        let client_path = socket_path.clone();

        let server = tokio::spawn(async move {
            let bus = AsyncBus::listen(server_path, 1 << 16).await.unwrap();
            let mut receiver: BusReceiver = bus.into_receiver(10_000, 32);
            let msg = receiver.recv().await.unwrap().unwrap();
            assert_eq!(msg.type_id, 42);
            assert_eq!(msg.payload, b"low-overhead");
        });

        let client = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let bus = AsyncBus::connect(client_path).await.unwrap();
            bus.send(b"low-overhead", 42).unwrap();
        });

        server.await.unwrap();
        client.await.unwrap();
        let _ = std::fs::remove_file(&socket_path);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bus_receiver_try_recv_buffered_empty_before_message() {
        let socket_path = unique_socket("try-recv");
        let _ = std::fs::remove_file(&socket_path);

        let server_path = socket_path.clone();
        let client_path = socket_path.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        let server = tokio::spawn(async move {
            let bus = AsyncBus::listen(server_path, 1 << 16).await.unwrap();
            let mut receiver: BusReceiver = bus.into_receiver(10_000, 32);

            // Before any message has arrived the channel is empty.
            assert_eq!(
                receiver.try_recv_buffered(),
                Err(TryRecvBufferedError::Empty)
            );
            let _ = ready_tx.send(());

            // Block until the first message arrives.
            let first = receiver.recv().await.unwrap().unwrap();
            assert_eq!(first.type_id, 1);

            // Drain any additionally buffered messages synchronously.
            loop {
                match receiver.try_recv_buffered() {
                    Ok(_extra) => {} // consume silently
                    Err(TryRecvBufferedError::Empty) => break,
                    Err(TryRecvBufferedError::Disconnected) => break,
                }
            }
        });

        let client = tokio::spawn(async move {
            let mut bus = None;
            let mut last_connect_error = None;
            for _ in 0..CONNECT_RETRY_ATTEMPTS {
                match AsyncBus::connect(client_path.clone()).await {
                    Ok(connected) => {
                        bus = Some(connected);
                        break;
                    }
                    Err(err) => {
                        last_connect_error = Some(err.to_string());
                        tokio::time::sleep(CONNECT_RETRY_DELAY).await;
                    }
                }
            }
            let bus = bus.unwrap_or_else(|| {
                panic!(
                    "client failed to connect to server after {CONNECT_RETRY_ATTEMPTS} attempts over {CONNECT_TOTAL_WAIT:?}; last error: {}",
                    last_connect_error.unwrap_or_else(|| "unknown".to_string())
                )
            });
            ready_rx.await.unwrap();
            bus.send(b"first", 1).unwrap();
        });

        server.await.unwrap();
        client.await.unwrap();
        let _ = std::fs::remove_file(&socket_path);
    }
}
