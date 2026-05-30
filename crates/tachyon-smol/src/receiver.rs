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
/// Created via [`crate::AsyncBus::into_receiver`]. The driver thread takes exclusive
/// ownership of the upstream `tachyon_ipc::Bus` handle and continuously calls the
/// blocking `acquire_rx(spin_threshold)` in a loop, forwarding each received message
/// as an [`OwnedMessage`] into an internal bounded channel.
///
/// This mirrors the Tokio low-overhead receive strategy while using smol-friendly
/// primitives.
pub struct BusReceiver {
    rx: smol::channel::Receiver<Result<OwnedMessage, AsyncBusError>>,
    _driver: std::thread::JoinHandle<()>,
}

impl BusReceiver {
    pub(crate) fn new(
        rx: smol::channel::Receiver<Result<OwnedMessage, AsyncBusError>>,
        driver: std::thread::JoinHandle<()>,
    ) -> Self {
        Self {
            rx,
            _driver: driver,
        }
    }

    /// Receive the next message, suspending the current task until one is available.
    ///
    /// Returns `None` when the driver has stopped and all buffered messages have
    /// been consumed.
    pub async fn recv(&mut self) -> Option<Result<OwnedMessage, AsyncBusError>> {
        self.rx.recv().await.ok()
    }

    /// Try to consume a buffered message without suspending.
    pub fn try_recv_buffered(&mut self) -> Result<OwnedMessage, TryRecvBufferedError> {
        match self.rx.try_recv() {
            Ok(Ok(msg)) => Ok(msg),
            Ok(Err(_bus_err)) => Err(TryRecvBufferedError::Disconnected),
            Err(smol::channel::TryRecvError::Empty) => Err(TryRecvBufferedError::Empty),
            Err(smol::channel::TryRecvError::Closed) => Err(TryRecvBufferedError::Disconnected),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BusReceiver, TryRecvBufferedError};
    use crate::bus::AsyncBus;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_socket(name: &str) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before epoch")
            .as_nanos();
        format!("/tmp/tachyon_smol_receiver_{name}_{ts}.sock")
    }

    #[test]
    fn bus_receiver_recv_owned_message() {
        smol::block_on(async {
            let socket_path = unique_socket("recv");
            let _ = std::fs::remove_file(&socket_path);

            let server_path = socket_path.clone();
            let client_path = socket_path.clone();

            let server = smol::spawn(async move {
                let bus = AsyncBus::listen(server_path, 1 << 16).await.unwrap();
                let mut receiver: BusReceiver = bus.into_receiver(10_000, 32);
                let msg = receiver.recv().await.unwrap().unwrap();
                assert_eq!(msg.type_id, 42);
                assert_eq!(msg.payload, b"low-overhead");
            });

            let client = smol::spawn(async move {
                smol::Timer::after(std::time::Duration::from_millis(20)).await;
                let bus = AsyncBus::connect(client_path).await.unwrap();
                bus.send(b"low-overhead", 42).unwrap();
            });

            server.await;
            client.await;
            let _ = std::fs::remove_file(&socket_path);
        });
    }

    #[test]
    fn bus_receiver_try_recv_buffered_empty_before_message() {
        smol::block_on(async {
            let socket_path = unique_socket("try-recv");
            let _ = std::fs::remove_file(&socket_path);

            let server_path = socket_path.clone();
            let client_path = socket_path.clone();

            let server = smol::spawn(async move {
                let bus = AsyncBus::listen(server_path, 1 << 16).await.unwrap();
                let mut receiver: BusReceiver = bus.into_receiver(10_000, 32);

                assert_eq!(
                    receiver.try_recv_buffered(),
                    Err(TryRecvBufferedError::Empty)
                );

                let first = receiver.recv().await.unwrap().unwrap();
                assert_eq!(first.type_id, 1);

                loop {
                    match receiver.try_recv_buffered() {
                        Ok(_extra) => {}
                        Err(TryRecvBufferedError::Empty) => break,
                        Err(TryRecvBufferedError::Disconnected) => break,
                    }
                }
            });

            let client = smol::spawn(async move {
                smol::Timer::after(std::time::Duration::from_millis(20)).await;
                let bus = AsyncBus::connect(client_path).await.unwrap();
                bus.send(b"first", 1).unwrap();
            });

            server.await;
            client.await;
            let _ = std::fs::remove_file(&socket_path);
        });
    }
}
