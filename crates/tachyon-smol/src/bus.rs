use std::sync::{Arc, Mutex};

use tachyon_ipc::{Bus, TachyonError};

use crate::receiver::BusReceiver;

/// Owned message representation safe to move across `.await` points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedMessage {
    pub type_id: u32,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub enum AsyncBusError {
    Tachyon(TachyonError),
    LockPoisoned,
}

impl std::fmt::Display for AsyncBusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tachyon(err) => write!(f, "tachyon error: {err}"),
            Self::LockPoisoned => write!(f, "async bus mutex was poisoned"),
        }
    }
}

impl std::error::Error for AsyncBusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Tachyon(err) => Some(err),
            Self::LockPoisoned => None,
        }
    }
}

impl From<TachyonError> for AsyncBusError {
    fn from(value: TachyonError) -> Self {
        Self::Tachyon(value)
    }
}

/// Conservative smol adapter over upstream `tachyon_ipc::Bus`.
///
/// The current upstream receive API is blocking, so `recv` uses `smol::unblock`
/// and returns an owned message copy.
#[derive(Clone)]
pub struct AsyncBus {
    inner: Arc<Mutex<Bus>>,
}

impl std::fmt::Debug for AsyncBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Bus does not implement Debug upstream; show only the type name.
        f.debug_struct("AsyncBus").finish_non_exhaustive()
    }
}

impl AsyncBus {
    /// Connect to an existing Tachyon socket path without blocking smol executors.
    pub async fn connect(socket_path: impl Into<String>) -> Result<Self, AsyncBusError> {
        let socket_path = socket_path.into();
        let bus = smol::unblock(move || Bus::connect(&socket_path)).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(bus)),
        })
    }

    /// Listen on a Tachyon socket path without blocking smol executors.
    pub async fn listen(
        socket_path: impl Into<String>,
        capacity: usize,
    ) -> Result<Self, AsyncBusError> {
        let socket_path = socket_path.into();
        let bus = smol::unblock(move || Bus::listen(&socket_path, capacity)).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(bus)),
        })
    }

    /// Preserve a synchronous send path for conservative MVP behavior.
    pub fn send(&self, data: &[u8], type_id: u32) -> Result<(), AsyncBusError> {
        let bus = self.inner.lock().map_err(|_| AsyncBusError::LockPoisoned)?;
        bus.send(data, type_id)?;
        Ok(())
    }

    /// Async receive bridge using a blocking receive call off the smol executor.
    pub async fn recv(&self, spin_threshold: u32) -> Result<OwnedMessage, AsyncBusError> {
        let inner = Arc::clone(&self.inner);

        smol::unblock(move || {
            let bus = inner.lock().map_err(|_| AsyncBusError::LockPoisoned)?;
            let guard = bus.acquire_rx(spin_threshold)?;

            let message = OwnedMessage {
                type_id: guard.type_id,
                payload: guard.data().to_vec(),
            };

            guard.commit()?;
            Ok::<OwnedMessage, AsyncBusError>(message)
        })
        .await
    }

    /// Convert this bus into a [`BusReceiver`] backed by a dedicated driver thread.
    ///
    /// The driver takes exclusive ownership of the upstream [`Bus`] handle and
    /// continuously calls `acquire_rx(spin_threshold)` in a loop, forwarding each
    /// received message as an [`OwnedMessage`] into a buffered channel with capacity
    /// `channel_capacity`.
    ///
    /// This is the recommended low-overhead receive path for steady-state
    /// consumption. It amortizes repeated offload overhead across many messages,
    /// unlike [`AsyncBus::recv`] which offloads one blocking receive per call.
    ///
    /// See [`BusReceiver`] for the full usage pattern and limitations.
    ///
    /// # Panics
    ///
    /// Panics if there are outstanding [`AsyncBus`] clones sharing the same inner
    /// handle (i.e., if the internal `Arc` reference count is not 1). Drop all other
    /// clones before calling `into_receiver`.
    pub fn into_receiver(self, spin_threshold: u32, channel_capacity: usize) -> BusReceiver {
        let bus = Arc::try_unwrap(self.inner)
            .unwrap_or_else(|_| {
                panic!(
                    "into_receiver requires exclusive ownership; \
                     all AsyncBus clones must be dropped first"
                )
            })
            .into_inner()
            .expect("AsyncBus mutex was poisoned");

        let (tx, rx) = smol::channel::bounded(channel_capacity);

        let driver = std::thread::spawn(move || loop {
            match bus.acquire_rx(spin_threshold) {
                Ok(guard) => {
                    let msg = OwnedMessage {
                        type_id: guard.type_id,
                        payload: guard.data().to_vec(),
                    };
                    if let Err(e) = guard.commit() {
                        let _ = tx.send_blocking(Err(AsyncBusError::from(e)));
                        break;
                    }
                    if tx.send_blocking(Ok(msg)).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx.send_blocking(Err(AsyncBusError::from(e)));
                    break;
                }
            }
        });

        BusReceiver::new(rx, driver)
    }
}

#[cfg(test)]
mod tests {
    use super::AsyncBus;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_socket(name: &str) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before epoch")
            .as_nanos();
        format!("/tmp/tachyon_smol_{name}_{ts}.sock")
    }

    #[test]
    fn async_recv_returns_owned_message() {
        smol::block_on(async {
            let socket_path = unique_socket("recv");
            let _ = std::fs::remove_file(&socket_path);

            let server_path = socket_path.clone();
            let client_path = socket_path.clone();

            let server = smol::spawn(async move {
                let bus = AsyncBus::listen(server_path, 1 << 16).await.unwrap();
                let msg = bus.recv(10_000).await.unwrap();
                assert_eq!(msg.type_id, 7);
                assert_eq!(msg.payload, b"smol-mvp");
            });

            let client = smol::spawn(async move {
                smol::Timer::after(std::time::Duration::from_millis(20)).await;
                let bus = AsyncBus::connect(client_path).await.unwrap();
                bus.send(b"smol-mvp", 7).unwrap();
            });

            server.await;
            client.await;
            let _ = std::fs::remove_file(&socket_path);
        });
    }
}
