/// Phase 5 low-overhead smol example.
///
/// Demonstrates the recommended receive pattern using [`BusReceiver`]:
///
/// 1. Convert an `AsyncBus` into a `BusReceiver` backed by a dedicated driver thread.
/// 2. Await the first message with `recv().await`.
/// 3. Drain any additionally buffered messages synchronously with
///    `try_recv_buffered()` before yielding back to the executor.
use std::time::Duration;

use tachyon_smol::{AsyncBus, TryRecvBufferedError};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    smol::block_on(async {
        let socket_path = "/tmp/tachyon_smol_low_overhead.sock";
        let _ = std::fs::remove_file(socket_path);

        let server_path = socket_path.to_string();
        let server = smol::spawn(async move {
            let bus = AsyncBus::listen(server_path, 1 << 16).await?;
            let mut receiver = bus.into_receiver(10_000, 64);

            let mut total = 0u32;
            while let Some(result) = receiver.recv().await {
                let msg = result?;
                println!(
                    "[server] recv type_id={} payload={:?}",
                    msg.type_id,
                    std::str::from_utf8(&msg.payload).unwrap_or("<binary>")
                );
                total += 1;

                loop {
                    match receiver.try_recv_buffered() {
                        Ok(extra) => {
                            println!(
                                "[server] buffered type_id={} payload={:?}",
                                extra.type_id,
                                std::str::from_utf8(&extra.payload).unwrap_or("<binary>")
                            );
                            total += 1;
                        }
                        Err(TryRecvBufferedError::Empty) => break,
                        Err(TryRecvBufferedError::Disconnected) => break,
                    }
                }

                if total >= 3 {
                    break;
                }
            }

            Ok::<(), tachyon_smol::AsyncBusError>(())
        });

        smol::Timer::after(Duration::from_millis(20)).await;

        let client = AsyncBus::connect(socket_path).await?;
        for i in 1u32..=3 {
            let payload = format!("message-{i}");
            client.send(payload.as_bytes(), i)?;
            println!("[client] sent type_id={i} payload={payload:?}");
        }

        server.await?;
        let _ = std::fs::remove_file(socket_path);
        Ok(())
    })
}
