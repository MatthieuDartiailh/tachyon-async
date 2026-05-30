/// Phase 4 low-overhead Tokio example.
///
/// Demonstrates the recommended receive pattern using [`BusReceiver`]:
///
/// 1. Convert an `AsyncBus` into a `BusReceiver` backed by a dedicated driver thread.
/// 2. Await the first message with `recv().await`.
/// 3. Drain any additionally buffered messages synchronously with
///    `try_recv_buffered()` before yielding back to the executor.
///
/// This pattern amortizes `spawn_blocking` overhead across bursts of messages and
/// is lower-overhead than calling `AsyncBus::recv` for each individual message.
use std::time::Duration;

use tachyon_tokio::{AsyncBus, TryRecvBufferedError};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket_path = "/tmp/tachyon_tokio_low_overhead_example.sock";
    let _ = std::fs::remove_file(socket_path);

    // --- Server side: convert bus to a dedicated BusReceiver ---
    let server_path = socket_path.to_string();
    let server = tokio::spawn(async move {
        let bus = AsyncBus::listen(server_path, 1 << 16).await?;

        // `into_receiver` moves the bus into a background driver thread that
        // continuously calls the blocking `acquire_rx` and forwards messages
        // into the channel. The channel capacity (64) controls how many
        // messages can be buffered before the driver blocks on the channel.
        let mut receiver = bus.into_receiver(10_000, 64);

        let mut total = 0usize;
        loop {
            // Step 1: Suspend until at least one message is ready.
            // This is the async "wait for readiness + consume" step.
            let Some(result) = receiver.recv().await else {
                break; // driver stopped
            };
            let msg = result?;
            println!(
                "[server] recv type_id={} payload={:?}",
                msg.type_id,
                std::str::from_utf8(&msg.payload).unwrap_or("<binary>")
            );
            total += 1;

            // Step 2: Drain any additionally buffered messages synchronously
            // without suspending again, reducing round-trip overhead on bursts.
            loop {
                match receiver.try_recv_buffered() {
                    Ok(extra) => {
                        println!(
                            "[server] buffered type_id={} payload={:?}",
                            extra.type_id,
                            std::str::from_utf8(&extra.payload).unwrap_or("<binary>")
                        );
                        total += 1;
                        if total >= 3 {
                            println!("[server] received all 3 messages, done.");
                            return Ok::<(), tachyon_tokio::AsyncBusError>(());
                        }
                    }
                    Err(TryRecvBufferedError::Empty) => break,
                    Err(TryRecvBufferedError::Disconnected) => {
                        return Ok(());
                    }
                }
            }

            if total >= 3 {
                break;
            }
        }

        println!("[server] done. total={total}");
        Ok(())
    });

    // Give the server a moment to start listening.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // --- Client side: send three messages ---
    let client = AsyncBus::connect(socket_path).await?;
    for i in 1u32..=3 {
        let payload = format!("message-{i}");
        client.send(payload.as_bytes(), i)?;
        println!("[client] sent type_id={i} payload={payload:?}");
    }

    server.await??;
    let _ = std::fs::remove_file(socket_path);
    Ok(())
}
