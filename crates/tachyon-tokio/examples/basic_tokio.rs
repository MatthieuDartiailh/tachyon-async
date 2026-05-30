use std::time::Duration;

use tachyon_tokio::AsyncBus;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket_path = "/tmp/tachyon_tokio_example.sock";
    let _ = std::fs::remove_file(socket_path);

    let server_path = socket_path.to_string();
    let server = tokio::spawn(async move {
        let bus = AsyncBus::listen(server_path, 1 << 16).await?;
        let msg = bus.recv(10_000).await?;
        println!("received type_id={} payload={:?}", msg.type_id, msg.payload);
        Ok::<(), tachyon_tokio::AsyncBusError>(())
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let client = AsyncBus::connect(socket_path).await?;
    client.send(b"hello from tokio", 1)?;

    server.await??;
    let _ = std::fs::remove_file(socket_path);
    Ok(())
}
