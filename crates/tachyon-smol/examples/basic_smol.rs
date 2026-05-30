use std::time::Duration;

use tachyon_smol::AsyncBus;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    smol::block_on(async {
        let socket_path = "/tmp/tachyon_smol_example.sock";
        let _ = std::fs::remove_file(socket_path);

        let server_path = socket_path.to_string();
        let server = smol::spawn(async move {
            let bus = AsyncBus::listen(server_path, 1 << 16).await?;
            let msg = bus.recv(10_000).await?;
            println!("received type_id={} payload={:?}", msg.type_id, msg.payload);
            Ok::<(), tachyon_smol::AsyncBusError>(())
        });

        smol::Timer::after(Duration::from_millis(20)).await;

        let client = AsyncBus::connect(socket_path).await?;
        client.send(b"hello from smol", 1)?;

        server.await?;
        let _ = std::fs::remove_file(socket_path);
        Ok(())
    })
}
