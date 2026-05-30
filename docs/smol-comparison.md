# Smol Runtime Comparison (Phase 5)

## What was implemented for smol

Phase 5 adds a conservative smol adapter in `crates/tachyon-smol` that mirrors the Tokio integration from Phases 3 and 4 as closely as practical:

- `AsyncBus` wrapper over `tachyon_ipc::Bus`
  - `AsyncBus::connect(socket_path).await`
  - `AsyncBus::listen(socket_path, capacity).await`
  - `AsyncBus::recv(spin_threshold).await`
  - `AsyncBus::send(data, type_id)` (synchronous send path)
- Owned message boundary identical in shape to Tokio:
  - `OwnedMessage { type_id, payload: Vec<u8> }`
- Low-overhead receive path analogous to Tokio Phase 4:
  - `AsyncBus::into_receiver(spin_threshold, channel_capacity)`
  - `BusReceiver::recv().await`
  - `BusReceiver::try_recv_buffered()`
  - `TryRecvBufferedError::{Empty, Disconnected}`

Minimal usage examples are provided at `crates/tachyon-smol/examples/basic_smol.rs` and `crates/tachyon-smol/examples/low_overhead_smol.rs` (recommended low-overhead receive pattern).

## API and architecture parity with Tokio

The smol crate intentionally follows the Tokio crate's public naming and semantics:

- same top-level type names (`AsyncBus`, `OwnedMessage`, `BusReceiver`),
- same core receive APIs (`recv`, `into_receiver`, `try_recv_buffered`),
- same conservative rule: convert borrowed upstream receive guards into owned payloads before returning async values.

Like Tokio, the low-overhead path is built with a dedicated OS driver thread that continuously calls blocking upstream `acquire_rx(spin_threshold)` and forwards owned messages into a runtime channel.

## Runtime-specific differences

The key differences are implementation-level, not conceptual:

- Tokio uses `tokio::task::spawn_blocking` for per-call bridge mode; smol uses `smol::unblock`.
- Tokio low-overhead forwarding channel is `tokio::sync::mpsc`; smol uses `smol::channel`.
- Tokio's `AsyncBusError` includes a `Join` variant for blocking-task join failures; smol has no direct analogue and therefore keeps `AsyncBusError` to `Tachyon` + `LockPoisoned`.

These differences reflect runtime primitives rather than API intent.

## Remaining upstream and architectural limitations

The same upstream constraints from Phase 2 still apply to both runtimes:

- receive-side API is blocking (`acquire_rx`) with no Rust nonblocking `try_acquire_rx`,
- no readiness primitive decoupled from receive acquisition,
- no safe async zero-copy path that can hold borrowed receive data across `.await`.

Therefore both adapters keep the conservative owned-message boundary and avoid APIs that would encourage holding borrowed receive guards across suspension points.

## What benchmark phase should compare next

Phase 6 should compare Tokio and smol on the same adapter shapes:

1. **Per-call bridge mode** (`AsyncBus::recv`) overhead and latency.
2. **Low-overhead receiver mode** (`into_receiver` + `try_recv_buffered`) throughput and tail latency.
3. Sensitivity to `spin_threshold` and channel capacity tuning under burst traffic.
4. Runtime overhead deltas under equivalent message sizes/rates.
