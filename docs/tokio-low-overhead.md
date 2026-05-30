# Tokio Low-Overhead Receive Path (Phase 4)

## Overview

Phase 4 extends the Phase 3 `AsyncBus` API with a lower-overhead receive path
implemented via a dedicated driver thread and an internal `tokio::sync::mpsc`
channel.  The new entry-point is `AsyncBus::into_receiver`, which returns a
`BusReceiver`.

---

## API added in this phase

### `AsyncBus::into_receiver(spin_threshold, channel_capacity) -> BusReceiver`

Consumes the `AsyncBus`, moves the upstream `tachyon_ipc::Bus` handle into a
dedicated blocking driver thread, and returns a `BusReceiver` that reads from an
internal channel.

```rust
let bus = AsyncBus::listen("/tmp/my.sock", 1 << 16).await?;
let mut receiver = bus.into_receiver(10_000, 64);
```

### `BusReceiver::recv() -> Option<Result<OwnedMessage, AsyncBusError>>`

Async receive.  Suspends the current Tokio task until a message (or termination)
is available in the channel.  This is the "wait for readiness + consume" step in
one call.

### `BusReceiver::try_recv_buffered() -> Result<OwnedMessage, TryRecvBufferedError>`

Synchronous, non-suspending drain.  Returns immediately:

| Result | Meaning |
|--------|---------|
| `Ok(msg)` | A message was already buffered in the channel. |
| `Err(Empty)` | Channel is empty; driver is still running. |
| `Err(Disconnected)` | Driver has stopped; no more messages will arrive. |

---

## Recommended usage pattern

```rust
let mut receiver = bus.into_receiver(10_000, 64);

while let Some(result) = receiver.recv().await {
    let msg = result?;
    // process msg …

    // Drain any burst of messages that accumulated while we were processing.
    loop {
        match receiver.try_recv_buffered() {
            Ok(extra) => { /* process extra */ }
            Err(TryRecvBufferedError::Empty) => break,
            Err(TryRecvBufferedError::Disconnected) => return Ok(()),
        }
    }
}
```

This pattern mirrors the intended `readable().await` + synchronous-consume shape
described in the Phase 4 brief.  The `recv().await` step fulfils the "readiness
wait" role; `try_recv_buffered` provides the immediate-consumption step for
messages that are already in the buffer.

See `crates/tachyon-tokio/examples/low_overhead_tokio.rs` for a runnable demo.

---

## Is this true zero-copy?

**No.**  Each message is still copied from the upstream guard into an owned
`Vec<u8>` before being placed into the channel.

The reason is an upstream constraint confirmed during the Phase 2 gap analysis:
the `tachyon-ipc` 0.5.1 Rust API couples message acquisition and slot release
into a single blocking call (`Bus::acquire_rx`).  There is no separate readiness
probe that returns without consuming a slot, and `RxGuard` lifetimes are tied to
the guard object and must not be held across `.await` points.

It is therefore impossible to expose a zero-copy guard to async code without
either unsound lifetime extension or blocking the Tokio executor thread.

---

## What upstream APIs make this possible?

Only the APIs already confirmed in Phase 2 are used:

| API | Role |
|-----|------|
| `Bus::listen` / `Bus::connect` | Lifecycle |
| `Bus::acquire_rx(spin_threshold)` | Blocking receive in driver thread |
| `RxGuard::data()` | Copy payload bytes |
| `RxGuard::commit()` | Release upstream slot |
| `Bus::send` | Synchronous send path |

No new upstream primitives are required.

---

## Why is this lower-overhead than Phase 3?

| | Phase 3 (`AsyncBus::recv`) | Phase 4 (`BusReceiver`) |
|---|---|---|
| Blocking task cost | One `spawn_blocking` per message | One `spawn_blocking` total (driver) |
| Mutex contention | Shared send/recv mutex per call | No mutex; driver owns `Bus` |
| Burst efficiency | Linear task-spawn cost | Driver buffers bursts into channel |
| API model | One `async fn` call per message | Background driver + channel drain |

The improvement is real but **not benchmarked yet** — see the benchmark phase
(Phase 5 / 6) for quantified data.

---

## Limitations

1. **Still copies on every message.** Zero-copy is not achievable without an
   upstream nonblocking or readiness-decoupled receive API.
2. **`into_receiver` panics if `AsyncBus` has been cloned.**  The driver needs
   exclusive ownership of the `Bus` handle; all other clones must be dropped
   before calling `into_receiver`.
3. **`send` path is separate.**  After calling `into_receiver`, the original
   `AsyncBus` is consumed.  To send on the same socket, keep a separate
   `AsyncBus` connected from the other side, or clone before calling
   `into_receiver` (then ensure the clone is dropped before the call).
4. **`channel_capacity` should be tuned to the expected burst size.**  Too small
   a capacity will cause the driver to block on the channel, back-pressuring
   upstream slot release.
5. **Runtime specificity.** This document is Tokio-specific; smol now has a matching Phase 5 adapter with small runtime-driven differences documented in `docs/smol-comparison.md`.

---

## What should the benchmark phase measure?

- Per-message latency: `spawn_blocking`-per-message (Phase 3) vs driver-channel (Phase 4).
- Throughput at varying message sizes (64 B, 1 KiB, 64 KiB).
- Latency distribution tail (p99, p999) under bursty load.
- Overhead of the copy step relative to a synchronous baseline.
- Sensitivity of driver channel capacity to burst handling.

---

## What would enable future improvements?

From the Phase 2 gap analysis, the following upstream additions would allow
genuine zero-copy or readiness-separated async receive in a future phase:

1. **`Bus::try_acquire_rx`** — a nonblocking receive attempt returning
   `Err(BufferEmpty)` immediately when no message is available.  This would
   allow a user-space busy-poll loop or an `AsyncRead`-style `poll_recv`
   implementation without a blocking thread.
2. **Readiness FD or event handle** — an OS-level pollable handle for "slot
   available" that could integrate with Tokio's reactor, enabling a true
   `readable().await` that does not consume a message.
3. **Documented lifetime/safety guarantees for `RxGuard` across threads** —
   required before any borrow-based zero-copy can be exposed without `unsafe`.
