# Tokio MVP design (Phase 3)

## Goal

Provide a conservative, usable Tokio adapter around upstream `tachyon-ipc` while staying aligned with the Phase 2 confirmed Rust API surface and gaps.

## Implemented architecture

Phase 3 implements `AsyncBus` in `crates/tachyon-tokio` as a blocking bridge:

- `connect`/`listen` are async-friendly by calling upstream blocking constructors in `tokio::task::spawn_blocking`.
- `recv().await` also uses `spawn_blocking` and calls upstream `Bus::acquire_rx(spin_threshold)`.
- Received data is copied into an owned message (`OwnedMessage { type_id, payload: Vec<u8> }`) before returning to async code.
- `send` stays synchronous and calls upstream `Bus::send` directly.

This avoids holding borrowed upstream guards across `.await` while still offering a practical Tokio receive API.

## Upstream APIs relied on

The MVP uses only confirmed upstream APIs from Phase 2:

- `Bus::connect`, `Bus::listen`
- `Bus::acquire_rx(spin_threshold)` + `RxGuard::{data,commit}`
- `Bus::send`
- `TachyonError`

## Why this compromise was chosen

Phase 2 confirmed that the Rust binding currently has:

- no dedicated readiness primitive decoupled from receive,
- no first-class nonblocking receive API.

Because of that, this MVP intentionally uses a blocking bridge (`spawn_blocking`) instead of attempting speculative readiness integration.

## Current limitations

- `recv` allocates/copies into an owned buffer for async safety.
- `send` is synchronous.
- Shared access currently serializes through a mutex around one upstream `Bus` handle; this keeps the MVP simple but may add contention in mixed send/recv usage.
- No smol adapter work is included in this phase.
- No benchmark suite is added in this phase.
- Building `tachyon-ipc` currently depends on upstream `tachyon-sys` C++ toolchain compatibility; in this sandbox, dependency builds fail before Rust tests run.

## Phase 4 targets

Phase 4 should improve overhead and concurrency by introducing a lower-overhead receive path (likely a dedicated driver/readiness architecture) and by tightening send/recv contention behavior once upstream primitives or clearer guarantees are available.
