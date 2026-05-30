# Phase Plan for tachyon-async

This document outlines the series of planned phases for implementing async Rust support around Tachyon.

## Phase 1: Repository Scaffolding
- Create a Cargo workspace.
- Prepare crates for core code, Tokio bindings, smol bindings, and shared benchmark types.
- Add minimal CI, docs, and examples.

## Phase 2: Upstream Gap Analysis
- Analyze what Tachyon Rust bindings expose.
- Document any missing APIs needed for async wrappers.
- Track external input needs in this repository issues.
- Gap analysis output: [`docs/upstream-gap-analysis.md`](./upstream-gap-analysis.md)

## Phase 3: Tokio MVP *(implemented)*
- Implement a conservative blocking-bridge Tokio adapter in [`crates/tachyon-tokio`](../crates/tachyon-tokio).
- Support async-friendly `connect`/`listen`, `recv().await`, and synchronous `send`.
- Keep the design constrained to confirmed upstream `tachyon-ipc` APIs and known gaps.
- Design notes: [`docs/tokio-mvp.md`](./tokio-mvp.md)

## Phase 4: Low-Overhead Mode *(implemented)*
- Introduced `BusReceiver` in `crates/tachyon-tokio`: a dedicated driver-thread receive stream backed by `tokio::sync::mpsc`.
- Added `AsyncBus::into_receiver(spin_threshold, channel_capacity)` as the recommended low-overhead receive entry-point.
- Added `BusReceiver::try_recv_buffered()` for synchronous burst-draining after an async wakeup.
- Confirmed that true zero-copy is not achievable without upstream `try_acquire_rx` or a readiness FD (see Phase 2 gap analysis).
- Added example `examples/low_overhead_tokio.rs` and design notes in `docs/tokio-low-overhead.md`.

## Phase 5: smol Runtime Comparison *(implemented)*
- Implemented `crates/tachyon-smol` with `AsyncBus`, `OwnedMessage`, `AsyncBusError`, `BusReceiver`, and `TryRecvBufferedError` mirroring the Tokio crate where practical.
- Added `AsyncBus::into_receiver(spin_threshold, channel_capacity)` with a dedicated driver-thread receive path and synchronous burst-draining via `try_recv_buffered()`.
- Added examples `crates/tachyon-smol/examples/basic_smol.rs` and `crates/tachyon-smol/examples/low_overhead_smol.rs`, plus runtime comparison notes in [`docs/smol-comparison.md`](./smol-comparison.md).

## Phase 6: Benchmark Suite *(next)*
- Add a focused benchmark harness comparing Tokio and smol adapters for both per-call bridge mode and low-overhead receiver mode.
- Measure per-message latency, throughput, and tail latency under burst load across both runtimes.
- Provide data to guide further upstream API requests and runtime-specific recommendations.

---
