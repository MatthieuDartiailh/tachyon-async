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

## Phase 4: Low-Overhead Mode *(next)*
- Reduce bridging overhead and mutex contention with a lower-overhead architecture.
- Expose carefully scoped low-overhead/zero-copy-oriented receive APIs where safe.
- Align design with any new upstream readiness or nonblocking primitives.

## Phase 5: smol Runtime Comparison
- Mirror Tokio APIs for smol.
- Add comparative results.

---
