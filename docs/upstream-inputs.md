# External Inputs for Async Rust Support

This document tracks the concrete external input needed after the Phase 2 upstream Rust binding gap analysis.

## Confirmed needs from Phase 2

Analysis reference: [`docs/upstream-gap-analysis.md`](./upstream-gap-analysis.md)

### 1) Rust nonblocking receive/wait APIs

Current Rust bindings expose blocking receive/wait/serve APIs, but no explicit `try_*` receive-side primitives.

**Question for upstream maintainers:**

- Are there C-core primitives that can be safely exposed as Rust `try_acquire_rx` / `try_wait` / `try_serve` without changing wire semantics?
- If not, what minimal C additions would be acceptable?

### 2) Readiness signal decoupled from message consumption

Current Rust API waits and consumes in one operation (`acquire_rx`, `wait`, `serve`).

**Question for upstream maintainers:**

- Is there an upstream-supported way to wait for readiness separately (or expose a pollable/eventfd-style handle) for runtime integration?
- If not, would a minimal readiness API be acceptable upstream?

### 3) Async usage guidance for zero-copy guards

Zero-copy guards exist and are lifetime-bound, but async best-practice guidance is implicit.

**Question for upstream maintainers:**

- Can upstream document recommended async usage boundaries (for example, consume/commit without crossing `.await`)?
- Are there known constraints around holding guards longer than a short critical section?

### 4) Benchmark methodology alignment

Phase 3+ benchmarking should mirror upstream assumptions where possible.

**Question for upstream maintainers/community:**

- Which CPU pinning, polling mode, and kernel/runtime settings are considered representative for Tachyon comparisons?
- Which percentile metrics and warmup strategy should be treated as baseline methodology?

## Tracking issues in this repository

The Phase 2 intent is to track these items via repository issues in `MatthieuDartiailh/tachyon-async`.

Planned issues:

1. "Expose/clarify nonblocking Rust receive primitives for async adapters"
2. "Need readiness primitive (or pollable handle) in Rust bindings"
3. "Document zero-copy guard async usage expectations"
4. "Define benchmark environment assumptions for async comparisons"
5. "Clarify/document required C++ toolchain baseline for tachyon-sys consumers"

> Note: issue creation from this execution environment was not possible due unavailable GitHub issue-write capability; the above titles are ready to open directly.
