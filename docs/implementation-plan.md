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

## Phase 3: Tokio MVP
- Implement a driver-thread readiness model.
- Support `connect`, `listen`, `recv`, and basic zero-copy patterns.
- Constrain design to the confirmed upstream surface from Phase 2.

## Phase 4: Low-Overhead Mode
- Expose zero-copy APIs focused on low-latency.
- Optimize CI benchmarks.

## Phase 5: smol Runtime Comparison
- Mirror Tokio APIs for smol.
- Add comparative results.

---
