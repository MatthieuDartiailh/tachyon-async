# tachyon-async

This repository serves as an experimental workspace for developing async Rust wrappers around Tachyon, focusing on both Tokio and smol runtime support. It also provides benchmarks to measure the impact of async runtimes on Tachyon’s performance.

## Purpose
This project is external to the upstream [Tachyon repo](https://github.com/riyaneel/Tachyon). It prototypes async integration and produces data and designs that could influence upstream or remain as runtime extensions.

## Layout
- **crates/**: Rust crates separated by concern (core abstractions, runtime-specific bindings).
- **docs/**: Documentation on implementation and design.
- **benches/**: Benchmarks for measuring async overhead and strategies.
- **scripts/**: Helper scripts for running benchmarks consistently.

## Project status
- Phase 1 (repository scaffold): **complete**.
- Phase 2 (upstream Rust gap analysis): **complete**.
- Phase 3 (Tokio MVP adapter): **implemented**.
- Phase 4 (low-overhead refinements): **next**.

## Phase outputs
- Upstream capability/gap analysis: [`docs/upstream-gap-analysis.md`](docs/upstream-gap-analysis.md)
- Tokio MVP architecture and limitations: [`docs/tokio-mvp.md`](docs/tokio-mvp.md)
- External input questions and tracking: [`docs/upstream-inputs.md`](docs/upstream-inputs.md)
- Overall phased plan: [`docs/implementation-plan.md`](docs/implementation-plan.md)
