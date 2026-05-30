# tachyon-async

This repository serves as an experimental workspace for developing async Rust wrappers around Tachyon, focusing on both Tokio and smol runtime support. It also provides benchmarks to measure the impact of async runtimes on Tachyon’s performance.

## Purpose
This project is external to the upstream [Tachyon repo](https://github.com/riyaneel/Tachyon). It prototypes async integration and produces data and designs that could influence upstream or remain as runtime extensions.

## Layout
- **crates/**: Rust crates separated by concern (core abstractions, runtime-specific bindings).
- **docs/**: Documentation on implementation and design.
- **benches/**: Benchmarks for measuring async overhead and strategies.
- **scripts/**: Helper scripts for running benchmarks consistently.

## Phases
1. Repository scaffold and planning (current phase).
2. Upstream gap analysis to identify missing primitives or hooks.
3. Prototyping async `Tokio` interfaces.
4. Adding smol equivalence and benchmarks.
5. Producing results for upstream discussions.