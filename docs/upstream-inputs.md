# External Inputs for Async Rust Support

This document outlines questions or decisions requiring external input or upstream engagement.

### Questions for Upstream Tachyon
1. Does the upstream Rust binding expose nonblocking primitives?
2. Can upstream support readiness without consuming a message?
3. Are zero-copy primitives safe for async-friendly integration?
4. How is RPC designed in Rust bindings?

### Benchmark-Specific Questions
1. What is the hardware/kernel recommended for comparing async runtimes?
2. Are there guidelines for interpreting tail percentile effects?
