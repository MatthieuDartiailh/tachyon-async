# Upstream Rust Binding Gap Analysis (Phase 2)

Date: 2026-05-30
Source inspected: published `tachyon-ipc` crate `0.5.1` (`docs.rs`/crates.io source)

## Scope and evidence level

This analysis is based on direct inspection of the current Rust crate source (`bus.rs`, `rpc.rs`, `lib.rs`, `error.rs`) in `tachyon-ipc` 0.5.1.

- **Confirmed**: statements backed by concrete Rust API or comments in the inspected source.
- **Inferred**: implications for async integration not explicitly guaranteed by upstream docs.

## Confirmed upstream Rust API surface

### `Bus`

Confirmed public APIs include:

- lifecycle: `Bus::listen`, `Bus::connect`
- configuration: `set_numa_node`, `set_polling_mode`
- send path: `send`, `acquire_tx`, `TxGuard::write`, `TxGuard::as_mut_slice`, `TxGuard::commit`, `TxGuard::commit_unflushed`, `flush`
- receive path: `acquire_rx(spin_threshold)`, `drain_batch(max_msgs, spin_threshold)`, `RxGuard::data`, `RxGuard::commit`, batch iterators/views

Notes:

- `Bus` is `Send` but **not** `Sync` (explicitly documented in code comment).
- receive APIs exposed in Rust are blocking (`acquire_rx`, `drain_batch`).

### `RpcBus`

Confirmed public APIs include:

- lifecycle: `RpcBus::listen`, `RpcBus::connect`
- call/request path: `acquire_call`, `call`, `wait(correlation_id, spin_threshold)`
- serve/reply path: `serve(spin_threshold)`, `acquire_reply`, `reply`
- tuning: `set_polling_mode`
- zero-copy guards: `RpcTxGuard::{write,as_mut_slice,commit}`, `RpcRxGuard::{data,commit}`

Notes:

- Rust RPC support **does exist** in upstream bindings today.
- `RpcBus` is `Send + Sync` in the current Rust implementation.

## Answers to Phase 2 questions

### 1) What is already available for `Bus` / `RpcBus`?

**Confirmed:** both are exposed and usable in Rust. `Bus` supports SPSC message transport with copy and zero-copy guard APIs. `RpcBus` supports request/reply with correlation IDs and both copy/zero-copy paths.

### 2) Do nonblocking receive/send helpers exist?

**Confirmed:**

- No Rust-level `try_recv`, `try_wait`, `try_serve`, or explicit readiness probe API is exposed.
- `Bus::acquire_tx`/`RpcBus::acquire_call` can fail fast with `BufferFull`, which provides a nonblocking-ish send reservation attempt.

**Gap:** receive-side nonblocking helpers are not present as first-class Rust API.

### 3) Do zero-copy Rust APIs exist, and constraints?

**Confirmed:** zero-copy APIs exist via guard types (`TxGuard`, `RxGuard`, `RxBatchGuard`, `RpcTxGuard`, `RpcRxGuard`) and slice accessors.

**Confirmed constraints from API shape/comments:**

- borrowed slices are guard-lifetime-bound (`data()` lifetime tied to guard);
- slots must be released via `commit()` (or drop behavior);
- dropping TX guards without commit triggers rollback;
- dropping RX guards auto-commits/release.

**Inferred async implication:** these guards should generally be consumed synchronously in a short section and should not be held across `.await` points.

### 4) Can readiness be waited on separately from consuming a message?

**Confirmed:** no explicit Rust API provides readiness waiting decoupled from message acquisition/consumption. Current receive/call wait functions both wait and return a consumable guard.

### 5) Does RPC support exist in Rust bindings?

**Confirmed:** yes (`RpcBus`, call/wait/serve/reply, zero-copy guard types).

### 6) Which async integration pieces can be built externally now?

The following can be implemented externally without upstream changes:

- runtime adapters that move blocking receive/wait/serve calls off executor worker threads (driver thread model);
- async wrappers over existing connect/listen/send/call/reply operations;
- owned-message conversion layers (copy out of guards before `.await`);
- benchmark harnesses comparing sync baseline vs adapter strategies.

### 7) Which minimal upstream additions would be needed/helpful?

Minimal additions likely to reduce adapter overhead/complexity:

1. **Nonblocking receive primitives in Rust**
   - e.g. `Bus::try_acquire_rx`, `RpcBus::try_wait`, `RpcBus::try_serve` returning `BufferEmpty` when no work.
2. **Readiness primitive decoupled from consume**
   - e.g. wait-until-readable API or pollable event handle/FD exposure.
3. **Clarify/guarantee async-relevant semantics in Rust docs**
   - especially `spin_threshold` behavior and guard usage expectations for integration with async runtimes.

## Confirmed gaps summary

- Missing explicit nonblocking receive helpers in Rust.
- Missing separate readiness-wait primitive.
- Async integration ergonomics must currently rely on external blocking bridges.

## Inferred risks (tracked for Phase 3)

- Driver-thread orchestration may add wakeup overhead vs a future native readiness primitive.
- Zero-copy in async flows is safest when consumption remains synchronous and short-lived.

## Phase 3 gating from this analysis

Phase 3 should proceed with a conservative external adapter MVP:

- implement driver-thread readiness bridge using existing blocking APIs;
- expose async APIs around owned-message delivery first;
- keep zero-copy surface explicit and narrowly scoped;
- avoid assuming missing upstream primitives until they are added.
