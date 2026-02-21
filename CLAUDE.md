# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Project Overview

`pmbot-rust` is a **high-performance, production-grade bot** written in Rust. It is designed with strict correctness, zero-cost abstractions, and minimal latency at the forefront. All design decisions must be grounded in systems-engineering first principles.

---

## Architecture Principles

- **Performance is a feature.** Prefer stack allocation, zero-copy patterns, and allocation-free hot paths wherever possible.
- **Correctness before optimization.** Use Rust's type system and ownership model to eliminate entire classes of bugs at compile time.
- **Minimal dependencies.** Every crate added to `Cargo.toml` must be justified. Prefer the standard library and well-audited foundational crates (`tokio`, `serde`, `tracing`, etc.).
- **Async-first concurrency.** Use `tokio` as the async runtime. Avoid blocking the executor thread — offload CPU-bound work to `tokio::task::spawn_blocking`.
- **Layered architecture.** Separate concerns into clear modules: transport, protocol, domain logic, and infrastructure (persistence, config, telemetry).
- **Explicit over implicit.** Avoid implicit global state. Pass dependencies through constructors or function parameters. Prefer `Arc<T>` over `lazy_static!` or `once_cell::Lazy` for shared runtime state.

---

## Code Style and Standards

### Rust Edition and Toolchain

- Use the **latest stable Rust toolchain** (`rustup default stable`).
- Target `edition = "2021"` in `Cargo.toml`.
- Enable `resolver = "2"` for workspace and feature resolution.

### Formatting and Linting

```bash
# Format all code
cargo fmt --all

# Lint with pedantic and restriction lints enabled
cargo clippy --all-targets --all-features -- -D warnings
```

All code must be free of `clippy` warnings before merging. The CI pipeline enforces this.

### Naming Conventions

- Types, traits, enums: `UpperCamelCase`
- Functions, methods, variables, modules: `snake_case`
- Constants and statics: `SCREAMING_SNAKE_CASE`
- Lifetimes: short lowercase (`'a`, `'buf`, `'req`)
- Avoid abbreviations unless they are universally understood (e.g., `msg`, `buf`, `id`)

### Error Handling

- Use `thiserror` for library/domain errors that need rich types.
- Use `anyhow` for application-level error propagation where context matters more than type.
- **Never use `.unwrap()` or `.expect()` in production code paths.** Reserve them for tests and `main`-level initialisation where a panic is acceptable.
- Propagate errors with `?`. Add context with `.context("…")` (via `anyhow`) when the call site adds meaningful information.

```rust
// Preferred
let config = load_config(path).context("failed to load bot configuration")?;

// Never in production paths
let config = load_config(path).unwrap();
```

### Unsafe Code

- `unsafe` blocks are **prohibited** unless:
  1. There is no safe alternative.
  2. The block is accompanied by a `// SAFETY:` comment explaining the invariant being upheld.
  3. The change is reviewed by a second engineer.

---

## Performance Guidelines

### Memory

- Prefer `Vec<T>` with a known `capacity` hint over repeated pushes.
- Use `bytes::Bytes` for shared, immutable network buffers.
- Profile with `heaptrack` or `dhat` before optimizing allocator behaviour.
- Do **not** reach for custom allocators (e.g., `jemalloc`, `mimalloc`) without first profiling to confirm allocator contention.

### Async & I/O

- All network I/O must go through `tokio::net` or an async abstraction built on it.
- Use `tokio::sync::mpsc` for intra-task messaging; prefer bounded channels to apply backpressure.
- Use `tokio::time::timeout` to enforce deadlines on all external calls.
- Avoid `tokio::sync::Mutex` in hot paths — prefer `tokio::sync::RwLock` for read-heavy workloads or a lock-free structure where appropriate.

### Benchmarking

```bash
# Run micro-benchmarks (criterion)
cargo bench

# Profile with flamegraph (requires cargo-flamegraph)
cargo flamegraph --bin pmbot
```

All performance-sensitive changes must include a benchmark or reference an existing one demonstrating no regression.

---

## Testing

```bash
# Run all unit and integration tests
cargo test --all

# Run tests with output visible (useful for debugging)
cargo test -- --nocapture

# Run a single test by name
cargo test <test_name>
```

### Test Philosophy

- **Unit tests** live in the same file as the code under test, inside a `#[cfg(test)] mod tests { … }` block.
- **Integration tests** live in `tests/` and test public API boundaries.
- Use `tokio::test` for async test functions.
- Mock external I/O with trait objects or `mockall`; do not make real network calls in unit tests.
- Target >= 80% line coverage on domain logic. Use `cargo-llvm-cov` to measure:

```bash
cargo llvm-cov --all-features --workspace --lcov --output-path lcov.info
```

---

## Build & CI

```bash
# Development build
cargo build

# Optimized release build
cargo build --release

# Check without producing artifacts (fastest feedback)
cargo check --all-targets --all-features
```

CI runs the following gates in order — all must pass:

1. `cargo fmt --all -- --check`
2. `cargo clippy --all-targets --all-features -- -D warnings`
3. `cargo test --all`
4. `cargo audit` (via `cargo-audit`) — no high/critical advisories

---

## Dependency Management

- Pin exact versions for security-critical crates in `Cargo.lock`; commit `Cargo.lock` to the repository.
- Run `cargo audit` regularly and resolve advisories promptly.
- Before adding a new dependency, ask:
  1. Does the standard library already provide this?
  2. Is this crate actively maintained and widely used in the ecosystem?
  3. What is its transitive dependency footprint?

---

## Logging and Observability

- Use `tracing` for structured, async-aware logging. Do **not** use `log` or `println!` in library code.
- Instrument all significant operations with `tracing::instrument`:

```rust
#[tracing::instrument(skip(self), err)]
async fn handle_message(&self, msg: IncomingMessage) -> Result<()> {
    // …
}
```

- Export traces to an OTLP-compatible collector (Jaeger, Honeycomb, etc.) via `opentelemetry-otlp`.
- Expose Prometheus metrics at `/metrics` using `metrics` + `metrics-exporter-prometheus`.

---

## Module Structure (Recommended)

```
src/
├── main.rs          # Entrypoint: runtime setup, config loading, graceful shutdown
├── config.rs        # Typed configuration (serde + envy / config crate)
├── bot/
│   ├── mod.rs       # Bot core — message dispatch loop
│   ├── handler.rs   # Command/event handlers
│   └── state.rs     # Shared bot state (Arc<BotState>)
├── transport/
│   ├── mod.rs       # Transport abstraction trait
│   └── websocket.rs # WebSocket implementation (tokio-tungstenite)
├── protocol/
│   ├── mod.rs       # Protocol types and (de)serialisation
│   └── codec.rs     # Tokio Codec for framing
├── domain/          # Pure business logic, no I/O
│   └── mod.rs
└── telemetry.rs     # Tracing / metrics initialisation
```

---

## Git Workflow

- Branch from `main` using the pattern `feat/<short-description>` or `fix/<short-description>`.
- Commits follow [Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `perf:`, `refactor:`, `test:`, `chore:`.
- Every PR must include a description explaining *what* changed, *why*, and any *performance impact*.
- Squash-merge into `main`; keep a linear history.

---

## Security

- Treat all external input as untrusted. Validate and sanitise at the transport boundary before passing data into the domain layer.
- Secrets (tokens, API keys) must **never** be committed to the repository. Load from environment variables or a secrets manager.
- Run `cargo audit` as part of every CI run.
- Regularly update dependencies: `cargo update` + review `Cargo.lock` diff.

---

## Glossary

| Term | Meaning |
|------|---------|
| Hot path | Code executed on every message — must be allocation-free and O(1) or O(log n) |
| Backpressure | Mechanism to slow producers when consumers fall behind |
| OTLP | OpenTelemetry Protocol |
| Codec | Tokio framing abstraction for encoding/decoding byte streams |
