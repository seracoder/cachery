# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] — 2026-11-15

Initial release: a fast, type-safe, sync+async caching library for Python.

### Added

- **`@cache` decorator** for sync and async callables — one decorator, identical
  behaviour, signatures preserved via `ParamSpec` (`inspect.signature` equality is tested).
- **Static key-template validation** at decoration time: unknown placeholders raise
  `InvalidKeyTemplateError` with a "did you mean …" suggestion, so typos fail in CI,
  not in production. `Annotated[...]`-based `KeyIgnore` / `KeyPart` key control.
- **Backends** — in-memory (`mem://`, LRU/LFU/FIFO/MRU/RR policies, incremental reaper),
  disk (`disk://`, stdlib `sqlite3` with WAL), Redis (`redis://`, RESP3, pipelined batch
  ops), tiered (`tiered://mem+redis://…`, read/write-through with bounded L1 staleness),
  and a duck-typed `Backend` Protocol for custom stores — no inheritance required. All
  backends pass one shared contract test suite.
- **Strategy toolbox** — `cache.early`, `cache.soft`, `cache.hit`, `cache.failover`,
  `cache.locked`, `cache.rate_limit`, `cache.circuit_breaker`, `cache.iterator`, plus
  tag-based invalidation and `cache.multi_cached`.
- **Serialization** — fast-path primitives skip the codec entirely; typed msgspec
  msgpack (extra `speed`); lossless-checked untyped msgpack; pickle fallback; custom
  type registry; optional zlib/zstandard compression (extra `compress`).
- **Fail-soft by default** — a down cache degrades to "run the function"; `suppress=False`
  opts into strictness. Configuration errors still fail fast at decoration time.
- **Observability** — `cache.detect` context manager, typed middleware chain, Prometheus
  (extra `prometheus`) and OpenTelemetry (extra `otel`) middleware.
- **CLI** — `python -m cachery ping|keys|stats` for production debugging (stdlib only).
- **Testing support** — deterministic `FakeBackend`, pytest fixtures, and a shared
  backend contract suite for custom backends.
- **Sync fast path** — sync callers of simple-strategy functions on sync-capable
  backends run the whole cache flow inline, skipping the event-loop bridge.
- **CI** — ruff + mypy strict + pyright strict, test matrix (Python 3.10–3.13 ×
  Linux/Windows), real-Redis integration job, weekly dependency-freshness gate,
  GitHub Pages docs, and PyPI trusted publishing.

[Unreleased]: https://github.com/seracoder/cachery/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/seracoder/cachery/releases/tag/v0.1.0
