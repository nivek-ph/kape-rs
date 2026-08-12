# Kape

Kape is a type-safe orchestration layer for composing multiple named cache
backends in Rust. A cache can contain any number of backends, in any order,
including multiple instances of the same adapter.

```text
request -> memory -> Redis -> PostgreSQL
              ^         |
              +---------+ backfill with remaining TTL
```

Kape preserves the configured order. It does not sort backends, deduplicate
adapter types, or force a fixed primary/secondary model. A later fresh hit can
refill eligible earlier backends, but the refill never restarts the source
entry's full TTL.

The core API retains the caller's `K` and `V` types. Concrete backend and error
types are erased only inside the heterogeneous chain. Serialization remains an
adapter concern, so the in-memory adapter can store `Arc<V>` directly while
Redis and PostgreSQL use explicit codecs.

## Where to start

- [Getting started](getting-started.md) builds a small ordered in-memory cache.
- [Architecture](architecture.md) explains the core/adapter boundary and read
  flow.
- [Backends](backends.md) compares memory, Redis, and PostgreSQL behavior.
- [TTL and failure policies](policies.md) covers expiry, stale values, and
  failures.
- [Operations](operations.md) documents scalar, batch, loader, and management
  behavior.
- [Examples](examples.md) lists runnable programs and service requirements.

Kape is pre-release. The current API and storage formats may change before
`1.0`.
