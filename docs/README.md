# Kape guide

Kape is a type-safe orchestration layer for ordered, named cache backends.

```text
read:      memory -> Redis -> PostgreSQL
backfill:  memory <- Redis <- hit
mutation:  memory <- Redis <- PostgreSQL
```

A read miss continues, a hit stops, and a failure returns immediately. A later
hit backfills earlier backends with the exact remaining TTL. Mutations visit
the chain in reverse configured order and also stop on the first failure.

Serialization remains adapter-local: memory stores `Arc<V>`, while Redis and
PostgreSQL expose separate codec traits. Backend errors are never converted
into misses.

## Guide

- [Getting started](getting-started.md)
- [Architecture](architecture.md)
- [Backends](backends.md)
- [Operations](operations.md)
- [Examples](examples.md)
- [Development and releases](development.md)

Kape `0.1.0` requires Rust 1.96. It has no stale fallback, configurable failure
policies, same-key loader coalescing, or cross-backend transactions.
