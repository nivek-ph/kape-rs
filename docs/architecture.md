# Architecture

Kape separates typed orchestration from storage-specific behavior.

```text
application
    |
    v
kape::Cache<K, V>
    |
    +-- configured-order reads and elapsed-adjusted backfill
    +-- reverse-order fail-fast mutations
    |
    +-- kape-memory      typed Arc<V>
    +-- kape-redis       RedisCodec<K, V>
    +-- kape-postgres    PostgresCodec<K, V>
```

The core owns `CacheBackend<K, V>`, backend naming,
ordering, validation, and error context. It has no async-runtime, storage, or
serialization dependency.

Every backend implements four scalar methods: `get`, `set`, `remove`, and
`clear`. Default batch methods execute the scalar contract sequentially;
adapters may override them while preserving read positions and TTL semantics
and rejecting duplicate batch-write keys before mutation.

Reads visit backends in configured order. A valid hit has a remaining TTL of
`-1` or a positive millisecond value. A hit carrying `0` or a value below `-1`
is a backend contract violation and becomes a named `get` error. A later hit
is backfilled into every earlier backend, nearest first. Before invoking each
destination write, the core deducts elapsed read and completed-backfill time
from finite TTLs. If no positive TTL remains, later writes are skipped. Time
spent inside the current destination write is storage-specific and is not
included in this core guarantee.

Set, remove, clear, and batch mutations visit backends in reverse order. All
operations are fail-fast. Kape does not roll back earlier effects, so `Err`
does not mean the chain is unchanged.

Tag invalidation, cross-instance synchronization, hooks, metrics, background
writes, and loader coalescing remain outside the `0.1.0` core.
