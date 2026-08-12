# Architecture

Kape separates typed orchestration from storage-specific behavior.

```text
application
    |
    v
kape::Cache<K, V>
    |
    +-- named backend policy
    +-- ordered lookup and backfill
    +-- partial-failure reporting
    |
    +-- kape-memory      Arc<V>, no serialization
    +-- kape-redis       RedisCodec<K, V>
    +-- kape-postgres    PostgresCodec<K, V>
```

## Core boundary

The `kape` crate owns:

- the typed `CacheBackend<K, V>` contract;
- exact backend ordering and unique instance names;
- lookup, write, backfill, invalidation, and management orchestration;
- TTL resolution and failure policies;
- optional tracing events.

The core has no async-runtime, Moka, Redis, PostgreSQL, or serialization
dependency. The public interface keeps `K` and `V` typed, erases only the
concrete backend type in the internal chain, and uses `KapeError` throughout.

## Read flow

Reads visit enabled backends sequentially in configured order.

1. A miss continues to the next backend.
2. A fresh hit stops the read.
3. Eligible earlier backends are refilled using the source entry's remaining
   TTL and destination caps.
4. A stale result is retained as a candidate while later backends are checked
   for a fresh value.
5. A backend error follows that backend's explicit read-failure policy.

Kape never infers misses from the contents of `V`. Empty strings, zero values,
`false`, and empty collections remain ordinary cached values.

## Write and invalidation flow

Writes visit write-enabled backends in configured order. Removal, clear, and
disconnect visit participating backends in reverse order. Cross-backend
operations are deterministic but not transactional; errors identify both the
operation and backend and can aggregate partial failures.

## Extension boundary

Tag invalidation, cross-instance synchronization, hooks, built-in metrics, and
background writes are intentionally outside the core MVP. They introduce
metadata, delivery, ordering, or lifecycle requirements that should remain
separate capabilities rather than expanding the cache backend contract.
