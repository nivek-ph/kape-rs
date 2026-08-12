# kape-memory

Local in-memory backend adapter for `Kape`.

The adapter stores `Arc<V>` directly without serialization. It retains `Kape`
expiry metadata beside the value so it can report remaining TTL and optionally
expose expired entries as stale candidates. Its current storage engine is Moka,
but that implementation detail is not exposed by the public API.

```rust
use kape_memory::MemoryBackend;

let backend = MemoryBackend::<String, String>::new(10_000);
```

By default stale entries are retained until `Moka` evicts them by capacity. Call
`retain_stale(false)` to invalidate expired entries on read and return a miss.

The backend supports iteration and clear. Pages use an opaque offset cursor
over an arbitrary, weakly-consistent iteration order, so concurrent writes or
eviction can change later pages. `disconnect` is an idempotent no-op because
memory resources are owned by its handles.
