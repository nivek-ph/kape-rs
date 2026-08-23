# kape-memory

Process-local in-memory adapter for Kape.

```toml
[dependencies]
kape = "0.1.0"
kape-memory = "0.1.0"
```

```rust
use kape_memory::MemoryBackend;

let backend = MemoryBackend::<String, String>::new(10_000);
```

The capacity is a maximum entry count, not a freshness guarantee: capacity
eviction may remove a value before its TTL expires. TTL is only an upper bound
on freshness.

Values remain typed and are stored as `Arc<V>` without serialization. Finite
expiry uses a private monotonic clock. An expired entry observed by a read is
invalidated and returned as a miss. Moka is a private implementation detail.

Clones of one backend share storage. Separately constructed backend instances
are isolated, and `clear` affects only the instance on which it is called.
