# Getting started

Add the runtime-independent core, an adapter, and your executor:

```toml
[dependencies]
kape = "0.1.0"
kape-memory = "0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Create named backends in lookup order and use explicit millisecond TTLs:

```rust,ignore
use std::sync::Arc;
use kape::{Cache, CacheLookup};
use kape_memory::MemoryBackend;

#[tokio::main]
async fn main() -> Result<(), kape::KapeError> {
    let cache = Cache::builder()
        .backend("hot", MemoryBackend::<String, String>::new(10_000))
        .backend("shared", MemoryBackend::<String, String>::new(100_000))
        .build()?;

    let key = "user:42".to_owned();
    cache.set(&key, Arc::new("Ada".to_owned()), 60_000).await?;
    assert_eq!(cache.get(&key).await?.as_deref().map(String::as_str), Some("Ada"));

    match cache.lookup(&key).await? {
        CacheLookup::Hit { backend, remaining_ttl, .. } => {
            println!("hit {backend}, {remaining_ttl}ms remaining");
        }
        CacheLookup::Miss => println!("miss"),
    }
    Ok(())
}
```

TTL is always `i64` milliseconds:

- `-1`: never expires;
- `0`: immediately invalidate;
- a positive value: finite lifetime;
- below `-1`: `KapeError::InvalidTtl`, before any mutation.

`get` returns `Option<Arc<V>>`. `lookup` additionally returns the backend name
and exact remaining TTL. Absence is only an `Ok` miss; read and backfill
failures return `Err`.

Backend names must be non-blank and unique, even when multiple instances use
the same adapter type. `backend_names()` returns them in configured order.
