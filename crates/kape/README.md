# kape

Runtime-independent cache-chain orchestration for Rust.

```toml
[dependencies]
kape = "0.1.0"
```

Backends are named and configured in read order. Reads stop at the first hit
and backfill earlier backends with its exact remaining TTL. Mutations run in
reverse configured order. All failures are fail-fast.

```rust
use std::sync::Arc;
use kape::{Cache, CacheBackend, CacheLookup, SetItem};

async fn example<B>(hot: B, shared: B) -> Result<(), kape::KapeError>
where
    B: CacheBackend<String, String> + 'static,
{
    let cache = Cache::builder()
        .backend("hot", hot)
        .backend("shared", shared)
        .build()?;

    let key = "user:42".to_owned();
    cache.set(&key, Arc::new("Ada".to_owned()), 60_000).await?;
    match cache.lookup(&key).await? {
        CacheLookup::Hit { value, backend, remaining_ttl } => {
            assert_eq!(value.as_str(), "Ada");
            assert!(!backend.is_empty());
            assert!(remaining_ttl == -1 || remaining_ttl > 0);
        }
        CacheLookup::Miss => {}
    }

    cache.set_many(&[
        SetItem::new("a".to_owned(), "one".to_owned(), -1),
        SetItem::new("b".to_owned(), "two".to_owned(), 30_000),
    ]).await?;
    let values = cache.get_many(&["a".to_owned(), "a".to_owned()]).await?;
    assert_eq!(values.len(), 2);

    let loaded = cache.get_or_load(
        &"loaded".to_owned(),
        || async { Ok::<_, std::io::Error>("value".to_owned()) },
        60_000,
    ).await?;
    assert_eq!(loaded.as_str(), "value");

    let wrapped = cache.wrap(
        &"profile".to_owned(),
        || async { Ok::<_, std::io::Error>("premium".to_owned()) },
        |value| if value == "premium" { 300_000 } else { 60_000 },
    ).await?;
    assert_eq!(wrapped.as_str(), "premium");
    Ok(())
}
```

TTL is an `i64` millisecond value: `-1` never expires, `0` immediately
invalidates, positive values expire, and values below `-1` are rejected before
mutation. Hits can report only `-1` or a positive exact remaining TTL.

`lookup_many` and `get_many` preserve input positions and duplicate keys.
`set_many` and `remove_many` visit backends in reverse order. Any read,
backfill, write, removal, clear, or loader failure stops the operation. Earlier
effects remain and are not rolled back.

Use `get_or_load(key, loader, ttl)` when the TTL is known before loading. Use
`wrap(key, loader, ttl)` when the freshly loaded value determines its TTL;
the selector runs only after a miss and successful load. Each invocation is
independent, so concurrent misses for the same key may run duplicate loaders.
The core has no async-runtime, storage, or serialization dependency.
