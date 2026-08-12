# kape

`kape` is the runtime-independent core of `Kape`. It composes any number of
named cache backends while preserving their user-defined order.

*Kape* means “coffee” in Filipino—a nod to Rust's coffee-named caching
ecosystem.

```rust
use kape::Cache;

# fn example<P, R>(pg_hot: P, redis: R)
# where
#     P: kape::CacheBackend<String, String> + 'static,
#     R: kape::CacheBackend<String, String> + 'static,
# {
let cache = Cache::builder()
    .backend("pg-hot", pg_hot)
    .backend("redis", redis)
    .build()
    .expect("backend names are unique");
# let _ = cache;
# }
```

The core crate does not depend on Moka, Redis, `PostgreSQL`, a serialization
format, or an async runtime. Backend adapters live in sibling workspace crates.

Core operations include:

- `lookup` for hit source, freshness, remaining TTL, and backfill reports;
- `get` for an ergonomic `Option<Arc<V>>` result;
- scalar `has` and non-atomic `take` convenience operations;
- ordered `lookup_many`, `get_many`, `set_many`, `has_many`, and `take_many`;
- deterministic `set` and reverse-order `remove` fan-out;
- reverse-order `clear` and `disconnect`, plus `clear_backend`;
- typed, cursor-based `scan` pages with TTL and freshness metadata;
- `get_or_load` with explicit loader and write-failure policies;
- dynamic per-backend TTL selection for explicit and loader writes;
- per-backend participation, TTL, read-failure, and backfill-failure policies;
- an optional `tracing` feature.

TTL can be selected from the backend name, configured position, typed key, and
loaded value. Returning `None` retains the fallback TTL; the destination
backend's maximum TTL is always applied after the selection:

```rust
use std::{sync::Arc, time::Duration};

use kape::{Cache, CacheBackend, SetItem, TTL};

# async fn example<B>(hot: B, shared: B)
# where
#     B: CacheBackend<String, String> + 'static,
# {
let cache = Cache::builder()
    .backend("hot", hot)
    .backend("shared", shared)
    .build()
    .expect("backend names are unique");

cache
    .set_with_ttl(
        &"user:42".to_owned(),
        Arc::new("premium".to_owned()),
        TTL::Default,
        |context| match (context.backend, context.value.as_str()) {
            ("hot", "premium") => Some(TTL::After(Duration::from_secs(30))),
            ("shared", "premium") => Some(TTL::After(Duration::from_secs(300))),
            _ => None,
        },
    )
    .await
    .expect("write succeeds");

let items = [
    SetItem::new("a".to_owned(), "one".to_owned(), TTL::Never),
    SetItem::new("b".to_owned(), "two".to_owned(), TTL::Default),
];
cache.set_many(&items).await.expect("batch write succeeds");

let keys = ["a".to_owned(), "missing".to_owned(), "a".to_owned()];
let values = cache.get_many(&keys).await.expect("batch read succeeds");
assert_eq!(values.len(), keys.len());
# }
```

Batch inputs and outputs preserve order and duplicate keys. `has_many` checks
fresh existence without warming earlier backends. `take_many` reads without
backfill and then removes in reverse backend order; it is not atomic across
unrelated backend systems.

Every backend implements `clear`. The chain attempts every write-enabled
backend in reverse order and reports failures as named partial failures.
`clear_backend(name)` targets one instance explicitly. Remote adapters must
scope clear to their configured `Kape` namespace.

Iteration always names one backend:

```rust,no_run
use kape::{Cache, KapeError};

async fn visit(cache: &Cache<String, String>) -> Result<(), KapeError> {
let mut cursor = None;
loop {
    let page = cache
        .scan("shared", cursor.as_deref(), 100)
        .await?;
    for entry in page.entries {
        // entry.key, entry.value, entry.remaining_ttl, entry.freshness
    }
    cursor = page.next_cursor;
    if cursor.is_none() {
        break;
    }
}
Ok(())
}
```

The cursor is opaque and only reusable with the same backend instance.
Iteration is weakly consistent under concurrent writes, expiration, and
eviction. `disconnect` visits all backends in reverse order and is idempotent;
handle-managed backends may implement it as a no-op.

The API is pre-release and is not ready for production use. See the
[workspace design document](../../.notes/design.md) for current semantics and
scope.
