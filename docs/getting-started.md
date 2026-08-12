# Getting started

The smallest complete setup uses the runtime-independent `kape` core with the
`kape-memory` adapter.

Until the crates are published, use workspace paths from a checkout:

```toml
[dependencies]
kape = { path = "crates/kape" }
kape-memory = { path = "crates/kape-memory" }
futures-lite = "2"
```

Create named backends in lookup order:

```rust,ignore
use std::{sync::Arc, time::Duration};

use kape::{Cache, TTL};
use kape_memory::MemoryBackend;

# async fn example() -> Result<(), kape::KapeError> {
let hot = MemoryBackend::<String, String>::new(10_000);
let shared = MemoryBackend::<String, String>::new(100_000);

let cache = Cache::builder()
    .backend("hot", hot)
    .backend("shared", shared)
    .build()
    .expect("backend names are unique");

let key = "user:42".to_owned();
cache
    .set(
        &key,
        Arc::new("Ada".to_owned()),
        TTL::After(Duration::from_secs(60)),
    )
    .await?;

let value = cache.get(&key).await?;
assert_eq!(value.as_deref().map(String::as_str), Some("Ada"));
# Ok(())
# }
```

Backend names must be unique even when the underlying adapter type repeats.
Names identify failures, tracing events, policy overrides, iteration targets,
and management operations.

## Selecting the right result API

Use `get` when only the value matters. Use `lookup` when the caller needs the
source backend, freshness, remaining TTL, skipped read failures, or backfill
failures.

```rust,ignore
let value: Option<Arc<Value>> = cache.get(&key).await?;
let result: CacheLookup<Value> = cache.lookup(&key).await?;
```

A backend failure is never converted into an ordinary miss. Policies can
continue after a failure, but raw lookup metadata retains it.

## Toolchain

The workspace declares Rust 1.96 as its minimum supported version and pins the
repository toolchain to Rust 1.96.0. CI verifies the declared MSRV directly.
