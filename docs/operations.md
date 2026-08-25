# Operations

## Scalar reads and mutations

- `lookup` reads in configured order and returns miss or hit metadata.
- `get` applies the same read and returns only `Option<Arc<V>>`.
- `set`, `remove`, and `clear` visit backends in reverse order.

A miss continues. A hit stops later reads and backfills earlier backends in
reverse order without extending its observed lifetime. Time spent reading and
refilling is deducted from finite TTLs. Read and backfill failures return
`Err`; absence alone is an `Ok` miss.

Mutations stop at the first failure. Earlier backend effects remain. There is
no cross-backend rollback or transaction, and an error never guarantees that
the cache chain is unchanged.

## Batch operations

`lookup_many`, `get_many`, `set_many`, and `remove_many` preserve input
positions and duplicate keys. Empty input succeeds without calling a backend.

Reads query unresolved positions through the chain. Each hit position is
backfilled independently, including duplicate positions. A wrong result count,
read failure, invalid hit, or any backfill failure makes the entire public
operation return `Err`; no partial result vector is returned. Earlier effects
remain.

Batch mutations visit backends in reverse order and fail fast. All write TTLs
are validated before the first backend mutation.

## Loading

```rust,ignore
let value = cache
    .get_or_load(
        &key,
        || async { load_value().await },
        60_000,
    )
    .await?;
```

`get_or_load(key, loader, ttl)` first performs an ordinary lookup. On miss it
runs the loader once for that invocation and writes through the ordinary
reverse set path. Loader computation errors become `KapeError::Loader`; write
errors are named `set` failures.

When the loaded value determines its own lifetime, use `wrap`:

```rust,ignore
let value = cache
    .wrap(
        &key,
        || async { load_value().await },
        |value| {
            if value.is_premium() { 300_000 } else { 60_000 }
        },
    )
    .await?;
```

`wrap(key, loader, ttl)` performs the same lookup and reverse write as
`get_or_load`. On a hit it runs neither the loader nor the selector. On a miss,
the selector receives the successfully loaded value and returns its raw `i64`
millisecond TTL. Values below `-1` fail before any backend write. The one
selected TTL applies to the complete backend chain; there are no global hooks,
per-backend selectors, defaults, or additional wrap variants.

TTL zero from either loader API runs the loader, invalidates the full chain,
and returns the loaded value only if every invalidation succeeds. Concurrent
misses for the same key are independent and may run duplicate loaders; `0.1.0`
has no coalescing.

## Errors

Named backend failures expose both backend and the typed `Operation` enum:
`Get`, `Backfill`, `Set`, `Remove`, or `Clear`. Fail-fast behavior prevents
later backend calls after an error but does not undo earlier writes or
backfills.
