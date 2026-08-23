# Kape

Composable, type-safe cache chains for Rust.

Kape keeps cache orchestration small and explicit: applications configure one
or more named backends in read order, while adapters own storage and
serialization details.

```rust
let cache = Cache::builder()
    .backend("memory", memory)
    .backend("redis", redis)
    .backend("postgres", postgres)
    .build()?;
```

Reads visit backends in configured order. A miss continues; a hit stops and
refills earlier backends in reverse order with the hit's exact remaining TTL.
Writes, removals, batch mutations, and clear visit the chain in reverse order.
Every failure stops immediately and identifies the operation and backend.

## TTL contract

All public TTL values are `i64` milliseconds:

| TTL | Meaning |
| ---: | --- |
| `-1` | Never expire |
| `0` | Immediately invalidate the key |
| `> 0` | Finite lifetime in milliseconds |
| `< -1` | Invalid; rejected before mutation |

A backend hit reports either `-1` or a positive exact remaining TTL. Expired
entries are misses. Empty strings, zero values, `false`, and empty collections
remain ordinary cached values.

Loaded values can use either a fixed TTL through `get_or_load` or a TTL derived
from the freshly loaded value through `wrap`. Both use the same fail-fast write
path and raw millisecond contract.

## Workspace

| Crate | Responsibility |
| --- | --- |
| [`kape`](crates/kape) | Backend contract and ordered orchestration |
| [`kape-memory`](crates/kape-memory) | Process-local `Arc<V>` storage |
| [`kape-redis`](crates/kape-redis) | Redis adapter with explicit codecs |
| [`kape-postgres`](crates/kape-postgres) | PostgreSQL adapter with explicit codecs |
| [`kape-testkit`](crates/kape-testkit) | Unpublished shared adapter contract tests |

Version `0.1.0` intentionally has no stale fallback, configurable failure
policies, same-key loader coalescing, targeted backend management, or
cross-backend transaction. An error may be returned after earlier writes or
backfills succeeded; Kape does not roll those effects back.

Start with the [getting-started guide](docs/getting-started.md) and the
[runnable examples](examples/README.md). The full public guide begins at
[`docs/README.md`](docs/README.md).

## Verification

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
mdbook build
mdbook test
cargo package -p kape -p kape-memory -p kape-redis -p kape-postgres --allow-dirty
```

Redis and PostgreSQL contract tests require live services and are reported
separately from ordinary workspace tests; see [development](docs/development.md).

Kape requires Rust 1.96 and is licensed under MIT or Apache-2.0.
