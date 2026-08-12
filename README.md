# Kape

Composable multi-backend caching for Rust.

*Kape* means “coffee” in Filipino—a nod to Rust's coffee-named caching
ecosystem.

Kape is a type-safe orchestration layer for composing any number of named
cache backends in a stable, user-defined order. It does not impose an L1/L2/L3
model, reorder backends, or deduplicate repeated backend implementations.

```rust
let cache = Cache::builder()
    .backend("pg-hot", pg_hot)
    .backend("pg-shared", pg_shared)
    .backend("redis", redis)
    .backend("pg-archive", pg_archive)
    .build()?;
```

The core API retains the caller's `K` and `V` types. Concrete backend types and
their errors are erased only inside the heterogeneous backend chain. Remote
serialization belongs to adapter and codec boundaries; the in-memory backend
stores `Arc<V>` directly.

## Status

The first MVP is implemented across the workspace and its local, Redis, and
PostgreSQL contract suites have been exercised. The public API is still
pre-release and may change before `1.0`.

The public guide starts in [`docs/README.md`](docs/README.md), and the detailed
pre-release design record remains in [`.notes/design.md`](.notes/design.md).
Runnable memory, Redis, and PostgreSQL programs are in
[`examples/`](examples/README.md).

Build the guide locally with:

```text
mdbook build
```

## Workspace

Kape uses a Cargo workspace so that the core remains independent from
backend-specific dependencies and runtimes.

Initial workspace crates:

| Crate | Responsibility |
| --- | --- |
| [`kape`](crates/kape) | Typed backend contract, ordered orchestration, TTL and error policies, queued loading, and observability |
| [`kape-memory`](crates/kape-memory) | In-memory adapter storing `Arc<V>` without byte serialization |
| [`kape-redis`](crates/kape-redis) | Redis adapter and Redis-specific codec/configuration |
| [`kape-postgres`](crates/kape-postgres) | PostgreSQL adapter and PostgreSQL-specific codec/schema configuration |
| [`kape-testkit`](crates/kape-testkit) | Reusable backend contract tests |

### Backend capability matrix

| Capability | Memory | Redis | PostgreSQL | Custom backend |
| --- | --- | --- | --- | --- |
| Typed scalar and batch operations | Yes | Native remote batches | Native SQL batches | Required scalar methods; batch fallbacks |
| Remaining TTL | Monotonic process clock | Atomic `GET`/`PTTL` | PostgreSQL server clock | Backend-defined |
| Retained stale entries | Yes, configurable | No; Redis expires values | Yes, until purged | Backend-defined |
| Namespace-scoped `clear` | Whole memory instance | Framed namespace only | Framed namespace rows only | Optional capability |
| `iteration` | Weakly-consistent offset pages | Weakly-consistent `SCAN` pages | Keyset pages | Optional capability |
| `disconnect` | Handle-managed no-op | Handle-managed no-op | Closes the shared SQLx pool | Idempotent no-op by default |

Iteration always targets one named backend and never merges duplicate keys
across the chain. Redis and PostgreSQL clear operations never clear unrelated
database contents.

Possible later workspace crates include `kape-tags` for version-based tag
invalidation and `kape-sync` for cross-instance invalidation. They are not in
the first MVP.

## MVP

The implemented MVP covers:

- an ordered heterogeneous backend chain with unique instance names;
- explicit miss, hit, stale, and backend-failure outcomes;
- per-backend read, write, backfill, error, and TTL policies;
- remaining-TTL propagation when refilling earlier backends;
- deterministic writes and invalidation with explicit partial failures;
- `get_or_load` with a per-cache, per-key load queue;
- dynamic per-backend TTL selection from the typed key and value;
- ordered batch get/set/has/take with duplicate-key preservation;
- scalar has/take convenience operations;
- namespace-scoped clear, named backend iteration, and explicit disconnect;
- runtime-independent core orchestration;
- raw lookup metadata and optional tracing events;
- memory, Redis, and PostgreSQL adapters;
- shared adapter conformance tests.

Iteration is deliberately backend-specific rather than merged across the
chain: multiple backends may contain the same key with different values and
lifetimes. `clear` and `disconnect` fan out in reverse configured order and
retain named partial failures.

Tag invalidation, cross-instance synchronization, and background writes are
extension work. Their capability boundaries should be preserved by the MVP,
but their behavior is not part of the first release.

## Design influences

Kape draws lessons from `cacheable`, `omniqueue-rs`, `cachet`, and
`multi-tier-cache`, but it is an independent Rust design rather than a
file-by-file port. In particular, backend failures are never silently converted
into ordinary misses.

## Verification

Run the local workspace checks with:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo package --workspace --allow-dirty
```

Remote adapter contracts are ignored by the ordinary workspace test because
they require live services. Run them explicitly with isolated test namespaces:

```text
KAPE_REDIS_URL=redis://127.0.0.1/ \
  cargo test -p kape-redis --test contract -- --ignored

KAPE_POSTGRES_URL=postgres://USER:PASSWORD@127.0.0.1/DATABASE \
  cargo test -p kape-postgres --test contract -- --ignored
```

## License

Kape is licensed under either the [Apache License, Version 2.0](LICENSE-APACHE)
or the [MIT license](LICENSE-MIT), at your option.
