# Backends

All adapters satisfy the same public contract, but storage behavior remains
adapter-local.

| Crate | Storage | Time source | Clear scope |
| --- | --- | --- | --- |
| `kape-memory` | Direct `Arc<V>` | Monotonic process clock | One backend instance |
| `kape-redis` | Codec bytes | Redis PTTL | Caller-owned string prefix |
| `kape-postgres` | Codec-selected `TEXT` or `BYTEA` | PostgreSQL server clock | Caller-owned string prefix |

## Custom backends

Implement `CacheBackend<K, V>` and return `None` for a miss or
`Some(CacheEntry)` for a hit. A hit must report `-1` or a positive exact
remaining TTL. Scalar and batch writes must reject TTL below `-1` before
mutation; TTL zero must leave no observable entry. Batch reads preserve input
length, order, and duplicates. `clear` must affect only the backend's documented
ownership scope.

The shared testkit exercises these observable guarantees. Adapter-local native
batches may reduce transport round trips, but cannot change public semantics.

## Memory

Capacity is a maximum entry count. A fresh value may be evicted before its TTL,
so TTL is an upper freshness bound rather than a retention guarantee. Expired
entries are removed on observation. Clones share storage; separately created
instances and their clear operations are isolated.

## Redis

Redis keys use the caller's namespace directly, for example
`kape:my-service:user:42`. The caller owns uniqueness and storage safety; a
simple identifier containing letters, digits, `-`, or `_` is recommended. The
adapter does not encode, escape, or validate the namespace. Redis glob
characters can broaden clear because it performs one bounded `SCAN` of
`kape:<namespace>:*`. It never flushes the database. Scanning a large Redis
database can be expensive; applications should choose when to invoke clear
accordingly. Clear is not linearizable with concurrent writes to the same
namespace.

Reads pair `GET` and `PTTL`; native pipelines preserve batch positions and
duplicates. Applications may construct from a URL or retain their own
connection-manager clone. The default codec uses `String` keys and values;
`BytesCodec` supports raw `Vec<u8>` data.

## PostgreSQL

Applications own the pool, DDL, migrations, deployment validation, and
table-wide maintenance. The minimum table is:

```sql
CREATE TABLE kape_entries (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    expires_at_ms BIGINT NULL
);
```

The adapter validates configurable `table` or `schema.table` identifiers.
`clear` and `purge_expired` are namespace-scoped. Purging is explicit and the
application schedules it. Batch writes use one PostgreSQL transaction only;
that transaction does not include other backends. The default `StringCodec`
uses `String` with `TEXT` columns. `BytesCodec` uses raw `Vec<u8>` with `BYTEA`
columns. Applications create a schema whose key and value column types match
the selected codec. A provisional `BYTEA` table does not match the default
`StringCodec`; recreate the disposable cache table as `TEXT` or explicitly use
`BytesCodec`.

Redis and PostgreSQL have separate codec traits. Codec failures retain their
adapter-specific source when the cache adds a backend name and operation.
