# Backends

The workspace provides three adapters and a shared internal test kit.

| Crate | Storage | Stale values | Clear | Iteration | Disconnect |
| --- | --- | --- | --- | --- | --- |
| `kape-memory` | Direct `Arc<V>` | Retained by default | Whole backend instance | Weakly-consistent offset pages | Handle-managed no-op |
| `kape-redis` | Codec-encoded bytes | Normally unavailable after Redis expiry | Current framed namespace only | Weakly-consistent `SCAN` | Handle-managed no-op |
| `kape-postgres` | Codec-encoded `BYTEA` | Retained rows | Current framed namespace rows only | Keyset pagination | Closes the shared SQLx pool |
| `kape-testkit` | Test utility | Contract-dependent | Contract checks | Contract checks | Not published |

## Memory

`MemoryBackend<K, V>` stores `Arc<V>` directly. Moka is the current private
storage engine; no Moka type appears in the public API. Set
`retain_stale(false)` when expired values should be invalidated on read instead
of retained as stale candidates.

## Redis

`RedisBackend` uses a `RedisCodec<K, V>` for keys and values. Reads obtain the
value and `PTTL` atomically, and batch operations use native pipelines. Keys
are length-framed with the namespace so delimiter collisions cannot merge two
logical namespaces.

`clear` scans and deletes only keys in the adapter's namespace. It never uses
`FLUSHDB` or `FLUSHALL`.

Environment variables used by the example and live contract test:

- `KAPE_REDIS_URL`
- `KAPE_NAMESPACE`

## PostgreSQL

`PostgresBackend` uses a `PostgresCodec<K, V>`, stores encoded keys and values
as `BYTEA`, and evaluates expiry with the PostgreSQL server clock. The default
table is `kape_entries`; `with_table` accepts another validated SQL identifier.

The adapter never creates or migrates the table. Applications provision it
through their own migration system:

```sql
CREATE TABLE kape_entries (
    key BYTEA PRIMARY KEY,
    value BYTEA NOT NULL,
    expires_at_ms BIGINT NULL
);
```

Call `check_table()` during startup when an explicit existence check is useful.
It performs no schema changes; normal database errors still surface if the
table is removed after the check.

`clear` deletes only rows in the framed namespace. `disconnect` calls
`PgPool::close()`, which closes all handles sharing that pool, so it must be the
last shutdown operation for that pool.

Environment variables used by the example and live contract test:

- `KAPE_POSTGRES_URL`
- `KAPE_NAMESPACE`

TLS is opt-in for both remote adapters through their `native-tls` or `rustls`
features.
