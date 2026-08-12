# kape-postgres

`PostgreSQL` backend adapter for `Kape`.

The adapter stores encoded keys and values as `BYTEA` and expiration as Unix
epoch milliseconds. Remaining TTL is calculated with the `PostgreSQL` server
clock, avoiding application/server clock skew.

```rust,no_run
use kape_postgres::{PostgresBackend, StringCodec};

# async fn example(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
let backend = PostgresBackend::new(pool, StringCodec)
    .namespace("my-service");
backend.check_table().await?;
# let _ = backend;
# Ok(())
# }
```

The adapter never creates or migrates tables. Create the table through the
application's migration system before constructing the cache:

```sql
CREATE TABLE kape_entries (
    key BYTEA PRIMARY KEY,
    value BYTEA NOT NULL,
    expires_at_ms BIGINT NULL
);
```

`check_table()` verifies that the configured table exists without modifying
the database. The default is `kape_entries`. Use
`with_table("schema.table")` to select another validated SQL identifier.

Implement `PostgresCodec<K, V>` for application-specific serialization. The
codec encodes and decodes keys because iteration returns typed `K` values.
`Kape` never logs encoded keys or values.

Encoded keys are length-framed with the configured namespace, avoiding
delimiter collisions between namespace/key pairs.

Batch reads and existence checks use `UNNEST ... WITH ORDINALITY` so misses and
duplicate keys stay in input order. Batch writes execute in one transaction and
therefore preserve duplicate-key last-write order. Batch removal uses a single
`ANY(BYTEA[])` statement.

Iteration uses keyset pagination over framed keys and returns fresh or retained
stale entries with remaining TTL. Clear deletes only rows in the configured
namespace, never the whole table. `disconnect` calls `PgPool::close`; because
`SQLx` pool handles share one pool, this also closes any clones of that pool and
must be the final operation during shutdown.

TLS is opt-in through the `native-tls` and `rustls` Cargo features. Plain Tokio
transport is available without either feature.
