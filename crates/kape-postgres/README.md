# kape-postgres

`PostgreSQL` adapter for Kape.

```toml
[dependencies]
kape = "0.1.0"
kape-postgres = "0.1.0"
sqlx = { version = "0.9", default-features = false, features = ["postgres", "runtime-tokio"] }
```

```rust,no_run
use kape_postgres::PostgresBackend;

fn example(pool: sqlx::PgPool) {
    // Retain a pool clone if the application needs lifecycle control.
    let backend = PostgresBackend::<String, String>::new(pool).namespace("my-service");
    let _ = backend;
}
```

The application must create and migrate the table. The minimum default schema
is:

```sql
CREATE TABLE kape_entries (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    expires_at_ms BIGINT NULL
);
```

The adapter performs no DDL or startup table check. `with_table("schema.table")`
fallibly selects a validated identifier. Applications keep their own `PgPool`
clone for direct use and shutdown.

`PostgresBackend` defaults to `String` keys, `String` values, and `StringCodec`.
`StringCodec` maps to `TEXT` columns, so default keys and values remain readable
in `PostgreSQL` tools. `BytesCodec` maps `Vec<u8>` keys and values to `BYTEA`
columns without text conversion. Custom `PostgresCodec<K, V>` implementations
select `String` or `Vec<u8>` associated storage types, and the application must
create matching column types. `clear` and `purge_expired` affect only the
configured namespace. Applications decide when to schedule purging and remain
responsible for table-wide maintenance.

An existing provisional `BYTEA` table does not match the default
`StringCodec`. Because Kape is unreleased and cache rows are disposable,
applications should recreate that cache table with the default `TEXT` schema
or explicitly select `BytesCodec` and continue using a `BYTEA` schema.

For a bytes-only table, use `BytesCodec` with this schema instead:

```sql
CREATE TABLE kape_entries (
    key BYTEA PRIMARY KEY,
    value BYTEA NOT NULL,
    expires_at_ms BIGINT NULL
);
```

```rust,no_run
use kape_postgres::{BytesCodec, PostgresBackend};

fn bytes_example(pool: sqlx::PgPool) {
    let backend = PostgresBackend::<Vec<u8>, Vec<u8>>::new(pool)
        .with_codec(BytesCodec);
    let _ = backend;
}
```

Finite expiry timestamps are calculated from the application host's Unix clock;
remaining TTL and expired-row cleanup use the `PostgreSQL` server clock. Keep
the application and database hosts time-synchronized. Batch reads use
ordinality to retain misses and duplicate positions. Batch writes reject
duplicate keys, use one bulk UPSERT statement, and use a local transaction only
when the same batch also contains deletions. This does not create a transaction
across a Kape chain.

TLS is opt-in through the `native-tls` and `rustls` features.
