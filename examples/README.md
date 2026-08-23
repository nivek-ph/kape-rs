# Kape examples

Examples are non-publishable workspace packages and compile as API acceptance
checks.

| Level | Example | Focus | External service |
| --- | --- | --- | --- |
| Start here | `memory` | Cache chain, backfill, `get_or_load`, and value-dependent `wrap` | None |
| Extend Kape | `custom` | Implement `CacheBackend` for another store | None |
| Adapter | `redis` | Connect Redis and inspect a readable cache key | Redis |
| Adapter | `postgres` | Use an application-owned pool and schema | PostgreSQL |
| Integration | `layered` | Verify PostgreSQL hit → Redis and memory backfill | Redis and PostgreSQL |

Run the local examples with:

```text
cargo run -p kape-example-memory
cargo run -p kape-example-custom
```

Redis defaults to `redis://127.0.0.1/`; PostgreSQL requires
`KAPE_POSTGRES_URL`. Both accept `KAPE_NAMESPACE` and default to
`kape-example`. Use a namespace dedicated to the example. These examples leave
their final value in storage so it can be inspected; it expires after 1 minute
in Redis and 6 minutes in PostgreSQL.

```text
KAPE_REDIS_URL=redis://127.0.0.1/ cargo run -p kape-example-redis
KAPE_POSTGRES_URL=postgres://USER:PASSWORD@127.0.0.1/DATABASE \
  cargo run -p kape-example-postgres
```

PostgreSQL schema is application-owned and must exist before the PostgreSQL
example runs:

```sql
CREATE TABLE kape_entries (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    expires_at_ms BIGINT NULL
);
```

The examples use the default `StringCodec`, so an older provisional table with
`BYTEA` key/value columns must be recreated as the `TEXT` schema above before
running them.

Run the integration example after both services and the PostgreSQL schema are
ready. It seeds PostgreSQL, verifies that the first lookup comes from
PostgreSQL, and verifies that the second lookup comes from memory after
backfill. The example cleans up its dedicated namespace when it finishes.

```text
KAPE_REDIS_URL=redis://127.0.0.1/ \
KAPE_POSTGRES_URL=postgres://USER:PASSWORD@127.0.0.1/DATABASE \
  cargo run -p kape-example-layered
```
