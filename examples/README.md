# Kape examples

The examples are non-publishable packages in the root Cargo workspace. Each
example keeps only the dependencies and runtime it needs.

## Layered backend chain

This example composes `memory -> Redis -> PostgreSQL`. The `kape_entries` table
must already exist. It seeds only
PostgreSQL, demonstrates a later-backend hit and remaining-TTL backfill into
Redis and memory, then exercises per-backend TTL, ordered batch reads,
iteration, namespace-scoped clear, and shutdown:

```text
KAPE_REDIS_URL=redis://127.0.0.1/ \
KAPE_POSTGRES_URL=postgres://USER:PASSWORD@127.0.0.1/DATABASE \
  cargo run -p kape-example-layered
```

`KAPE_REDIS_URL` defaults to local Redis. Set `KAPE_NAMESPACE` to override the
process-specific namespace shared by the two remote adapters.

## Ordered in-memory cache

This example needs no external service. It demonstrates ordered lookup and
backfill, dynamic per-backend TTL, batch operations, `has`, `take`, iteration,
clear, and disconnect:

```text
cargo run -p kape-example-memory
```

## Redis

The Redis example uses a process-specific namespace by default. `clear()` only
deletes keys inside that namespace:

```text
KAPE_REDIS_URL=redis://127.0.0.1/ \
  cargo run -p kape-example-redis
```

Set `KAPE_NAMESPACE` when a stable application namespace is required.

## PostgreSQL

The PostgreSQL example requires a pre-provisioned `kape_entries` table and uses
a process-specific namespace:

```text
KAPE_POSTGRES_URL=postgres://USER:PASSWORD@127.0.0.1/DATABASE \
  cargo run -p kape-example-postgres
```

Create the table through the application's migration system using the DDL in
the `kape-postgres` README. The example calls `check_table()` and fails without
modifying the database when the table is absent. `disconnect()` closes the
shared SQLx pool, so the example calls it last.
