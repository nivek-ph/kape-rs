# Examples

Examples live as non-publishable packages under `examples/` and are members of
the root workspace. This keeps development commands available from the
repository root while each example carries only the dependencies it needs.

## Layered backend chain

The layered example builds `memory -> Redis -> PostgreSQL`. It requires the
`kape_entries` table to exist, seeds only the last backend, and makes the first
lookup backfill both earlier backends with the remaining PostgreSQL TTL. It
also demonstrates dynamic per-backend TTL, ordered batch reads, per-backend
iteration, namespace-scoped clear, and disconnect:

```text
KAPE_REDIS_URL=redis://127.0.0.1/ \
KAPE_POSTGRES_URL=postgres://USER:PASSWORD@127.0.0.1/DATABASE \
  cargo run -p kape-example-layered
```

The example creates a process-specific namespace unless `KAPE_NAMESPACE` is
set.

## In-memory

The complete local example covers ordered lookup and backfill, dynamic TTL,
batch operations, `has`, `take`, iteration, clear, and disconnect:

```text
cargo run -p kape-example-memory
```

It requires no external service.

## Redis

```text
KAPE_REDIS_URL=redis://127.0.0.1/ \
  cargo run -p kape-example-redis
```

The example creates a process-specific namespace unless `KAPE_NAMESPACE` is
set. Its clear operation is namespace-scoped.

## PostgreSQL

```text
KAPE_POSTGRES_URL=postgres://USER:PASSWORD@127.0.0.1/DATABASE \
  cargo run -p kape-example-postgres
```

Provision the configured table through the application's migration system
before running the example. The example calls `check_table()` and never creates
or migrates schema. It calls `disconnect()` last because closing the SQLx pool
affects shared clones.

## Live contract tests

Ordinary workspace tests skip live remote contracts. Run them explicitly with
isolated namespaces:

```text
KAPE_REDIS_URL=redis://127.0.0.1/ \
  cargo test -p kape-redis --test contract -- --ignored

KAPE_POSTGRES_URL=postgres://USER:PASSWORD@127.0.0.1/DATABASE \
  cargo test -p kape-postgres --test contract -- --ignored
```
