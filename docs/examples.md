# Examples

Runnable, non-publishable packages live under `examples/`:

- `memory`: lookup metadata, elapsed-adjusted backfill, and fixed and value-dependent loaders;
- `custom`: a normal backend using the public scalar contract;
- `redis`: URL construction, readable namespace keys, and finite TTL;
- `postgres`: application-owned pool and externally provisioned schema;
- `layered`: PostgreSQL hit followed by Redis and memory backfill.

Local examples need no service:

```text
cargo run -p kape-example-memory
cargo run -p kape-example-custom
```

Service examples read `KAPE_REDIS_URL`, `KAPE_POSTGRES_URL`, and optionally
`KAPE_NAMESPACE`. The PostgreSQL table must be created by application migration
before running its example. Both examples retain their final value briefly so
the backing store can be inspected after the command finishes.

```text
KAPE_REDIS_URL=redis://127.0.0.1/ cargo run -p kape-example-redis
KAPE_POSTGRES_URL=postgres://USER:PASSWORD@127.0.0.1/DATABASE \
  cargo run -p kape-example-postgres
```

The [repository example index](../examples/README.md) contains the full setup,
including the Redis + PostgreSQL integration command.

Ordinary workspace tests compile examples but skip live service contracts.
Run Redis and PostgreSQL contract tests explicitly and report them separately;
see [development and releases](development.md).
