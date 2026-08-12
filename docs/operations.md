# Operations

Kape exposes value-oriented convenience methods and metadata-preserving
operations.

## Scalar operations

- `lookup` returns source, freshness, remaining TTL, and non-fatal failures.
- `get` returns `Option<Arc<V>>`.
- `set` writes to enabled backends in order.
- `has` checks fresh existence without warming earlier backends.
- `remove` invalidates in reverse backend order.
- `take` reads without backfill and then removes in reverse order.

`take` is not atomic across unrelated backend systems.

## Batch operations

`lookup_many`, `get_many`, `set_many`, `has_many`, `remove_many`, and
`take_many` preserve input order and duplicate keys. Adapters can override the
default scalar fallbacks with native Redis pipelines or PostgreSQL batch SQL.

Duplicate writes retain their ordinary sequential meaning. Remote adapters
preserve that meaning even when they optimize the transport.

## Load queue

`get_or_load` and its policy/TTL variants enqueue concurrent misses per cache
and key. One leader runs the loader while queued waiters receive the same
result. Completion, cancellation, or panic dequeues the load so later calls can
retry.

The load queue is runtime-independent and scoped to one `Cache` instance. It is
not distributed locking across processes.

## Management operations

- `clear()` visits write-enabled backends in reverse order and aggregates
  partial failures.
- `clear_backend(name)` targets exactly one named backend.
- `scan(name, cursor, limit)` scans one backend and never merges
  values across the chain.
- `disconnect()` visits backends in reverse order, is idempotent, and aggregates
  failures.

Iteration cursors are opaque backend bytes. They are only reusable with the
same backend instance, and pages are weakly consistent under concurrent writes,
expiry, and eviction.
