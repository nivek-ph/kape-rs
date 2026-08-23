# kape-testkit

Unpublished shared contract checks for Kape backend adapters.

The helpers exercise the public `CacheBackend` seam: scalar and batch
miss/hit behavior, `i64` millisecond TTL boundaries, expiry, duplicate
positions, removal, and clear. Built-in adapters use the same suite while
keeping service-backed Redis and `PostgreSQL` runs explicit. `get_random_string`
supplies unique keys for examples and tests.
