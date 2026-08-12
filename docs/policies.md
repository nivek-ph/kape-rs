# TTL and failure policies

Kape keeps expiry, stale data, and failures structurally separate.

## TTL input

Every write uses one of three requests:

- `TTL::Default` uses the destination backend's configured default;
- `TTL::Never` requests no expiry;
- `TTL::After(duration)` requests a specific lifetime.

A destination `max_ttl` caps both explicit and default values. Backfill uses
the source's remaining TTL and can also be capped by the destination's
`backfill_ttl_cap` and `max_ttl`. It does not restart the original lifetime.

`set_with_ttl`, `set_many_with_ttl`, and `get_or_load_with_ttl` can select a
different TTL for every write-enabled backend from the typed backend name,
position, key, and value. Dynamic selection applies to explicit and loader
writes, not read backfill.

## Lookup outcomes

A backend returns one of:

- `Lookup::Miss`;
- `Lookup::Hit` with a value and remaining TTL;
- `Lookup::Stale` with an expired but retained value.

Remaining lifetime is `Never`, `Known(duration)`, or `Unknown`. An unknown or
exhausted remaining TTL is not backfilled by default because doing so could
extend the source entry accidentally.

## Read failures

Each backend chooses a `ReadFailurePolicy`:

- `Propagate` stops and returns the named backend failure;
- `SkipBackend` records the failure and continues;
- `ServeStale` serves the earliest retained stale candidate when a later read
  fails, otherwise it propagates the failure.

Skipped failures remain visible in `CacheLookup`; they are not rewritten as
misses.

## Backfill and loader failures

Backfill has a separate policy because a later backend has already returned a
fresh value. The default reports the warming failure and returns the hit;
`Propagate` makes warming failure fail the lookup.

Loader failures and loader-write failures also have their own policies. This
keeps application loading, cache reads, and cache writes from silently sharing
one error behavior.
