# kape-redis

`Redis` backend adapter for `Kape` using `Redis`' asynchronous connection manager.

Serialization is explicit at the adapter boundary:

```rust,no_run
use kape_redis::RedisBackend;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
	let backend = RedisBackend::<String, String>::connect("redis://127.0.0.1/").await?;
	let backend = backend.namespace("my-service");
	let _ = backend;
	Ok(())
}
```

The default `StringCodec` handles `String` keys and values. Use
`with_codec(...)` when an application-specific `RedisCodec<K, V>` is needed;
the codec encodes and decodes keys because iteration returns typed `K` values.
`Kape` never logs encoded keys or values.

Encoded keys are formed as `namespace:key`. Namespace and encoded key bytes
must not contain `:`. Because `clear` and iteration use the namespace directly
as a Redis `SCAN MATCH` pattern, namespace values must also not contain `*`,
`?`, `[`, `]`, or `\\`.

Reads obtain the value and `Redis` PTTL atomically, so remaining-TTL propagation
does not restart a full lifetime. `Redis` normally removes expired values and
therefore does not produce stale candidates.

Batch reads pipeline one atomic GET/PTTL pair per input key. Batch writes use a
transactional pipeline with each item's resolved TTL, `has_many` pipelines
EXISTS, and `remove_many` uses one multi-key DEL. Result order and duplicate
keys match the input.

Iteration uses Redis `SCAN` with an opaque cursor and is weakly consistent.
Clear scans and deletes only keys with this adapter's delimited namespace;
it never issues `FLUSHDB` or `FLUSHALL`. Because Redis `ConnectionManager`
clones own the connection lifetime and automatically reconnect, `disconnect`
is an idempotent no-op; connections are physically released when the final
manager handle is dropped.

TLS is opt-in through alternative `native-tls` and `rustls` Cargo features.
Plain Tokio transport is available without either feature.
