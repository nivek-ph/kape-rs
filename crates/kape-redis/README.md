# kape-redis

`Redis` backend adapter for `Kape` using `Redis`' asynchronous connection manager.

Serialization is explicit at the adapter boundary:

```rust,no_run
use kape_redis::{RedisBackend, StringCodec};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let backend = RedisBackend::connect("redis://127.0.0.1/", StringCodec).await?;
let backend = backend.namespace("my-service");
# let _ = backend;
# Ok(())
# }
```

Implement `RedisCodec<K, V>` to use application-specific key and value
representations. The codec encodes and decodes keys because iteration returns
typed `K` values. `Kape` never logs encoded keys or values.

Encoded keys are length-framed with the configured namespace, avoiding
delimiter collisions between namespace/key pairs.

Reads obtain the value and `Redis` PTTL atomically, so remaining-TTL propagation
does not restart a full lifetime. `Redis` normally removes expired values and
therefore does not produce stale candidates.

Batch reads pipeline one atomic GET/PTTL pair per input key. Batch writes use a
transactional pipeline with each item's resolved TTL, `has_many` pipelines
EXISTS, and `remove_many` uses one multi-key DEL. Result order and duplicate
keys match the input.

Iteration uses Redis `SCAN` with an opaque cursor and is weakly consistent.
Clear scans and deletes only keys with this adapter's length-framed namespace;
it never issues `FLUSHDB` or `FLUSHALL`. Because Redis `ConnectionManager`
clones own the connection lifetime and automatically reconnect, `disconnect`
is an idempotent no-op; connections are physically released when the final
manager handle is dropped.

TLS is opt-in through alternative `native-tls` and `rustls` Cargo features.
Plain Tokio transport is available without either feature.
