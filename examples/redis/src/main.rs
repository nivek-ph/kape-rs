use std::{env, error::Error, sync::Arc, time::Duration};

use kape::{Cache, IterationFreshness, TTL};
use kape_redis::{RedisBackend, StringCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = env::var("KAPE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_owned());
    let namespace = env::var("KAPE_NAMESPACE")
        .unwrap_or_else(|_| format!("kape-example-{}", std::process::id()));
    let backend = RedisBackend::connect(&url, StringCodec)
        .await?
        .namespace(namespace);
    let cache = Cache::builder().backend("redis", backend).build()?;

    cache
        .set(
            &"user:42".to_owned(),
            Arc::new("Ada".to_owned()),
            TTL::After(Duration::from_mins(1)),
        )
        .await?;
    println!("value: {:?}", cache.get(&"user:42".to_owned()).await?);
    println!("exists: {}", cache.has(&"user:42".to_owned()).await?);

    let page = cache.scan("redis", None, 100).await?;
    for entry in page.entries {
        let freshness = match entry.freshness {
            IterationFreshness::Fresh => "fresh",
            IterationFreshness::Stale => "stale",
        };
        println!("{} = {} ({freshness})", entry.key, entry.value);
    }

    // Clear is restricted to the configured Kape namespace.
    cache.clear().await?;
    cache.disconnect().await?;
    Ok(())
}
