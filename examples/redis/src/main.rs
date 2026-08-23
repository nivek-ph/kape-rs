use std::{env, error::Error, sync::Arc};

use dotenv::dotenv;
use kape::Cache;
use kape_redis::RedisBackend;
use kape_testkit::get_random_string;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let url = env::var("KAPE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_owned());
    let namespace = env::var("KAPE_NAMESPACE").unwrap_or_else(|_| "kape-example".to_owned());
    let backend = RedisBackend::connect(&url)
        .await?
        .namespace(namespace.clone());
    let cache = Cache::builder().backend("redis", backend).build()?;

    let key = get_random_string();
    let value = get_random_string();
    cache.set(&key, Arc::new(value), 60_000).await?;
    println!("value: {:?}", cache.get(&key).await?);
    println!("Redis key: kape:{namespace}:{key}");

    Ok(())
}
