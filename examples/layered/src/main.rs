use std::{env, error::Error, sync::Arc};

use dotenv::dotenv;
use kape::{Cache, CacheBackend, CacheLookup};
use kape_memory::MemoryBackend;
use kape_postgres::PostgresBackend;
use kape_redis::RedisBackend;
use kape_testkit::get_random_string;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let redis_url = env::var("KAPE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_owned());
    let postgres_url = env::var("KAPE_POSTGRES_URL")?;
    let namespace = env::var("KAPE_NAMESPACE")
        .unwrap_or_else(|_| format!("kape-layered-example-{}", std::process::id()));

    let memory = MemoryBackend::<String, String>::new(10_000);
    let redis = RedisBackend::connect(&redis_url)
        .await?
        .namespace(namespace.clone());
    let pool = sqlx::PgPool::connect(&postgres_url).await?;
    let postgres = PostgresBackend::new(pool.clone()).namespace(namespace.clone());

    memory.clear().await?;
    redis.clear().await?;
    postgres.clear().await?;

    let key = get_random_string();
    let value = get_random_string();
    postgres.set(&key, Arc::new(value), 300_000).await?;

    let cache = Cache::builder()
        .backend("memory", memory)
        .backend("redis", redis)
        .backend("postgres", postgres)
        .build()?;

    let CacheLookup::Hit {
        value,
        backend,
        remaining_ttl,
    } = cache.lookup(&key).await?
    else {
        panic!("the seeded PostgreSQL value must be found");
    };
    assert_eq!(backend.as_ref(), "postgres");
    println!("first lookup: {backend} hit {value}, {remaining_ttl}ms remaining");

    let CacheLookup::Hit { backend, .. } = cache.lookup(&key).await? else {
        panic!("the backfilled value must be found");
    };
    assert_eq!(backend.as_ref(), "memory");
    println!("second lookup after backfill: {backend} hit");

    cache.clear().await?;
    drop(cache);
    pool.close().await;
    Ok(())
}
