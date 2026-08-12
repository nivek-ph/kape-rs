use std::{env, error::Error, sync::Arc, time::Duration};

use kape::{Cache, CacheBackend, ResolvedTTL, TTL};
use kape_memory::MemoryBackend;
use kape_postgres::{PostgresBackend, StringCodec as PostgresStringCodec};
use kape_redis::{RedisBackend, StringCodec as RedisStringCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let redis_url = env::var("KAPE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_owned());
    let postgres_url = env::var("KAPE_POSTGRES_URL")?;
    let namespace = env::var("KAPE_NAMESPACE")
        .unwrap_or_else(|_| format!("kape-layered-example-{}", std::process::id()));

    let memory = MemoryBackend::<String, String>::new(10_000);
    let redis = RedisBackend::connect(&redis_url, RedisStringCodec)
        .await?
        .namespace(namespace.as_bytes());
    let pool = sqlx::PgPool::connect(&postgres_url).await?;
    let postgres = PostgresBackend::new(pool, PostgresStringCodec).namespace(namespace.as_bytes());
    postgres.check_table().await?;

    // Seed only the last backend. The first lookup will hit PostgreSQL and
    // backfill Redis and memory with PostgreSQL's remaining TTL.
    let cache_key = "cache:42".to_owned();
    postgres
        .set(
            &cache_key,
            Arc::new("Ada".to_owned()),
            ResolvedTTL::After(Duration::from_mins(5)),
        )
        .await?;

    let cache = Cache::builder()
        .backend("memory", memory)
        .backend("redis", redis)
        .backend("postgres", postgres)
        .build()?;

    println!("backend order: {:?}", cache.backend_names());
    println!("first lookup: {:#?}", cache.lookup(&cache_key).await?);

    // The backfilled key is now visible in every backend.
    for backend in ["memory", "redis", "postgres"] {
        scan_entries(&cache, backend).await?;
    }

    let session_key = "session:7".to_owned();
    cache
        .set_with_ttl(
            &session_key,
            Arc::new("active".to_owned()),
            TTL::After(Duration::from_mins(10)),
            |context| match context.backend {
                "memory" => Some(TTL::After(Duration::from_secs(30))),
                "redis" => Some(TTL::After(Duration::from_mins(2))),
                "postgres" => Some(TTL::After(Duration::from_mins(10))),
                _ => None,
            },
        )
        .await?;

    let keys = [
        cache_key.clone(),
        "missing".to_owned(),
        session_key.clone(),
        cache_key,
    ];
    println!("ordered batch: {:#?}", cache.get_many(&keys).await?);

    // Redis and PostgreSQL clear only this process-specific namespace.
    cache.clear().await?;
    // PostgreSQL closes the shared pool; no pool operation may follow this.
    cache.disconnect().await?;
    Ok(())
}

async fn scan_entries(cache: &Cache<String, String>, backend: &str) -> Result<(), kape::Error> {
    let mut cursor = None;
    loop {
        let page = cache.scan(backend, cursor.as_deref(), 100).await?;
        for entry in page.entries {
            println!(
                "{backend}: {} = {} ({:?}, {:?})",
                entry.key, entry.value, entry.freshness, entry.remaining_ttl
            );
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(());
        }
    }
}
