use std::{env, error::Error, sync::Arc, time::Duration};

use kape::{Cache, IterationFreshness, TTL};
use kape_postgres::{PostgresBackend, StringCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = env::var("KAPE_POSTGRES_URL")?;
    let namespace = env::var("KAPE_NAMESPACE")
        .unwrap_or_else(|_| format!("kape-example-{}", std::process::id()));
    let pool = sqlx::PgPool::connect(&url).await?;
    let backend = PostgresBackend::new(pool, StringCodec).namespace(namespace);
    backend.check_table().await?;
    let cache = Cache::builder().backend("postgres", backend).build()?;

    cache
        .set(
            &"user:42".to_owned(),
            Arc::new("Ada".to_owned()),
            TTL::After(Duration::from_mins(1)),
        )
        .await?;
    println!("value: {:?}", cache.get(&"user:42".to_owned()).await?);
    println!("exists: {}", cache.has(&"user:42".to_owned()).await?);

    let page = cache.scan("postgres", None, 100).await?;
    for entry in page.entries {
        let freshness = match entry.freshness {
            IterationFreshness::Fresh => "fresh",
            IterationFreshness::Stale => "stale",
        };
        println!("{} = {} ({freshness})", entry.key, entry.value);
    }

    // Clear deletes only rows in this backend's framed namespace.
    cache.clear().await?;
    // PostgreSQL disconnect closes the shared SQLx pool and must be last.
    cache.disconnect().await?;
    Ok(())
}
