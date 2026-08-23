use std::{env, error::Error, sync::Arc};

use dotenv::dotenv;
use kape::Cache;
use kape_postgres::PostgresBackend;
use kape_testkit::get_random_string;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let url = env::var("KAPE_POSTGRES_URL")?;
    let namespace = env::var("KAPE_NAMESPACE").unwrap_or_else(|_| "kape-example".to_owned());

    // The application must provision kape_entries through its migrations.
    // Keep the pool clone when the application needs direct lifecycle control.
    let pool = sqlx::PgPool::connect(&url).await?;
    let backend = PostgresBackend::new(pool.clone()).namespace(namespace);
    let cache = Cache::builder().backend("postgres", backend).build()?;

    let key = get_random_string();
    let value = get_random_string();
    cache.set(&key, Arc::new(value), 360_000).await?;
    println!("value: {:?}", cache.get(&key).await?);
    println!("The row is intentionally retained so it can be inspected.");

    drop(cache);
    pool.close().await;
    Ok(())
}
