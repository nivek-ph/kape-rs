use std::{error::Error, sync::Arc};

use kape::{Cache, CacheBackend, CacheLookup};
use kape_memory::MemoryBackend;
use kape_testkit::get_random_string;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let hot = MemoryBackend::<String, String>::new(1_000);
    let shared = MemoryBackend::<String, String>::new(10_000);
    let key = get_random_string();
    let value = get_random_string();

    // A later-backend hit is returned with its exact remaining TTL and refills
    // every earlier backend.
    shared.set(&key, Arc::new(value), 300_000).await?;
    let cache = Cache::builder()
        .backend("hot", hot)
        .backend("shared", shared)
        .build()?;

    match cache.lookup(&key).await? {
        CacheLookup::Hit {
            value,
            backend,
            remaining_ttl,
        } => println!("{backend} hit: {value}, {remaining_ttl}ms remaining"),
        CacheLookup::Miss => println!("miss"),
    }

    let load_key = get_random_string();
    let loaded = cache
        .get_or_load(
            &load_key,
            || async { Ok::<_, std::io::Error>(get_random_string()) },
            60_000,
        )
        .await?;
    println!("loaded: {loaded}");

    let wrap_key = get_random_string();
    let wrapped = cache
        .wrap(
            &wrap_key,
            || async { Ok::<_, std::io::Error>(get_random_string()) },
            |value| {
                if value.as_bytes().first().is_some_and(u8::is_ascii_uppercase) {
                    300_000
                } else {
                    60_000
                }
            },
        )
        .await?;
    println!("wrapped: {wrapped}");

    cache.clear().await?;
    Ok(())
}
