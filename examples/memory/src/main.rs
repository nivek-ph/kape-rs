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
    let loader = || async { Ok::<_, std::io::Error>(get_random_string()) };
    let value = cache.get_or_load(&load_key, loader, 60_000).await?;
    println!("loaded: {value}");

    let key = get_random_string();
    let get_ttl = |value: &String| {
        if value.as_bytes().first().is_some_and(u8::is_ascii_uppercase) {
            3_000
        } else {
            6_000
        }
    };
    let wrapped = cache.wrap(&key, loader, get_ttl).await?;
    println!("wrapped: {wrapped}");

    cache.clear().await?;
    Ok(())
}
