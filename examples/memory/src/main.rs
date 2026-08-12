use std::{error::Error, sync::Arc, time::Duration};

use kape::{Cache, CacheBackend, IterationFreshness, ResolvedTTL, SetItem, TTL};
use kape_memory::MemoryBackend;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let hot = MemoryBackend::<String, String>::new(1_000);
    let shared = MemoryBackend::<String, String>::new(10_000);
    // Seed only the later backend so the first read demonstrates backfill.
    let cache_key = "cache:42".to_owned();
    shared
        .set(
            &cache_key,
            Arc::new("Ada".to_owned()),
            ResolvedTTL::After(Duration::from_mins(5)),
        )
        .await?;

    let cache = Cache::builder()
        .backend("hot", hot)
        .backend("shared", shared)
        .build()?;

    let value = cache.get(&cache_key).await?;
    println!("later-backend hit: {value:?}");

    cache
        .set_with_ttl(
            &"session:7".to_owned(),
            Arc::new("active".to_owned()),
            TTL::Never,
            |context| match context.backend {
                "hot" => Some(TTL::After(Duration::from_secs(30))),
                "shared" => Some(TTL::After(Duration::from_mins(5))),
                _ => None,
            },
        )
        .await?;

    cache
        .set_many(&[
            SetItem::new("feature:a".to_owned(), "on".to_owned(), TTL::Never),
            SetItem::new(
                "feature:b".to_owned(),
                "off".to_owned(),
                TTL::After(Duration::from_mins(1)),
            ),
        ])
        .await?;

    let keys = ["feature:a".to_owned(), "missing".to_owned()];
    println!("batch values: {:?}", cache.get_many(&keys).await?);
    println!(
        "session exists: {}",
        cache.has(&"session:7".to_owned()).await?
    );

    let mut cursor = None;
    loop {
        let page = cache.scan("hot", cursor.as_deref(), 100).await?;
        for entry in page.entries {
            let freshness = match entry.freshness {
                IterationFreshness::Fresh => "fresh",
                IterationFreshness::Stale => "stale",
            };
            println!("{} = {} ({freshness})", entry.key, entry.value);
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    println!(
        "taken session: {:?}",
        cache.take(&"session:7".to_owned()).await?
    );
    cache.clear().await?;
    cache.disconnect().await?;
    Ok(())
}
