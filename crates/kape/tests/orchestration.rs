use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_lite::future::block_on;
use kape::{
    BackendFailure, Cache, CacheBackend, CacheEntry, CacheHit, KapeError, KapeResult, Operation,
    SetItem,
};

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Get(&'static str, String),
    GetMany(&'static str, Vec<String>),
    Set(&'static str, String, i64),
    SetMany(&'static str, Vec<(String, i64)>),
    Remove(&'static str, String),
    RemoveMany(&'static str, Vec<String>),
    Clear(&'static str),
}

#[derive(Clone)]
struct RecordingBackend {
    name: &'static str,
    entries: Arc<Mutex<HashMap<String, CacheEntry<String>>>>,
    events: Arc<Mutex<Vec<Event>>>,
    failures: HashSet<&'static str>,
    failing_set_keys: HashSet<String>,
    wrong_batch_len: bool,
    set_return_delay: Option<Duration>,
    get_many_return_delay: Option<Duration>,
}

impl RecordingBackend {
    fn new(name: &'static str, events: Arc<Mutex<Vec<Event>>>) -> Self {
        Self {
            name,
            entries: Arc::new(Mutex::new(HashMap::new())),
            events,
            failures: HashSet::new(),
            failing_set_keys: HashSet::new(),
            wrong_batch_len: false,
            set_return_delay: None,
            get_many_return_delay: None,
        }
    }

    fn entry(self, key: &str, value: &str, remaining_ttl: i64) -> Self {
        self.entries.lock().expect("entries mutex poisoned").insert(
            key.to_owned(),
            CacheEntry::new(Arc::new(value.to_owned()), remaining_ttl),
        );
        self
    }

    fn failing_get(mut self) -> Self {
        self.failures.insert("get");
        self
    }

    fn failing_set(mut self) -> Self {
        self.failures.insert("set");
        self
    }

    fn delaying_set_return(mut self, delay: Duration) -> Self {
        self.set_return_delay = Some(delay);
        self
    }

    fn failing_set_for(mut self, key: &str) -> Self {
        self.failing_set_keys.insert(key.to_owned());
        self
    }

    fn delaying_get_many_return(mut self, delay: Duration) -> Self {
        self.get_many_return_delay = Some(delay);
        self
    }

    fn failing_remove(mut self) -> Self {
        self.failures.insert("remove");
        self
    }

    fn failing_clear(mut self) -> Self {
        self.failures.insert("clear");
        self
    }

    fn wrong_batch_len(mut self) -> Self {
        self.wrong_batch_len = true;
        self
    }

    fn record(&self, event: Event) {
        self.events
            .lock()
            .expect("events mutex poisoned")
            .push(event);
    }
}

#[async_trait::async_trait]
impl CacheBackend<String, String> for RecordingBackend {
    async fn get(&self, key: &String) -> KapeResult<Option<CacheEntry<String>>> {
        self.record(Event::Get(self.name, key.clone()));
        if self.failures.contains("get") {
            return Err(KapeError::backend(TestError("get failed")));
        }
        Ok(self
            .entries
            .lock()
            .expect("entries mutex poisoned")
            .get(key)
            .cloned())
    }

    async fn set(&self, key: &String, value: Arc<String>, ttl: i64) -> KapeResult<()> {
        self.record(Event::Set(self.name, key.clone(), ttl));
        if self.failures.contains("set") || self.failing_set_keys.contains(key) {
            return Err(KapeError::backend(TestError("set failed")));
        }
        let mut entries = self.entries.lock().expect("entries mutex poisoned");
        if ttl == 0 {
            entries.remove(key);
        } else {
            entries.insert(key.clone(), CacheEntry::new(value, ttl));
        }
        drop(entries);
        if let Some(delay) = self.set_return_delay {
            std::thread::sleep(delay);
        }
        Ok(())
    }

    async fn remove(&self, key: &String) -> KapeResult<()> {
        self.record(Event::Remove(self.name, key.clone()));
        if self.failures.contains("remove") {
            return Err(KapeError::backend(TestError("remove failed")));
        }
        self.entries
            .lock()
            .expect("entries mutex poisoned")
            .remove(key);
        Ok(())
    }

    async fn clear(&self) -> KapeResult<()> {
        self.record(Event::Clear(self.name));
        if self.failures.contains("clear") {
            return Err(KapeError::backend(TestError("clear failed")));
        }
        self.entries.lock().expect("entries mutex poisoned").clear();
        Ok(())
    }

    async fn get_many(&self, keys: &[&String]) -> KapeResult<Vec<Option<CacheEntry<String>>>> {
        self.record(Event::GetMany(
            self.name,
            keys.iter().map(|key| (*key).clone()).collect(),
        ));
        if self.failures.contains("get") {
            return Err(KapeError::backend(TestError("get failed")));
        }
        let entries = self.entries.lock().expect("entries mutex poisoned");
        let mut results = keys
            .iter()
            .map(|key| entries.get(*key).cloned())
            .collect::<Vec<_>>();
        if self.wrong_batch_len {
            results.pop();
        }
        if let Some(delay) = self.get_many_return_delay {
            std::thread::sleep(delay);
        }
        Ok(results)
    }

    async fn set_many(&self, items: &[SetItem<&String, String>]) -> KapeResult<()> {
        self.record(Event::SetMany(
            self.name,
            items
                .iter()
                .map(|item| (item.key.clone(), item.ttl))
                .collect(),
        ));
        if self.failures.contains("set") {
            return Err(KapeError::backend(TestError("set failed")));
        }
        let mut entries = self.entries.lock().expect("entries mutex poisoned");
        for item in items {
            if item.ttl == 0 {
                entries.remove(item.key);
            } else {
                entries.insert(
                    item.key.clone(),
                    CacheEntry::new(Arc::clone(&item.value), item.ttl),
                );
            }
        }
        Ok(())
    }

    async fn remove_many(&self, keys: &[&String]) -> KapeResult<()> {
        self.record(Event::RemoveMany(
            self.name,
            keys.iter().map(|key| (*key).clone()).collect(),
        ));
        if self.failures.contains("remove") {
            return Err(KapeError::backend(TestError("remove failed")));
        }
        let mut entries = self.entries.lock().expect("entries mutex poisoned");
        for key in keys {
            entries.remove(*key);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct TestError(&'static str);

impl fmt::Display for TestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for TestError {}

#[test]
fn reads_in_order_and_deducts_elapsed_time_before_each_backfill_write() {
    block_on(async {
        let events = events();
        let hot = RecordingBackend::new("hot", Arc::clone(&events));
        let warm = RecordingBackend::new("warm", Arc::clone(&events));
        let cold = RecordingBackend::new("cold", Arc::clone(&events)).entry("key", "value", 60_000);
        let cache = Cache::builder()
            .backend("hot", hot)
            .backend("warm", warm)
            .backend("cold", cold)
            .build()
            .expect("cache should build");

        let lookup = cache
            .lookup(&"key".to_owned())
            .await
            .expect("lookup should succeed");
        assert!(matches!(
            lookup,
            Some(CacheHit {
                ref backend,
                entry: CacheEntry {
                    ref value,
                    remaining_ttl: 60_000,
                },
            }) if value.as_str() == "value" && backend.as_ref() == "cold"
        ));
        let events = take_events(&events);
        assert_eq!(
            &events[..3],
            &[
                Event::Get("hot", "key".to_owned()),
                Event::Get("warm", "key".to_owned()),
                Event::Get("cold", "key".to_owned()),
            ]
        );
        assert!(matches!(
            &events[3..],
            [
                Event::Set("warm", warm_key, warm_ttl),
                Event::Set("hot", hot_key, hot_ttl),
            ] if warm_key == "key"
                && hot_key == "key"
                && *warm_ttl > 0
                && *warm_ttl < 60_000
                && *hot_ttl > 0
                && *hot_ttl <= *warm_ttl
        ));
    });
}

#[test]
fn first_hit_does_not_backfill_or_read_later_backends() {
    block_on(async {
        let events = events();
        let hot = RecordingBackend::new("hot", Arc::clone(&events)).entry("key", "value", -1);
        let cold = RecordingBackend::new("cold", Arc::clone(&events)).entry("key", "old", -1);
        let cache = Cache::builder()
            .backend("hot", hot)
            .backend("cold", cold)
            .build()
            .expect("cache should build");

        cache
            .get(&"key".to_owned())
            .await
            .expect("lookup should succeed");
        assert_eq!(take_events(&events), [Event::Get("hot", "key".to_owned())]);
    });
}

#[test]
fn elapsed_time_between_backfill_writes_can_skip_the_next_backend() {
    block_on(async {
        let events = events();
        let cache = Cache::builder()
            .backend("hot", RecordingBackend::new("hot", Arc::clone(&events)))
            .backend(
                "warm",
                RecordingBackend::new("warm", Arc::clone(&events))
                    .delaying_set_return(Duration::from_millis(75)),
            )
            .backend(
                "cold",
                RecordingBackend::new("cold", Arc::clone(&events)).entry("key", "value", 50),
            )
            .build()
            .expect("cache should build");

        let hit = cache
            .lookup(&"key".to_owned())
            .await
            .expect("lookup should succeed")
            .expect("cold backend should hit");
        assert_eq!(hit.backend.as_ref(), "cold");

        let events = take_events(&events);
        assert_eq!(
            &events[..3],
            &[
                Event::Get("hot", "key".to_owned()),
                Event::Get("warm", "key".to_owned()),
                Event::Get("cold", "key".to_owned()),
            ]
        );
        assert!(matches!(
            &events[3..],
            [Event::Set("warm", key, ttl)] if key == "key" && *ttl > 0 && *ttl < 50
        ));
    });
}

#[test]
fn mutations_run_in_reverse_and_stop_on_first_failure() {
    block_on(async {
        let events = events();
        let cache = Cache::builder()
            .backend("hot", RecordingBackend::new("hot", Arc::clone(&events)))
            .backend(
                "warm",
                RecordingBackend::new("warm", Arc::clone(&events)).failing_set(),
            )
            .backend("cold", RecordingBackend::new("cold", Arc::clone(&events)))
            .build()
            .expect("cache should build");

        let error = cache
            .set(&"key".to_owned(), Arc::new("value".to_owned()), -1)
            .await
            .expect_err("middle failure should propagate");
        assert_backend_failure(error, Operation::Set, "warm");
        assert_eq!(
            take_events(&events),
            [
                Event::Set("cold", "key".to_owned(), -1),
                Event::Set("warm", "key".to_owned(), -1),
            ]
        );
    });
}

#[test]
fn remove_and_clear_are_reverse_and_fail_fast() {
    block_on(async {
        let remove_events = events();
        let remove_cache = Cache::builder()
            .backend(
                "hot",
                RecordingBackend::new("hot", Arc::clone(&remove_events)),
            )
            .backend(
                "warm",
                RecordingBackend::new("warm", Arc::clone(&remove_events)).failing_remove(),
            )
            .backend(
                "cold",
                RecordingBackend::new("cold", Arc::clone(&remove_events)),
            )
            .build()
            .expect("cache should build");
        let error = remove_cache
            .remove(&"key".to_owned())
            .await
            .expect_err("remove failure should propagate");
        assert_backend_failure(error, Operation::Remove, "warm");
        assert_eq!(
            take_events(&remove_events),
            [
                Event::Remove("cold", "key".to_owned()),
                Event::Remove("warm", "key".to_owned()),
            ]
        );

        let clear_events = events();
        let clear_cache = Cache::builder()
            .backend(
                "hot",
                RecordingBackend::new("hot", Arc::clone(&clear_events)),
            )
            .backend(
                "warm",
                RecordingBackend::new("warm", Arc::clone(&clear_events)).failing_clear(),
            )
            .backend(
                "cold",
                RecordingBackend::new("cold", Arc::clone(&clear_events)),
            )
            .build()
            .expect("cache should build");
        let error = clear_cache
            .clear()
            .await
            .expect_err("clear failure should propagate");
        assert_backend_failure(error, Operation::Clear, "warm");
        assert_eq!(
            take_events(&clear_events),
            [Event::Clear("cold"), Event::Clear("warm")]
        );
    });
}

#[test]
fn read_and_backfill_failures_are_named_and_fail_fast() {
    block_on(async {
        let read_events = events();
        let read_cache = Cache::builder()
            .backend(
                "broken",
                RecordingBackend::new("broken", Arc::clone(&read_events)).failing_get(),
            )
            .backend(
                "later",
                RecordingBackend::new("later", Arc::clone(&read_events)).entry("key", "value", -1),
            )
            .build()
            .expect("cache should build");
        let error = read_cache
            .get(&"key".to_owned())
            .await
            .expect_err("read failure should propagate");
        assert_backend_failure(error, Operation::Get, "broken");
        assert_eq!(
            take_events(&read_events),
            [Event::Get("broken", "key".to_owned())]
        );

        let backfill_events = events();
        let backfill_cache = Cache::builder()
            .backend(
                "hot",
                RecordingBackend::new("hot", Arc::clone(&backfill_events)).failing_set(),
            )
            .backend(
                "cold",
                RecordingBackend::new("cold", Arc::clone(&backfill_events))
                    .entry("key", "value", 400),
            )
            .build()
            .expect("cache should build");
        let error = backfill_cache
            .get(&"key".to_owned())
            .await
            .expect_err("backfill failure should propagate");
        assert_backend_failure(error, Operation::Backfill, "hot");
    });
}

#[test]
fn invalid_hit_is_a_named_get_failure_without_backfill() {
    block_on(async {
        let events = events();
        let cache = Cache::builder()
            .backend(
                "invalid",
                RecordingBackend::new("invalid", Arc::clone(&events)).entry("key", "value", 0),
            )
            .backend("later", RecordingBackend::new("later", Arc::clone(&events)))
            .build()
            .expect("cache should build");

        let error = cache
            .lookup(&"key".to_owned())
            .await
            .expect_err("invalid hit must fail");
        let failure = assert_backend_failure(error, Operation::Get, "invalid");
        assert!(
            failure
                .source
                .to_string()
                .contains("invalid remaining TTL 0")
        );
        assert_eq!(
            take_events(&events),
            [Event::Get("invalid", "key".to_owned())]
        );
    });
}

#[test]
fn get_or_load_has_strict_failure_and_zero_ttl_semantics() {
    block_on(async {
        let events = events();
        let hot = RecordingBackend::new("hot", Arc::clone(&events));
        let cold = RecordingBackend::new("cold", Arc::clone(&events));
        let hot_handle = hot.clone();
        let cold_handle = cold.clone();
        let cache = Cache::builder()
            .backend("hot", hot)
            .backend("cold", cold)
            .build()
            .expect("cache should build");
        let calls = AtomicUsize::new(0);

        let value = cache
            .get_or_load(
                &"key".to_owned(),
                || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, TestError>("loaded".to_owned())
                },
                0,
            )
            .await
            .expect("zero TTL load should return value");
        assert_eq!(value.as_str(), "loaded");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            hot_handle
                .get(&"key".to_owned())
                .await
                .expect("hot read")
                .is_none()
        );
        assert!(
            cold_handle
                .get(&"key".to_owned())
                .await
                .expect("cold read")
                .is_none()
        );
    });
}

#[test]
fn get_or_load_distinguishes_loader_and_write_failures() {
    block_on(async {
        let loader_events = events();
        let loader_cache = Cache::builder()
            .backend("only", RecordingBackend::new("only", loader_events))
            .build()
            .expect("cache should build");
        let error = loader_cache
            .get_or_load(
                &"key".to_owned(),
                || async { Err::<String, _>(TestError("loader failed")) },
                -1,
            )
            .await
            .expect_err("loader failure should propagate");
        assert!(matches!(error, KapeError::Loader { .. }));

        let write_events = events();
        let write_cache = Cache::builder()
            .backend(
                "broken",
                RecordingBackend::new("broken", write_events).failing_set(),
            )
            .build()
            .expect("cache should build");
        let error = write_cache
            .get_or_load(
                &"key".to_owned(),
                || async { Ok::<_, TestError>("loaded".to_owned()) },
                -1,
            )
            .await
            .expect_err("write failure should propagate");
        assert_backend_failure(error, Operation::Set, "broken");
    });
}

#[test]
fn wrap_derives_ttl_from_the_loaded_value_only_on_miss() {
    block_on(async {
        let events = events();
        let cache = Cache::builder()
            .backend("only", RecordingBackend::new("only", Arc::clone(&events)))
            .build()
            .expect("cache should build");
        let loader_calls = AtomicUsize::new(0);
        let ttl_calls = AtomicUsize::new(0);
        let key = "profile".to_owned();

        let loaded = cache
            .wrap(
                &key,
                || async {
                    loader_calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, TestError>("premium".to_owned())
                },
                |value| {
                    ttl_calls.fetch_add(1, Ordering::SeqCst);
                    if value == "premium" { 300_000 } else { 60_000 }
                },
            )
            .await
            .expect("wrap should load and write");
        assert_eq!(loaded.as_str(), "premium");
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
        assert_eq!(ttl_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            take_events(&events),
            [
                Event::Get("only", key.clone()),
                Event::Set("only", key.clone(), 300_000),
            ]
        );

        let hit = cache
            .wrap(
                &key,
                || async {
                    panic!("loader must not run on hit");
                    #[allow(unreachable_code)]
                    Ok::<String, TestError>(String::new())
                },
                |_| panic!("TTL selector must not run on hit"),
            )
            .await
            .expect("wrap hit should succeed");
        assert_eq!(hit.as_str(), "premium");
        assert_eq!(
            take_events(&events),
            [Event::Get("only", "profile".to_owned())]
        );
    });
}

#[test]
fn wrap_rejects_an_invalid_derived_ttl_before_writing() {
    block_on(async {
        let events = events();
        let cache = Cache::builder()
            .backend("hot", RecordingBackend::new("hot", Arc::clone(&events)))
            .backend("cold", RecordingBackend::new("cold", Arc::clone(&events)))
            .build()
            .expect("cache should build");

        let error = cache
            .wrap(
                &"key".to_owned(),
                || async { Ok::<_, TestError>("loaded".to_owned()) },
                |value| {
                    assert_eq!(value, "loaded");
                    -2
                },
            )
            .await
            .expect_err("invalid derived TTL must fail");

        assert!(matches!(error, KapeError::InvalidTtl(-2)));
        assert_eq!(
            take_events(&events),
            [
                Event::Get("hot", "key".to_owned()),
                Event::Get("cold", "key".to_owned()),
            ]
        );
    });
}

#[test]
fn wrap_zero_ttl_invalidates_the_chain_and_preserves_failure_names() {
    block_on(async {
        let zero_events = events();
        let zero_cache = Cache::builder()
            .backend(
                "hot",
                RecordingBackend::new("hot", Arc::clone(&zero_events)),
            )
            .backend(
                "cold",
                RecordingBackend::new("cold", Arc::clone(&zero_events)),
            )
            .build()
            .expect("cache should build");
        let loaded = zero_cache
            .wrap(
                &"key".to_owned(),
                || async { Ok::<_, TestError>("loaded".to_owned()) },
                |_| 0,
            )
            .await
            .expect("zero TTL wrap should invalidate and return the value");
        assert_eq!(loaded.as_str(), "loaded");
        assert_eq!(
            take_events(&zero_events),
            [
                Event::Get("hot", "key".to_owned()),
                Event::Get("cold", "key".to_owned()),
                Event::Set("cold", "key".to_owned(), 0),
                Event::Set("hot", "key".to_owned(), 0),
            ]
        );

        let loader_events = events();
        let loader_cache = Cache::builder()
            .backend("only", RecordingBackend::new("only", loader_events))
            .build()
            .expect("cache should build");
        let ttl_calls = AtomicUsize::new(0);
        let error = loader_cache
            .wrap(
                &"key".to_owned(),
                || async { Err::<String, _>(TestError("loader failed")) },
                |_| {
                    ttl_calls.fetch_add(1, Ordering::SeqCst);
                    -1
                },
            )
            .await
            .expect_err("loader failure should propagate");
        assert!(matches!(error, KapeError::Loader { .. }));
        assert_eq!(ttl_calls.load(Ordering::SeqCst), 0);

        let write_events = events();
        let write_cache = Cache::builder()
            .backend(
                "broken",
                RecordingBackend::new("broken", write_events).failing_set(),
            )
            .build()
            .expect("cache should build");
        let error = write_cache
            .wrap(
                &"key".to_owned(),
                || async { Ok::<_, TestError>("loaded".to_owned()) },
                |_| -1,
            )
            .await
            .expect_err("write failure should propagate");
        assert_backend_failure(error, Operation::Set, "broken");
    });
}

#[test]
fn batch_preserves_positions_duplicates_and_backfills_each_hit() {
    block_on(async {
        let events = events();
        let hot = RecordingBackend::new("hot", Arc::clone(&events));
        let cold = RecordingBackend::new("cold", Arc::clone(&events))
            .entry("a", "A", 50_000)
            .entry("b", "B", -1);
        let cache = Cache::builder()
            .backend("hot", hot)
            .backend("cold", cold)
            .build()
            .expect("cache should build");
        let keys = [
            "a".to_owned(),
            "missing".to_owned(),
            "a".to_owned(),
            "b".to_owned(),
        ];

        let values = cache
            .get_many(&keys)
            .await
            .expect("batch lookup should succeed");
        assert_eq!(
            values
                .iter()
                .map(|value| value.as_deref().map(String::as_str))
                .collect::<Vec<_>>(),
            [Some("A"), None, Some("A"), Some("B")]
        );
        let events = take_events(&events);
        assert_eq!(
            &events[..2],
            &[
                Event::GetMany("hot", keys.to_vec()),
                Event::GetMany("cold", keys.to_vec()),
            ]
        );
        assert!(matches!(
            &events[2..],
            [
                Event::Set("hot", first_key, first_ttl),
                Event::Set("hot", second_key, second_ttl),
                Event::Set("hot", forever_key, -1),
            ] if first_key == "a"
                && second_key == "a"
                && forever_key == "b"
                && *first_ttl > 0
                && *first_ttl < 50_000
                && *second_ttl > 0
                && *second_ttl <= *first_ttl
        ));
    });
}

#[test]
fn batch_reads_only_unresolved_positions_and_preserves_hit_sources() {
    block_on(async {
        let events = events();
        let hot = RecordingBackend::new("hot", Arc::clone(&events)).entry("a", "hot-a", -1);
        let cold = RecordingBackend::new("cold", Arc::clone(&events)).entry("b", "cold-b", -1);
        let cache = Cache::builder()
            .backend("hot", hot)
            .backend("cold", cold)
            .build()
            .expect("cache should build");
        let keys = ["a".to_owned(), "b".to_owned(), "a".to_owned()];

        let hits = cache
            .lookup_many(&keys)
            .await
            .expect("batch lookup should succeed");

        assert!(matches!(
            hits.as_slice(),
            [Some(first), Some(second), Some(third)]
                if first.backend.as_ref() == "hot"
                    && first.entry.value.as_str() == "hot-a"
                    && second.backend.as_ref() == "cold"
                    && second.entry.value.as_str() == "cold-b"
                    && third.backend.as_ref() == "hot"
                    && third.entry.value.as_str() == "hot-a"
        ));
        assert_eq!(
            take_events(&events),
            [
                Event::GetMany("hot", keys.to_vec()),
                Event::GetMany("cold", vec!["b".to_owned()]),
                Event::Set("hot", "b".to_owned(), -1),
            ]
        );
    });
}

#[test]
fn batch_read_failure_does_not_backfill_already_collected_hits() {
    block_on(async {
        let events = events();
        let cache = Cache::builder()
            .backend(
                "hot",
                RecordingBackend::new("hot", Arc::clone(&events)).entry("a", "A", -1),
            )
            .backend(
                "cold",
                RecordingBackend::new("cold", Arc::clone(&events)).failing_get(),
            )
            .build()
            .expect("cache should build");

        let error = cache
            .lookup_many(&["a".to_owned(), "b".to_owned()])
            .await
            .expect_err("later batch read should fail");

        assert_backend_failure(error, Operation::Get, "cold");
        assert_eq!(
            take_events(&events),
            [
                Event::GetMany("hot", vec!["a".to_owned(), "b".to_owned()]),
                Event::GetMany("cold", vec!["b".to_owned()]),
            ]
        );
    });
}

#[test]
fn batch_invalid_hit_stops_reads_without_backfilling_collected_hits() {
    block_on(async {
        let events = events();
        let cache = Cache::builder()
            .backend(
                "hot",
                RecordingBackend::new("hot", Arc::clone(&events)).entry("a", "A", -1),
            )
            .backend(
                "cold",
                RecordingBackend::new("cold", Arc::clone(&events)).entry("b", "B", 0),
            )
            .backend("deep", RecordingBackend::new("deep", Arc::clone(&events)))
            .build()
            .expect("cache should build");

        let error = cache
            .lookup_many(&["a".to_owned(), "b".to_owned()])
            .await
            .expect_err("invalid hit must fail the batch");

        assert_backend_failure(error, Operation::Get, "cold");
        assert_eq!(
            take_events(&events),
            [
                Event::GetMany("hot", vec!["a".to_owned(), "b".to_owned()]),
                Event::GetMany("cold", vec!["b".to_owned()]),
            ]
        );
    });
}

#[test]
fn batch_skips_backfill_when_later_reads_exhaust_a_hit_ttl() {
    block_on(async {
        let events = events();
        let cache = Cache::builder()
            .backend("hot", RecordingBackend::new("hot", Arc::clone(&events)))
            .backend(
                "warm",
                RecordingBackend::new("warm", Arc::clone(&events)).entry("a", "A", 50),
            )
            .backend(
                "cold",
                RecordingBackend::new("cold", Arc::clone(&events))
                    .entry("b", "B", -1)
                    .delaying_get_many_return(Duration::from_millis(75)),
            )
            .build()
            .expect("cache should build");

        let hits = cache
            .lookup_many(&["a".to_owned(), "b".to_owned()])
            .await
            .expect("batch lookup should succeed");

        assert!(matches!(
            hits.as_slice(),
            [Some(first), Some(second)]
                if first.backend.as_ref() == "warm"
                    && first.entry.remaining_ttl == 50
                    && second.backend.as_ref() == "cold"
        ));
        assert_eq!(
            take_events(&events),
            [
                Event::GetMany("hot", vec!["a".to_owned(), "b".to_owned()]),
                Event::GetMany("warm", vec!["a".to_owned(), "b".to_owned()]),
                Event::GetMany("cold", vec!["b".to_owned()]),
                Event::Set("warm", "b".to_owned(), -1),
                Event::Set("hot", "b".to_owned(), -1),
            ]
        );
    });
}

#[test]
fn batch_skips_backfill_when_earlier_position_backfill_exhausts_ttl() {
    block_on(async {
        let events = events();
        let cache = Cache::builder()
            .backend(
                "hot",
                RecordingBackend::new("hot", Arc::clone(&events))
                    .delaying_set_return(Duration::from_millis(75)),
            )
            .backend(
                "cold",
                RecordingBackend::new("cold", Arc::clone(&events))
                    .entry("a", "A", -1)
                    .entry("b", "B", 50),
            )
            .build()
            .expect("cache should build");

        let hits = cache
            .lookup_many(&["a".to_owned(), "b".to_owned()])
            .await
            .expect("batch lookup should succeed");

        assert!(matches!(
            hits.as_slice(),
            [Some(first), Some(second)]
                if first.backend.as_ref() == "cold"
                    && first.entry.remaining_ttl == -1
                    && second.backend.as_ref() == "cold"
                    && second.entry.remaining_ttl == 50
        ));
        assert_eq!(
            take_events(&events),
            [
                Event::GetMany("hot", vec!["a".to_owned(), "b".to_owned()]),
                Event::GetMany("cold", vec!["a".to_owned(), "b".to_owned()]),
                Event::Set("hot", "a".to_owned(), -1),
            ]
        );
    });
}

#[test]
fn batch_backfill_failure_preserves_completed_earlier_positions() {
    block_on(async {
        let events = events();
        let cache = Cache::builder()
            .backend(
                "hot",
                RecordingBackend::new("hot", Arc::clone(&events)).failing_set_for("b"),
            )
            .backend(
                "cold",
                RecordingBackend::new("cold", Arc::clone(&events))
                    .entry("a", "A", -1)
                    .entry("b", "B", -1),
            )
            .build()
            .expect("cache should build");

        let error = cache
            .lookup_many(&["a".to_owned(), "b".to_owned()])
            .await
            .expect_err("later backfill should fail the batch");

        assert_backend_failure(error, Operation::Backfill, "hot");
        assert_eq!(
            take_events(&events),
            [
                Event::GetMany("hot", vec!["a".to_owned(), "b".to_owned()]),
                Event::GetMany("cold", vec!["a".to_owned(), "b".to_owned()]),
                Event::Set("hot", "a".to_owned(), -1),
                Event::Set("hot", "b".to_owned(), -1),
            ]
        );
    });
}

#[test]
fn batch_mutations_are_reverse_and_preserve_item_order() {
    block_on(async {
        let events = events();
        let cache = Cache::builder()
            .backend("hot", RecordingBackend::new("hot", Arc::clone(&events)))
            .backend("cold", RecordingBackend::new("cold", Arc::clone(&events)))
            .build()
            .expect("cache should build");

        cache
            .set_many(&[
                SetItem::new("a".to_owned(), "A".to_owned(), -1),
                SetItem::new("b".to_owned(), "B".to_owned(), 500),
            ])
            .await
            .expect("batch set should succeed");
        cache
            .remove_many(&["a".to_owned(), "a".to_owned()])
            .await
            .expect("batch remove should succeed");
        assert_eq!(
            take_events(&events),
            [
                Event::SetMany("cold", vec![("a".to_owned(), -1), ("b".to_owned(), 500)]),
                Event::SetMany("hot", vec![("a".to_owned(), -1), ("b".to_owned(), 500)]),
                Event::RemoveMany("cold", vec!["a".to_owned(), "a".to_owned()]),
                Event::RemoveMany("hot", vec!["a".to_owned(), "a".to_owned()]),
            ]
        );
    });
}

#[test]
fn duplicate_batch_write_is_rejected_before_mutation() {
    block_on(async {
        let events = events();
        let cache = Cache::builder()
            .backend("only", RecordingBackend::new("only", Arc::clone(&events)))
            .build()
            .expect("cache should build");
        let tuple_error = cache
            .set_many([
                ("a".to_owned(), "A".to_owned(), -1),
                ("a".to_owned(), "A2".to_owned(), 500),
            ])
            .await
            .expect_err("duplicate batch key must fail");
        let items = [
            SetItem::new("a".to_owned(), "A".to_owned(), -1),
            SetItem::new("a".to_owned(), "A2".to_owned(), 500),
        ];
        let set_item_error = cache
            .set_many(&items)
            .await
            .expect_err("duplicate batch key must fail");

        for error in [tuple_error, set_item_error] {
            assert!(matches!(
                error,
                KapeError::DuplicateBatchKey {
                    first_index: 0,
                    duplicate_index: 1,
                }
            ));
        }
        assert!(take_events(&events).is_empty());
    });
}

#[test]
fn batch_contract_failures_return_err_without_partial_results() {
    block_on(async {
        let length_events = events();
        let length_cache = Cache::builder()
            .backend(
                "broken",
                RecordingBackend::new("broken", length_events).wrong_batch_len(),
            )
            .build()
            .expect("cache should build");
        let error = length_cache
            .lookup_many(&["a".to_owned(), "b".to_owned()])
            .await
            .expect_err("wrong batch length must fail");
        assert_backend_failure(error, Operation::Get, "broken");

        let backfill_events = events();
        let backfill_cache = Cache::builder()
            .backend(
                "hot",
                RecordingBackend::new("hot", Arc::clone(&backfill_events)).failing_set(),
            )
            .backend(
                "cold",
                RecordingBackend::new("cold", Arc::clone(&backfill_events))
                    .entry("a", "A", 10_000)
                    .entry("b", "B", 20_000),
            )
            .build()
            .expect("cache should build");
        let error = backfill_cache
            .lookup_many(&["a".to_owned(), "b".to_owned()])
            .await
            .expect_err("one backfill failure must fail the batch");
        assert_backend_failure(error, Operation::Backfill, "hot");
        let backfill_events = take_events(&backfill_events);
        assert_eq!(
            &backfill_events[..2],
            &[
                Event::GetMany("hot", vec!["a".to_owned(), "b".to_owned()]),
                Event::GetMany("cold", vec!["a".to_owned(), "b".to_owned()]),
            ]
        );
        assert!(matches!(
            &backfill_events[2..],
            [Event::Set("hot", key, ttl)] if key == "a" && *ttl > 0 && *ttl < 10_000
        ));
    });
}

#[test]
fn empty_batches_do_not_call_backends() {
    block_on(async {
        let events = events();
        let cache = Cache::builder()
            .backend("only", RecordingBackend::new("only", Arc::clone(&events)))
            .build()
            .expect("cache should build");

        assert!(
            cache
                .lookup_many(&[])
                .await
                .expect("empty lookup")
                .is_empty()
        );
        assert!(cache.get_many(&[]).await.expect("empty get").is_empty());
        cache
            .set_many(std::iter::empty::<SetItem<String, String>>())
            .await
            .expect("empty set");
        cache.remove_many(&[]).await.expect("empty remove");
        assert!(take_events(&events).is_empty());
    });
}

fn events() -> Arc<Mutex<Vec<Event>>> {
    Arc::new(Mutex::new(Vec::new()))
}

fn take_events(events: &Arc<Mutex<Vec<Event>>>) -> Vec<Event> {
    std::mem::take(&mut *events.lock().expect("events mutex poisoned"))
}

fn assert_backend_failure(error: KapeError, operation: Operation, backend: &str) -> BackendFailure {
    let KapeError::Backend(failure) = error else {
        panic!("expected named backend failure")
    };
    assert_eq!(failure.operation, operation);
    assert_eq!(failure.backend.as_ref(), backend);
    failure
}
