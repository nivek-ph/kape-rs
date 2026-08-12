use std::{
    error::Error as StdError,
    fmt,
    future::{Future, pending, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use async_trait::async_trait;
use futures_lite::future::{block_on, yield_now, zip};
use kape::{
    BackendCapability, BackendOptions, BackendTTLPolicy, BackfillFailurePolicy, BuildError, Cache,
    CacheBackend, CacheEntry, CacheLookup, Error, IterationEntry, IterationFreshness,
    IterationPage, LoadOptions, LoadWriteFailurePolicy, LoaderFailurePolicy, Lookup, Operation,
    ReadFailurePolicy, RemainingTTL, ResolvedTTL, SetItem, TTL,
};

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Get(&'static str),
    Set(&'static str, ResolvedTTL),
    Remove(&'static str),
    Clear(&'static str),
    Iterate(&'static str),
    Disconnect(&'static str),
}

#[derive(Clone)]
struct TestBackend {
    name: &'static str,
    lookup: Lookup<String>,
    fail_get: Arc<AtomicBool>,
    fail_set: Arc<AtomicBool>,
    fail_remove: Arc<AtomicBool>,
    events: Arc<Mutex<Vec<Event>>>,
}

impl TestBackend {
    /// Creates a new test backend.
    fn new(name: &'static str, lookup: Lookup<String>, events: Arc<Mutex<Vec<Event>>>) -> Self {
        Self {
            name,
            lookup,
            fail_get: Arc::new(AtomicBool::new(false)),
            fail_set: Arc::new(AtomicBool::new(false)),
            fail_remove: Arc::new(AtomicBool::new(false)),
            events,
        }
    }

    /// Configures the backend to fail get operations.
    fn failing_get(self) -> Self {
        self.fail_get.store(true, Ordering::Relaxed);
        self
    }

    fn failing_set(self) -> Self {
        self.fail_set.store(true, Ordering::Relaxed);
        self
    }

    fn failing_remove(self) -> Self {
        self.fail_remove.store(true, Ordering::Relaxed);
        self
    }

    fn record(&self, event: Event) {
        lock(&self.events).push(event);
    }
}

#[derive(Debug)]
struct TestError(&'static str);

impl fmt::Display for TestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl StdError for TestError {}

struct BorrowedBackend;

#[async_trait]
impl<'data> CacheBackend<&'data str, &'data str> for BorrowedBackend {
    type Error = TestError;

    async fn get(&self, _key: &&'data str) -> Result<Lookup<&'data str>, Self::Error> {
        Ok(Lookup::Miss)
    }

    async fn set(
        &self,
        _key: &&'data str,
        _value: Arc<&'data str>,
        _ttl: ResolvedTTL,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn remove(&self, _key: &&'data str) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[test]
fn cache_accepts_non_static_key_and_value_types() {
    fn build<'data>(_key: &'data str, _value: &'data str) {
        let cache: Cache<&'data str, &'data str> = Cache::builder()
            .backend("borrowed", BorrowedBackend)
            .build()
            .unwrap();
        drop(cache);
    }

    let key = String::from("key");
    let value = String::from("value");
    build(&key, &value);
}

#[async_trait]
impl CacheBackend<String, String> for TestBackend {
    type Error = TestError;

    async fn get(&self, _key: &String) -> Result<Lookup<String>, Self::Error> {
        self.record(Event::Get(self.name));
        if self.fail_get.load(Ordering::Relaxed) {
            Err(TestError("get failed"))
        } else {
            Ok(self.lookup.clone())
        }
    }

    async fn set(
        &self,
        _key: &String,
        _value: Arc<String>,
        ttl: ResolvedTTL,
    ) -> Result<(), Self::Error> {
        self.record(Event::Set(self.name, ttl));
        if self.fail_set.load(Ordering::Relaxed) {
            Err(TestError("set failed"))
        } else {
            Ok(())
        }
    }

    async fn remove(&self, _key: &String) -> Result<(), Self::Error> {
        self.record(Event::Remove(self.name));
        if self.fail_remove.load(Ordering::Relaxed) {
            Err(TestError("remove failed"))
        } else {
            Ok(())
        }
    }

    async fn clear(&self) -> Result<BackendCapability<()>, Self::Error> {
        self.record(Event::Clear(self.name));
        if self.fail_remove.load(Ordering::Relaxed) {
            Err(TestError("clear failed"))
        } else {
            Ok(BackendCapability::Supported(()))
        }
    }

    async fn iterate(
        &self,
        _cursor: Option<&[u8]>,
        _limit: usize,
    ) -> Result<BackendCapability<IterationPage<String, String>>, Self::Error> {
        self.record(Event::Iterate(self.name));
        let entries = match &self.lookup {
            Lookup::Miss => Vec::new(),
            Lookup::Hit(entry) | Lookup::Stale(entry) => vec![IterationEntry {
                key: "iterated".to_owned(),
                value: Arc::clone(&entry.value),
                remaining_ttl: entry.remaining_ttl,
                freshness: if matches!(self.lookup, Lookup::Hit(_)) {
                    IterationFreshness::Fresh
                } else {
                    IterationFreshness::Stale
                },
            }],
        };
        Ok(BackendCapability::Supported(IterationPage {
            entries,
            next_cursor: None,
        }))
    }

    async fn disconnect(&self) -> Result<(), Self::Error> {
        self.record(Event::Disconnect(self.name));
        if self.fail_remove.load(Ordering::Relaxed) {
            Err(TestError("disconnect failed"))
        } else {
            Ok(())
        }
    }
}

struct ShortBatchBackend;

#[async_trait]
impl CacheBackend<String, String> for ShortBatchBackend {
    type Error = TestError;

    async fn get(&self, _key: &String) -> Result<Lookup<String>, Self::Error> {
        Ok(Lookup::Miss)
    }

    async fn set(
        &self,
        _key: &String,
        _value: Arc<String>,
        _ttl: ResolvedTTL,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn remove(&self, _key: &String) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn get_many(&self, _keys: &[&String]) -> Result<Vec<Lookup<String>>, Self::Error> {
        Ok(vec![Lookup::Miss])
    }
}

#[test]
fn preserves_read_order_and_caps_remaining_ttl_during_backfill() {
    let events = events();
    let value = Arc::new("value".to_owned());
    let first_options = BackendOptions::new()
        .ttl(BackendTTLPolicy::new().backfill_ttl_cap(Duration::from_secs(10)));
    let cache = Cache::builder()
        .backend_with(
            "first",
            TestBackend::new("first", Lookup::Miss, Arc::clone(&events)),
            first_options,
        )
        .backend(
            "second",
            TestBackend::new(
                "second",
                Lookup::Hit(CacheEntry::new(
                    Arc::clone(&value),
                    RemainingTTL::Known(Duration::from_secs(30)),
                )),
                Arc::clone(&events),
            ),
        )
        .backend(
            "third",
            TestBackend::new("third", Lookup::Miss, Arc::clone(&events)),
        )
        .build()
        .expect("cache should build");

    let lookup = block_on(cache.lookup(&"key".to_owned())).expect("read should succeed");

    match lookup {
        CacheLookup::Hit {
            value,
            backend,
            remaining_ttl,
            backfill_failures,
            read_failures,
        } => {
            assert_eq!(value.as_str(), "value");
            assert_eq!(backend.as_ref(), "second");
            assert_eq!(remaining_ttl, RemainingTTL::Known(Duration::from_secs(30)));
            assert!(backfill_failures.is_empty());
            assert!(read_failures.is_empty());
        }
        other => panic!("unexpected lookup: {other:?}"),
    }
    assert_eq!(
        take_events(&events),
        [
            Event::Get("first"),
            Event::Get("second"),
            Event::Set("first", ResolvedTTL::After(Duration::from_secs(10))),
        ]
    );
}

#[test]
fn unknown_remaining_ttl_is_not_backfilled() {
    let events = events();
    let cache = Cache::builder()
        .backend(
            "first",
            TestBackend::new("first", Lookup::Miss, Arc::clone(&events)),
        )
        .backend(
            "second",
            TestBackend::new(
                "second",
                Lookup::Hit(CacheEntry::new(
                    Arc::new("value".to_owned()),
                    RemainingTTL::Unknown,
                )),
                Arc::clone(&events),
            ),
        )
        .build()
        .expect("cache should build");

    let value = block_on(cache.get(&"key".to_owned())).expect("read should succeed");

    assert_eq!(value.as_deref().map(String::as_str), Some("value"));
    assert_eq!(
        take_events(&events),
        [Event::Get("first"), Event::Get("second")]
    );
}

#[test]
fn reports_backfill_failure_with_fresh_hit_by_default() {
    let events = events();
    let cache = Cache::builder()
        .backend(
            "first",
            TestBackend::new("first", Lookup::Miss, Arc::clone(&events)).failing_set(),
        )
        .backend(
            "second",
            TestBackend::new(
                "second",
                hit("value", RemainingTTL::Never),
                Arc::clone(&events),
            ),
        )
        .build()
        .expect("cache should build");

    let lookup = block_on(cache.lookup(&"key".to_owned())).expect("hit should be returned");
    let CacheLookup::Hit {
        backfill_failures, ..
    } = lookup
    else {
        panic!("expected hit");
    };
    assert_eq!(backfill_failures.len(), 1);
    assert_eq!(backfill_failures[0].backend.as_ref(), "first");
    assert_eq!(backfill_failures[0].operation, Operation::Backfill);
}

#[test]
fn can_propagate_backfill_failure() {
    let events = events();
    let options = BackendOptions::new().backfill_failure(BackfillFailurePolicy::Propagate);
    let cache = Cache::builder()
        .backend_with(
            "first",
            TestBackend::new("first", Lookup::Miss, Arc::clone(&events)).failing_set(),
            options,
        )
        .backend(
            "second",
            TestBackend::new(
                "second",
                hit("value", RemainingTTL::Never),
                Arc::clone(&events),
            ),
        )
        .build()
        .expect("cache should build");

    let error = block_on(cache.lookup(&"key".to_owned())).expect_err("backfill should fail");
    assert!(matches!(
        error,
        Error::Backend(ref failure)
            if failure.backend.as_ref() == "first"
                && failure.operation == Operation::Backfill
    ));
}

#[test]
fn resolves_default_and_max_ttl_per_backend() {
    let events = events();
    let first = BackendOptions::new().ttl(
        BackendTTLPolicy::new()
            .default_ttl(Duration::from_secs(30))
            .max_ttl(Duration::from_secs(20)),
    );
    let second =
        BackendOptions::new().ttl(BackendTTLPolicy::new().max_ttl(Duration::from_secs(10)));
    let cache = Cache::builder()
        .backend_with(
            "first",
            TestBackend::new("first", Lookup::Miss, Arc::clone(&events)),
            first,
        )
        .backend_with(
            "second",
            TestBackend::new("second", Lookup::Miss, Arc::clone(&events)),
            second,
        )
        .build()
        .expect("cache should build");

    block_on(cache.set(
        &"key".to_owned(),
        Arc::new("value".to_owned()),
        TTL::Default,
    ))
    .expect("write should succeed");

    assert_eq!(
        take_events(&events),
        [
            Event::Set("first", ResolvedTTL::After(Duration::from_secs(20))),
            Event::Set("second", ResolvedTTL::After(Duration::from_secs(10))),
        ]
    );
}

#[test]
fn selects_dynamic_ttl_per_write_backend_before_applying_caps() {
    let events = events();
    let selected = Arc::new(Mutex::new(Vec::new()));
    let first = BackendOptions::new().ttl(
        BackendTTLPolicy::new()
            .default_ttl(Duration::from_secs(30))
            .max_ttl(Duration::from_secs(15)),
    );
    let disabled = BackendOptions::new().write(false);
    let third =
        BackendOptions::new().ttl(BackendTTLPolicy::new().default_ttl(Duration::from_secs(7)));
    let cache = Cache::builder()
        .backend_with(
            "first",
            TestBackend::new("first", Lookup::Miss, Arc::clone(&events)),
            first,
        )
        .backend_with(
            "disabled",
            TestBackend::new("disabled", Lookup::Miss, Arc::clone(&events)),
            disabled,
        )
        .backend_with(
            "third",
            TestBackend::new("third", Lookup::Miss, Arc::clone(&events)),
            third,
        )
        .build()
        .expect("cache should build");
    let key = "key".to_owned();
    let value = Arc::new("value".to_owned());
    let selected_for_ttl = Arc::clone(&selected);

    block_on(
        cache.set_with_ttl(&key, value, TTL::Default, move |context| {
            assert_eq!(context.key, "key");
            assert_eq!(context.value, "value");
            lock(&selected_for_ttl).push((context.backend.to_owned(), context.backend_index));
            (context.backend == "first").then_some(TTL::After(Duration::from_mins(1)))
        }),
    )
    .expect("dynamic write should succeed");

    assert_eq!(
        take_events(&events),
        [
            Event::Set("first", ResolvedTTL::After(Duration::from_secs(15))),
            Event::Set("third", ResolvedTTL::After(Duration::from_secs(7))),
        ]
    );
    assert_eq!(
        *lock(&selected),
        [("first".to_owned(), 0), ("third".to_owned(), 2)]
    );
}

#[test]
fn loader_can_select_dynamic_ttl_from_the_loaded_value() {
    let events = events();
    let cache = Cache::builder()
        .backend(
            "memory",
            TestBackend::new("memory", Lookup::Miss, Arc::clone(&events)),
        )
        .build()
        .expect("cache should build");

    let value = block_on(cache.get_or_load_with_ttl(
        &"key".to_owned(),
        LoadOptions::new().ttl(TTL::After(Duration::from_secs(30))),
        |context| {
            assert_eq!(context.backend, "memory");
            assert_eq!(context.value, "loaded");
            Some(TTL::After(Duration::from_secs(9)))
        },
        || async { Ok::<_, TestError>("loaded".to_owned()) },
    ))
    .expect("value should load");

    assert_eq!(value.as_str(), "loaded");
    assert_eq!(
        take_events(&events),
        [
            Event::Get("memory"),
            Event::Get("memory"),
            Event::Set("memory", ResolvedTTL::After(Duration::from_secs(9))),
        ]
    );
}

#[test]
fn batch_get_preserves_duplicates_and_backfills_each_input_position() {
    let events = events();
    let cache = Cache::builder()
        .backend(
            "first",
            TestBackend::new("first", Lookup::Miss, Arc::clone(&events)),
        )
        .backend(
            "second",
            TestBackend::new(
                "second",
                hit("value", RemainingTTL::Known(Duration::from_secs(20))),
                Arc::clone(&events),
            ),
        )
        .build()
        .expect("cache should build");
    let keys = ["same".to_owned(), "same".to_owned()];

    let values = block_on(cache.get_many(&keys)).expect("batch read should succeed");

    assert_eq!(values.len(), keys.len());
    assert!(
        values
            .iter()
            .all(|value| value.as_ref().map(|value| value.as_str()) == Some("value"))
    );
    assert_eq!(
        take_events(&events),
        [
            Event::Get("first"),
            Event::Get("first"),
            Event::Get("second"),
            Event::Get("second"),
            Event::Set("first", ResolvedTTL::After(Duration::from_secs(20))),
            Event::Set("first", ResolvedTTL::After(Duration::from_secs(20))),
        ]
    );
}

#[test]
fn batch_set_resolves_each_item_and_backend_before_ordered_fanout() {
    let events = events();
    let first = BackendOptions::new().ttl(BackendTTLPolicy::new().max_ttl(Duration::from_secs(10)));
    let cache = Cache::builder()
        .backend_with(
            "first",
            TestBackend::new("first", Lookup::Miss, Arc::clone(&events)),
            first,
        )
        .backend(
            "second",
            TestBackend::new("second", Lookup::Miss, Arc::clone(&events)),
        )
        .build()
        .expect("cache should build");
    let items = [
        SetItem::new("a".to_owned(), "one".to_owned(), TTL::Never),
        SetItem::new(
            "b".to_owned(),
            "two".to_owned(),
            TTL::After(Duration::from_secs(20)),
        ),
    ];

    block_on(cache.set_many_with_ttl(&items, |item_index, context| {
        (item_index == 0 && context.backend == "second")
            .then_some(TTL::After(Duration::from_secs(3)))
    }))
    .expect("batch write should succeed");

    assert_eq!(
        take_events(&events),
        [
            Event::Set("first", ResolvedTTL::After(Duration::from_secs(10))),
            Event::Set("first", ResolvedTTL::After(Duration::from_secs(10))),
            Event::Set("second", ResolvedTTL::After(Duration::from_secs(3))),
            Event::Set("second", ResolvedTTL::After(Duration::from_secs(20))),
        ]
    );
}

#[test]
fn batch_has_and_take_do_not_backfill_and_take_removes_in_reverse_order() {
    let events = events();
    let cache = Cache::builder()
        .backend(
            "first",
            TestBackend::new("first", Lookup::Miss, Arc::clone(&events)),
        )
        .backend(
            "second",
            TestBackend::new(
                "second",
                hit("value", RemainingTTL::Never),
                Arc::clone(&events),
            ),
        )
        .build()
        .expect("cache should build");
    let keys = ["a".to_owned(), "b".to_owned()];

    let present = block_on(cache.has_many(&keys)).expect("batch has should succeed");
    assert_eq!(present, [true, true]);
    assert!(
        !take_events(&events)
            .iter()
            .any(|event| matches!(event, Event::Set(..)))
    );

    let values = block_on(cache.take_many(&keys)).expect("batch take should succeed");
    assert!(
        values
            .iter()
            .all(|value| value.as_ref().map(|value| value.as_str()) == Some("value"))
    );
    assert_eq!(
        take_events(&events),
        [
            Event::Get("first"),
            Event::Get("first"),
            Event::Get("second"),
            Event::Get("second"),
            Event::Remove("second"),
            Event::Remove("second"),
            Event::Remove("first"),
            Event::Remove("first"),
        ]
    );
}

#[test]
fn scalar_has_and_take_match_batch_semantics() {
    let events = events();
    let cache = Cache::builder()
        .backend(
            "first",
            TestBackend::new("first", Lookup::Miss, Arc::clone(&events)),
        )
        .backend(
            "second",
            TestBackend::new(
                "second",
                hit("value", RemainingTTL::Never),
                Arc::clone(&events),
            ),
        )
        .build()
        .expect("cache should build");
    let key = "key".to_owned();

    assert!(block_on(cache.has(&key)).expect("scalar has should succeed"));
    assert_eq!(
        take_events(&events),
        [Event::Get("first"), Event::Get("second")]
    );

    let value = block_on(cache.take(&key)).expect("scalar take should succeed");
    assert_eq!(value.as_deref().map(String::as_str), Some("value"));
    assert_eq!(
        take_events(&events),
        [
            Event::Get("first"),
            Event::Get("second"),
            Event::Remove("second"),
            Event::Remove("first"),
        ]
    );
}

#[test]
fn rejects_backend_batch_results_with_the_wrong_length() {
    let cache = Cache::builder()
        .backend("broken", ShortBatchBackend)
        .build()
        .expect("cache should build");
    let keys = ["a".to_owned(), "b".to_owned()];

    let error = block_on(cache.lookup_many(&keys)).expect_err("batch length must be enforced");
    assert!(matches!(
        error,
        Error::Backend(ref failure)
            if failure.backend.as_ref() == "broken"
                && failure.operation == Operation::Get
                && failure.source.to_string().contains("expected 2")
    ));
}

#[test]
fn set_is_ordered_best_effort_and_aggregates_failures() {
    let events = events();
    let cache = Cache::builder()
        .backend(
            "first",
            TestBackend::new("first", Lookup::Miss, Arc::clone(&events)).failing_set(),
        )
        .backend(
            "second",
            TestBackend::new("second", Lookup::Miss, Arc::clone(&events)),
        )
        .build()
        .expect("cache should build");

    let error = block_on(cache.set(&"key".to_owned(), Arc::new("value".to_owned()), TTL::Never))
        .expect_err("first write should fail");

    assert_eq!(
        take_events(&events),
        [
            Event::Set("first", ResolvedTTL::Never),
            Event::Set("second", ResolvedTTL::Never),
        ]
    );
    match error {
        Error::PartialFailure {
            operation,
            failures,
        } => {
            assert_eq!(operation, Operation::Set);
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].backend.as_ref(), "first");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn remove_runs_in_reverse_order_and_aggregates_failures() {
    let events = events();
    let cache = Cache::builder()
        .backend(
            "first",
            TestBackend::new("first", Lookup::Miss, Arc::clone(&events)),
        )
        .backend(
            "second",
            TestBackend::new("second", Lookup::Miss, Arc::clone(&events)).failing_remove(),
        )
        .backend(
            "third",
            TestBackend::new("third", Lookup::Miss, Arc::clone(&events)),
        )
        .build()
        .expect("cache should build");

    let error = block_on(cache.remove(&"key".to_owned())).expect_err("remove should fail");

    assert_eq!(
        take_events(&events),
        [
            Event::Remove("third"),
            Event::Remove("second"),
            Event::Remove("first"),
        ]
    );
    match error {
        Error::PartialFailure {
            operation,
            failures,
        } => {
            assert_eq!(operation, Operation::Remove);
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].backend.as_ref(), "second");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn clear_and_disconnect_run_in_reverse_order_and_aggregate_failures() {
    let events = events();
    let cache = Cache::builder()
        .backend(
            "first",
            TestBackend::new("first", Lookup::Miss, Arc::clone(&events)),
        )
        .backend(
            "second",
            TestBackend::new("second", Lookup::Miss, Arc::clone(&events)).failing_remove(),
        )
        .backend(
            "third",
            TestBackend::new("third", Lookup::Miss, Arc::clone(&events)),
        )
        .build()
        .expect("cache should build");

    let clear = block_on(cache.clear()).expect_err("clear should aggregate failures");
    assert!(matches!(
        clear,
        Error::PartialFailure { operation: Operation::Clear, ref failures }
            if failures.len() == 1 && failures[0].backend.as_ref() == "second"
    ));
    assert_eq!(
        take_events(&events),
        [
            Event::Clear("third"),
            Event::Clear("second"),
            Event::Clear("first"),
        ]
    );

    let disconnect =
        block_on(cache.disconnect()).expect_err("disconnect should aggregate failures");
    assert!(matches!(
        disconnect,
        Error::PartialFailure { operation: Operation::Disconnect, ref failures }
            if failures.len() == 1 && failures[0].backend.as_ref() == "second"
    ));
    assert_eq!(
        take_events(&events),
        [
            Event::Disconnect("third"),
            Event::Disconnect("second"),
            Event::Disconnect("first"),
        ]
    );
}

#[test]
fn named_iteration_is_typed_and_capability_errors_are_explicit() {
    let events = events();
    let cache = Cache::builder()
        .backend(
            "supported",
            TestBackend::new(
                "supported",
                hit("value", RemainingTTL::Never),
                Arc::clone(&events),
            ),
        )
        .backend("unsupported", ShortBatchBackend)
        .build()
        .expect("cache should build");

    let page = block_on(cache.scan("supported", None, 10)).expect("iteration should succeed");
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].key, "iterated");
    assert_eq!(page.entries[0].value.as_str(), "value");
    assert_eq!(page.entries[0].freshness, IterationFreshness::Fresh);

    assert!(matches!(
        block_on(cache.scan("supported", None, 0)),
        Err(Error::InvalidIterationLimit)
    ));
    assert!(matches!(
        block_on(cache.scan("missing", None, 1)),
        Err(Error::BackendNotFound(name)) if name.as_ref() == "missing"
    ));
    let unsupported = block_on(cache.clear_backend("unsupported"))
        .expect_err("unsupported clear must stay visible");
    assert!(matches!(
        unsupported,
        Error::Backend(ref failure)
            if failure.operation == Operation::Clear
                && failure.backend.as_ref() == "unsupported"
                && failure.source.to_string().contains("does not support")
    ));
}

#[test]
fn read_policies_skip_or_serve_the_earliest_stale_candidate() {
    let skip_events = events();
    let skip = Cache::builder()
        .backend_with(
            "failing",
            TestBackend::new("failing", Lookup::Miss, Arc::clone(&skip_events)).failing_get(),
            BackendOptions::new().read_failure(ReadFailurePolicy::SkipBackend),
        )
        .backend(
            "hit",
            TestBackend::new(
                "hit",
                hit("new", RemainingTTL::Never),
                Arc::clone(&skip_events),
            ),
        )
        .build()
        .expect("cache should build");
    let lookup = block_on(skip.lookup(&"key".to_owned())).expect("skip should continue");
    assert!(matches!(
        lookup,
        CacheLookup::Hit { value, read_failures, .. }
            if value.as_str() == "new"
                && read_failures.len() == 1
                && read_failures[0].backend.as_ref() == "failing"
    ));

    let stale_events = events();
    let stale = Cache::builder()
        .backend(
            "stale",
            TestBackend::new("stale", stale("old"), Arc::clone(&stale_events)),
        )
        .backend_with(
            "failing",
            TestBackend::new("failing", Lookup::Miss, Arc::clone(&stale_events)).failing_get(),
            BackendOptions::new().read_failure(ReadFailurePolicy::ServeStale),
        )
        .build()
        .expect("cache should build");
    let lookup = block_on(stale.lookup(&"key".to_owned())).expect("stale should be served");
    assert!(matches!(
        lookup,
        CacheLookup::Stale { value, backend, cause, .. }
            if value.as_str() == "old"
                && backend.as_ref() == "stale"
                && cause.backend.as_ref() == "failing"
    ));
}

#[test]
fn coalesces_concurrent_loaders_per_cache_and_key() {
    let events = events();
    let cache = Cache::builder()
        .backend(
            "memory",
            TestBackend::new("memory", Lookup::Miss, Arc::clone(&events)),
        )
        .build()
        .expect("cache should build");
    let loads = Arc::new(AtomicUsize::new(0));
    let key = "key".to_owned();

    let first_loads = Arc::clone(&loads);
    let first = cache.get_or_load(&key, move || async move {
        first_loads.fetch_add(1, Ordering::SeqCst);
        yield_now().await;
        Ok::<_, TestError>("loaded".to_owned())
    });
    let second_loads = Arc::clone(&loads);
    let second = cache.get_or_load(&key, move || async move {
        second_loads.fetch_add(1, Ordering::SeqCst);
        Ok::<_, TestError>("wrong".to_owned())
    });

    let (first, second) = block_on(zip(first, second));
    assert_eq!(first.expect("first should load").as_str(), "loaded");
    assert_eq!(second.expect("second should wait").as_str(), "loaded");
    assert_eq!(loads.load(Ordering::SeqCst), 1);
}

#[test]
fn loader_can_serve_stale_and_write_failure_policy_is_explicit() {
    let stale_events = events();
    let stale_cache = Cache::builder()
        .backend(
            "stale",
            TestBackend::new("stale", stale("old"), Arc::clone(&stale_events)),
        )
        .build()
        .expect("cache should build");
    let options = LoadOptions::new().loader_failure(LoaderFailurePolicy::ServeStale);
    let value = block_on(
        stale_cache.get_or_load_with(&"key".to_owned(), options, || async {
            Err::<String, _>(TestError("loader failed"))
        }),
    )
    .expect("stale should be served");
    assert_eq!(value.as_str(), "old");

    let write_events = events();
    let write_cache = Cache::builder()
        .backend(
            "failing",
            TestBackend::new("failing", Lookup::Miss, Arc::clone(&write_events)).failing_set(),
        )
        .build()
        .expect("cache should build");
    let options = LoadOptions::new().write_failure(LoadWriteFailurePolicy::ReturnValue);
    let value = block_on(
        write_cache.get_or_load_with(&"key".to_owned(), options, || async {
            Ok::<_, TestError>("loaded".to_owned())
        }),
    )
    .expect("loaded value should be returned");
    assert_eq!(value.as_str(), "loaded");
}

#[test]
fn cancelling_leader_notifies_waiter_and_dequeues_load() {
    let events = events();
    let cache = Cache::builder()
        .backend(
            "memory",
            TestBackend::new("memory", Lookup::Miss, Arc::clone(&events)),
        )
        .build()
        .expect("cache should build");
    let key = "key".to_owned();
    let mut leader = Box::pin(cache.get_or_load(&key, pending::<Result<String, TestError>>));

    poll_once_pending(leader.as_mut());

    let mut waiter =
        Box::pin(cache.get_or_load(&key, || async { Ok::<_, TestError>("unused".to_owned()) }));
    poll_once_pending(waiter.as_mut());

    drop(leader);
    assert!(matches!(block_on(waiter), Err(Error::LoadCancelled)));

    let retry =
        block_on(cache.get_or_load(&key, || async { Ok::<_, TestError>("retry".to_owned()) }))
            .expect("cancelled load should be dequeued");
    assert_eq!(retry.as_str(), "retry");
}

#[test]
fn loader_panic_dequeues_load_for_retry() {
    let events = events();
    let cache = Cache::builder()
        .backend(
            "memory",
            TestBackend::new("memory", Lookup::Miss, Arc::clone(&events)),
        )
        .build()
        .expect("cache should build");
    let key = "key".to_owned();

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(cache.get_or_load(&key, panicking_loader))
    }));
    assert!(panic.is_err());

    let retry =
        block_on(cache.get_or_load(&key, || async { Ok::<_, TestError>("retry".to_owned()) }))
            .expect("panicked load should be dequeued");
    assert_eq!(retry.as_str(), "retry");
}

#[test]
fn load_queue_does_not_cross_cache_instances() {
    let events = events();
    let backend = TestBackend::new("memory", Lookup::Miss, Arc::clone(&events));
    let first_cache = Cache::builder()
        .backend("memory", backend.clone())
        .build()
        .expect("cache should build");
    let second_cache = Cache::builder()
        .backend("memory", backend)
        .build()
        .expect("cache should build");
    let loads = Arc::new(AtomicUsize::new(0));
    let key = "key".to_owned();

    let first_loads = Arc::clone(&loads);
    let first = first_cache.get_or_load(&key, move || async move {
        first_loads.fetch_add(1, Ordering::SeqCst);
        yield_now().await;
        Ok::<_, TestError>("first".to_owned())
    });
    let second_loads = Arc::clone(&loads);
    let second = second_cache.get_or_load(&key, move || async move {
        second_loads.fetch_add(1, Ordering::SeqCst);
        yield_now().await;
        Ok::<_, TestError>("second".to_owned())
    });

    let (first, second) = block_on(zip(first, second));
    assert_eq!(first.unwrap().as_str(), "first");
    assert_eq!(second.unwrap().as_str(), "second");
    assert_eq!(loads.load(Ordering::SeqCst), 2);
}

#[test]
fn validates_names_and_allows_repeated_backend_implementations() {
    let events = events();
    let duplicate = Cache::builder()
        .backend(
            "same-name",
            TestBackend::new("one", Lookup::Miss, Arc::clone(&events)),
        )
        .backend(
            "same-name",
            TestBackend::new("two", Lookup::Miss, Arc::clone(&events)),
        )
        .build();
    assert!(matches!(
        duplicate,
        Err(BuildError::DuplicateBackendName(name)) if name == "same-name"
    ));

    let cache = Cache::builder()
        .backend(
            "one",
            TestBackend::new("one", Lookup::Miss, Arc::clone(&events)),
        )
        .backend(
            "two",
            TestBackend::new("two", Lookup::Miss, Arc::clone(&events)),
        )
        .build()
        .expect("same implementation with unique names is valid");
    assert_eq!(
        cache
            .backend_names()
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>(),
        ["one", "two"]
    );
    assert!(std::ptr::eq(cache.backend_names(), cache.backend_names()));
}

fn hit(value: &str, remaining_ttl: RemainingTTL) -> Lookup<String> {
    Lookup::Hit(CacheEntry::new(Arc::new(value.to_owned()), remaining_ttl))
}

fn stale(value: &str) -> Lookup<String> {
    Lookup::Stale(CacheEntry::new(
        Arc::new(value.to_owned()),
        RemainingTTL::Known(Duration::ZERO),
    ))
}

async fn panicking_loader() -> Result<String, TestError> {
    panic!("loader panic");
}

fn events() -> Arc<Mutex<Vec<Event>>> {
    Arc::new(Mutex::new(Vec::new()))
}

fn take_events(events: &Arc<Mutex<Vec<Event>>>) -> Vec<Event> {
    std::mem::take(&mut *lock(events))
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn poll_once_pending<F>(mut future: Pin<&mut F>)
where
    F: Future,
{
    block_on(poll_fn(|context| {
        assert!(future.as_mut().poll(context).is_pending());
        Poll::Ready(())
    }));
}
