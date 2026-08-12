use std::{
    collections::HashSet,
    error::Error as StdError,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    BackendFailure, BackendOptions, BackendSetItem, BackfillFailurePolicy, CacheBackend,
    CacheEntry, IterationPage, KapeError as Error, LoadOptions, LoadWriteFailurePolicy,
    LoaderFailurePolicy, Lookup, Operation, ReadFailurePolicy, RemainingTTL, SetItem, TTL,
    TTLContext,
};

struct CacheLayer<K, V> {
    name: Arc<str>,
    backend: Arc<dyn CacheBackend<K, V>>,
    options: BackendOptions,
}

struct CacheInner<K, V> {
    layers: Vec<CacheLayer<K, V>>,
    backend_names: Box<[Arc<str>]>,
}

/// The result of querying the full backend chain.
#[derive(Clone, Debug)]
pub enum CacheLookup<V> {
    /// Every enabled backend missed.
    Miss {
        /// Read failures skipped while reaching the final miss.
        read_failures: Vec<BackendFailure>,
    },
    /// A backend returned a fresh value.
    Hit {
        /// Shared cached value.
        value: Arc<V>,
        /// Name of the backend that returned the value.
        backend: Arc<str>,
        /// Remaining lifetime reported by the hit backend.
        remaining_ttl: RemainingTTL,
        /// Refill failures retained under `ReportAndContinue`.
        backfill_failures: Vec<BackendFailure>,
        /// Read failures skipped before reaching this hit.
        read_failures: Vec<BackendFailure>,
    },
    /// A stale candidate was served because a later backend failed.
    Stale {
        /// Shared stale value.
        value: Arc<V>,
        /// Name of the backend that retained the stale value.
        backend: Arc<str>,
        /// Remaining lifetime metadata reported with the stale value.
        remaining_ttl: RemainingTTL,
        /// Failure that caused the stale value to be served.
        cause: BackendFailure,
        /// Earlier read failures skipped before the stale-serving failure.
        read_failures: Vec<BackendFailure>,
    },
}

impl<V> CacheLookup<V> {
    /// Returns the contained hit or served-stale value.
    #[must_use]
    pub fn value(&self) -> Option<&Arc<V>> {
        match self {
            Self::Miss { .. } => None,
            Self::Hit { value, .. } | Self::Stale { value, .. } => Some(value),
        }
    }

    fn into_value(self) -> Option<Arc<V>> {
        match self {
            Self::Miss { .. } => None,
            Self::Hit { value, .. } | Self::Stale { value, .. } => Some(value),
        }
    }
}

/// An ordered chain of named cache backends.
pub struct Cache<K, V> {
    inner: Arc<CacheInner<K, V>>,
}

impl<K, V> Clone for Cache<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, V> Cache<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    /// Starts an empty cache builder.
    #[must_use]
    pub fn builder() -> CacheBuilder<K, V> {
        CacheBuilder::new()
    }

    /// Returns the cached backend instance names in their configured order.
    #[must_use]
    pub fn backend_names(&self) -> &[Arc<str>] {
        &self.inner.backend_names
    }

    /// Clears every write-enabled backend in reverse configured order.
    ///
    /// All backends are attempted. Backend errors are returned together as
    /// [`Error::PartialFailure`]. Successful clears are not rolled back.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PartialFailure`] when any backend cannot be cleared, or
    /// [`Error::NoBackendEnabled`] when no backend is write-enabled.
    pub async fn clear(&self) -> Result<(), Error> {
        let mut failures = Vec::new();
        let mut any_write = false;
        for (index, layer) in self.inner.layers.iter().enumerate().rev() {
            if !layer.options.write {
                continue;
            }
            any_write = true;
            let started = Instant::now();
            match layer.backend.clear().await {
                Ok(()) => {
                    observe(layer, index, Operation::Clear, "success", started.elapsed());
                }
                Err(source) => {
                    observe(layer, index, Operation::Clear, "error", started.elapsed());
                    failures.push(failure(Operation::Clear, layer, source));
                }
            }
        }
        fanout_result(Operation::Clear, any_write, failures)
    }

    /// Clears one named backend regardless of its write option.
    ///
    /// # Errors
    ///
    /// Returns a named backend failure when clear fails, or
    /// [`Error::BackendNotFound`] when `backend` is not configured.
    pub async fn clear_backend(&self, backend: &str) -> Result<(), Error> {
        let (index, layer) = self.find_layer(backend)?;
        let started = Instant::now();
        match layer.backend.clear().await {
            Ok(()) => {
                observe(layer, index, Operation::Clear, "success", started.elapsed());
                Ok(())
            }
            Err(source) => {
                observe(layer, index, Operation::Clear, "error", started.elapsed());
                Err(Error::Backend(failure(Operation::Clear, layer, source)))
            }
        }
    }

    /// Scans one named backend without merging entries across the chain.
    ///
    /// The cursor is backend-specific and opaque. Scanning is weakly
    /// consistent: concurrent writes, expiration, and eviction may change
    /// later pages.
    ///
    /// # Errors
    ///
    /// Returns a named backend failure when iteration fails,
    /// [`Error::BackendNotFound`] for an unknown name, or
    /// [`Error::InvalidIterationLimit`] when `limit` is zero.
    pub async fn scan(
        &self,
        backend: &str,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<IterationPage<K, V>, Error> {
        if limit == 0 {
            return Err(Error::InvalidIterationLimit);
        }
        let (index, layer) = self.find_layer(backend)?;
        let started = Instant::now();
        match layer.backend.iterate(cursor, limit).await {
            Ok(page) => {
                observe(
                    layer,
                    index,
                    Operation::Iterate,
                    "success",
                    started.elapsed(),
                );
                Ok(page)
            }
            Err(source) => {
                observe(layer, index, Operation::Iterate, "error", started.elapsed());
                Err(Error::Backend(failure(Operation::Iterate, layer, source)))
            }
        }
    }

    /// Releases backend resources in reverse configured order.
    ///
    /// The operation is idempotent by contract. Backends managed by handle
    /// lifetime may implement it as a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PartialFailure`] when any backend disconnect fails.
    pub async fn disconnect(&self) -> Result<(), Error> {
        let mut failures = Vec::new();
        for (index, layer) in self.inner.layers.iter().enumerate().rev() {
            let started = Instant::now();
            match layer.backend.disconnect().await {
                Ok(()) => observe(
                    layer,
                    index,
                    Operation::Disconnect,
                    "success",
                    started.elapsed(),
                ),
                Err(source) => {
                    observe(
                        layer,
                        index,
                        Operation::Disconnect,
                        "error",
                        started.elapsed(),
                    );
                    failures.push(failure(Operation::Disconnect, layer, source));
                }
            }
        }
        fanout_result(Operation::Disconnect, true, failures)
    }

    fn find_layer(&self, name: &str) -> Result<(usize, &CacheLayer<K, V>), Error> {
        self.inner
            .layers
            .iter()
            .enumerate()
            .find(|(_, layer)| layer.name.as_ref() == name)
            .ok_or_else(|| Error::BackendNotFound(Arc::from(name)))
    }

    /// Reads backends in their user-defined order and returns full metadata.
    ///
    /// # Errors
    ///
    /// Returns a named backend failure according to its read or backfill
    /// policy, or [`Error::NoBackendEnabled`] when no backend is readable.
    pub async fn lookup(&self, key: &K) -> Result<CacheLookup<V>, Error> {
        Ok(self.lookup_internal(key).await?.lookup)
    }

    /// Reads a cached value, discarding source and refill metadata.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::lookup`].
    pub async fn get(&self, key: &K) -> Result<Option<Arc<V>>, Error> {
        Ok(self.lookup(key).await?.into_value())
    }

    /// Checks whether a key has a fresh value without triggering backfill.
    ///
    /// A retained stale entry is reported as absent. This is the scalar
    /// counterpart of [`Self::has_many`] and uses the same backend capability
    /// and read-failure policies.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::has_many`].
    pub async fn has(&self, key: &K) -> Result<bool, Error> {
        let mut result = self.has_many(std::slice::from_ref(key)).await?;
        Ok(result.pop().unwrap_or(false))
    }

    /// Reads multiple keys while preserving input order and duplicates.
    ///
    /// Backends are queried with their batch API in configured order. Fresh
    /// hits are backfilled with the same remaining-TTL rules as [`Self::lookup`].
    ///
    /// # Errors
    ///
    /// Returns a propagated named backend failure, an invalid backend batch
    /// response, or [`Error::NoBackendEnabled`] when no backend is readable.
    pub async fn lookup_many(&self, keys: &[K]) -> Result<Vec<CacheLookup<V>>, Error> {
        self.lookup_many_internal(keys, true).await
    }

    /// Reads multiple values, discarding source and refill metadata.
    ///
    /// The returned vector has exactly the same length and order as `keys`.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::lookup_many`].
    pub async fn get_many(&self, keys: &[K]) -> Result<Vec<Option<Arc<V>>>, Error> {
        Ok(self
            .lookup_many(keys)
            .await?
            .into_iter()
            .map(CacheLookup::into_value)
            .collect())
    }

    /// Checks whether multiple keys have a fresh value without backfilling.
    ///
    /// Stale entries are absent. The returned vector preserves input order and
    /// duplicates. `SkipBackend` remains observable through tracing; like
    /// [`Self::get`], this ergonomic method does not return non-fatal metadata.
    ///
    /// # Errors
    ///
    /// Returns a propagated named backend failure, an invalid backend batch
    /// response, or [`Error::NoBackendEnabled`] when no backend is readable.
    pub async fn has_many(&self, keys: &[K]) -> Result<Vec<bool>, Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut present = vec![false; keys.len()];
        let mut resolved = vec![false; keys.len()];
        let mut any_read = false;

        for (layer_index, layer) in self.inner.layers.iter().enumerate() {
            if !layer.options.read {
                continue;
            }
            let unresolved = resolved
                .iter()
                .enumerate()
                .filter_map(|(index, done)| (!done).then_some(index))
                .collect::<Vec<_>>();
            if unresolved.is_empty() {
                break;
            }
            any_read = true;
            let backend_keys = unresolved
                .iter()
                .map(|index| &keys[*index])
                .collect::<Vec<_>>();
            let started = Instant::now();
            let backend_result = match layer.backend.has_many(&backend_keys).await {
                Ok(result) => {
                    observe(
                        layer,
                        layer_index,
                        Operation::Get,
                        "batch_has_success",
                        started.elapsed(),
                    );
                    result
                }
                Err(source) => {
                    observe(
                        layer,
                        layer_index,
                        Operation::Get,
                        "batch_has_error",
                        started.elapsed(),
                    );
                    let cause = failure(Operation::Get, layer, source);
                    match layer.options.read_failure {
                        ReadFailurePolicy::SkipBackend => continue,
                        ReadFailurePolicy::Propagate | ReadFailurePolicy::ServeStale => {
                            return Err(Error::Backend(cause));
                        }
                    }
                }
            };
            validate_batch_len(
                Operation::Get,
                layer,
                unresolved.len(),
                backend_result.len(),
            )?;

            for (item_index, exists) in unresolved.into_iter().zip(backend_result) {
                if exists {
                    present[item_index] = true;
                    resolved[item_index] = true;
                }
            }
        }

        if any_read {
            Ok(present)
        } else {
            Err(Error::NoBackendEnabled(Operation::Get))
        }
    }

    /// Writes all enabled backends sequentially in user-defined order.
    ///
    /// Later writes are attempted after a failure and all failures are returned
    /// together. Successful writes are not rolled back.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PartialFailure`] when any enabled write fails, or
    /// [`Error::NoBackendEnabled`] when no backend accepts explicit writes.
    pub async fn set(&self, key: &K, value: Arc<V>, ttl: TTL) -> Result<(), Error> {
        self.set_with_ttl(key, value, ttl, |_| None).await
    }

    /// Writes all enabled backends with a dynamically selected TTL.
    ///
    /// `ttl_for_backend` is evaluated in configured backend order before any
    /// backend write begins. Returning `Some(ttl)` overrides `fallback_ttl` for
    /// that backend; returning `None` retains the fallback. Each selected TTL
    /// is then resolved against the destination backend's default and maximum
    /// TTL policy.
    ///
    /// The selector is called only for backends that participate in explicit
    /// writes. It receives the backend's unique name and absolute configured
    /// index together with the typed key and value.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PartialFailure`] when any enabled write fails, or
    /// [`Error::NoBackendEnabled`] when no backend accepts explicit writes.
    pub async fn set_with_ttl<F>(
        &self,
        key: &K,
        value: Arc<V>,
        fallback_ttl: TTL,
        ttl_for_backend: F,
    ) -> Result<(), Error>
    where
        F: for<'a> Fn(TTLContext<'a, K, V>) -> Option<TTL>,
    {
        let mut failures = Vec::new();
        let writes = self
            .inner
            .layers
            .iter()
            .enumerate()
            .filter(|(_, layer)| layer.options.write)
            .map(|(index, layer)| {
                let requested = ttl_for_backend(TTLContext {
                    backend: &layer.name,
                    backend_index: index,
                    key,
                    value: value.as_ref(),
                })
                .unwrap_or(fallback_ttl);
                (index, layer, layer.options.ttl.resolve_write(requested))
            })
            .collect::<Vec<_>>();

        for (index, layer, resolved) in &writes {
            let started = Instant::now();
            match layer.backend.set(key, Arc::clone(&value), *resolved).await {
                Ok(()) => observe(layer, *index, Operation::Set, "success", started.elapsed()),
                Err(source) => {
                    observe(layer, *index, Operation::Set, "error", started.elapsed());
                    failures.push(failure(Operation::Set, layer, source));
                }
            }
        }

        fanout_result(Operation::Set, !writes.is_empty(), failures)
    }

    /// Writes multiple items to all write-enabled backends in configured order.
    ///
    /// Item order and duplicate keys are retained. Each item carries its own
    /// fallback TTL.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PartialFailure`] when any enabled backend batch fails,
    /// or [`Error::NoBackendEnabled`] when no backend accepts writes.
    pub async fn set_many(&self, items: &[SetItem<K, V>]) -> Result<(), Error> {
        self.set_many_with_ttl(items, |_, _| None).await
    }

    /// Writes multiple items with dynamic per-item, per-backend TTL selection.
    ///
    /// The selector receives the input item index and [`TTLContext`]. Returning
    /// `None` retains that item's [`SetItem::ttl`]. All selections are evaluated
    /// in backend order and item order before the first backend write starts.
    /// Destination default and maximum TTL policies are applied afterwards.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PartialFailure`] when any enabled backend batch fails,
    /// or [`Error::NoBackendEnabled`] when no backend accepts writes.
    pub async fn set_many_with_ttl<F>(
        &self,
        items: &[SetItem<K, V>],
        ttl_for_backend: F,
    ) -> Result<(), Error>
    where
        F: for<'a> Fn(usize, TTLContext<'a, K, V>) -> Option<TTL>,
    {
        if items.is_empty() {
            return Ok(());
        }

        let writes = self
            .inner
            .layers
            .iter()
            .enumerate()
            .filter(|(_, layer)| layer.options.write)
            .map(|(backend_index, layer)| {
                let ttls = items
                    .iter()
                    .enumerate()
                    .map(|(item_index, item)| {
                        let requested = ttl_for_backend(
                            item_index,
                            TTLContext {
                                backend: &layer.name,
                                backend_index,
                                key: &item.key,
                                value: item.value.as_ref(),
                            },
                        )
                        .unwrap_or(item.ttl);
                        layer.options.ttl.resolve_write(requested)
                    })
                    .collect::<Vec<_>>();
                (backend_index, layer, ttls)
            })
            .collect::<Vec<_>>();

        let mut failures = Vec::new();
        for (backend_index, layer, ttls) in &writes {
            let backend_items = items
                .iter()
                .zip(ttls)
                .map(|(item, ttl)| BackendSetItem {
                    key: &item.key,
                    value: &item.value,
                    ttl: *ttl,
                })
                .collect::<Vec<_>>();
            let started = Instant::now();
            match layer.backend.set_many(&backend_items).await {
                Ok(()) => observe(
                    layer,
                    *backend_index,
                    Operation::Set,
                    "batch_success",
                    started.elapsed(),
                ),
                Err(source) => {
                    observe(
                        layer,
                        *backend_index,
                        Operation::Set,
                        "batch_error",
                        started.elapsed(),
                    );
                    failures.push(failure(Operation::Set, layer, source));
                }
            }
        }

        fanout_result(Operation::Set, !writes.is_empty(), failures)
    }

    /// Removes a key sequentially in reverse backend order.
    ///
    /// Every write-enabled backend is attempted and failures are aggregated.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PartialFailure`] when any removal fails, or
    /// [`Error::NoBackendEnabled`] when no backend accepts removals.
    pub async fn remove(&self, key: &K) -> Result<(), Error> {
        let mut failures = Vec::new();
        let mut any_write = false;

        for (index, layer) in self.inner.layers.iter().enumerate().rev() {
            if !layer.options.write {
                continue;
            }
            any_write = true;
            let started = Instant::now();
            match layer.backend.remove(key).await {
                Ok(()) => observe(
                    layer,
                    index,
                    Operation::Remove,
                    "success",
                    started.elapsed(),
                ),
                Err(source) => {
                    observe(layer, index, Operation::Remove, "error", started.elapsed());
                    failures.push(failure(Operation::Remove, layer, source));
                }
            }
        }

        fanout_result(Operation::Remove, any_write, failures)
    }

    /// Removes multiple keys from write-enabled backends in reverse order.
    ///
    /// Input order and duplicate keys are retained inside every backend batch.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PartialFailure`] when any backend batch fails, or
    /// [`Error::NoBackendEnabled`] when no backend accepts removals.
    pub async fn remove_many(&self, keys: &[K]) -> Result<(), Error> {
        if keys.is_empty() {
            return Ok(());
        }

        let backend_keys = keys.iter().collect::<Vec<_>>();
        let mut failures = Vec::new();
        let mut any_write = false;
        for (index, layer) in self.inner.layers.iter().enumerate().rev() {
            if !layer.options.write {
                continue;
            }
            any_write = true;
            let started = Instant::now();
            match layer.backend.remove_many(&backend_keys).await {
                Ok(()) => observe(
                    layer,
                    index,
                    Operation::Remove,
                    "batch_success",
                    started.elapsed(),
                ),
                Err(source) => {
                    observe(
                        layer,
                        index,
                        Operation::Remove,
                        "batch_error",
                        started.elapsed(),
                    );
                    failures.push(failure(Operation::Remove, layer, source));
                }
            }
        }
        fanout_result(Operation::Remove, any_write, failures)
    }

    /// Reads multiple values and then invalidates every requested key.
    ///
    /// This operation is deliberately not described as atomic: unrelated
    /// backends cannot participate in one transaction, and another caller may
    /// write between the read and reverse-order removal phases. Reads do not
    /// trigger backfill.
    ///
    /// # Errors
    ///
    /// Returns lookup failures or removal failures. If removal fails, values
    /// already read are not returned because invalidation is incomplete.
    pub async fn take_many(&self, keys: &[K]) -> Result<Vec<Option<Arc<V>>>, Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let values = self
            .lookup_many_internal(keys, false)
            .await?
            .into_iter()
            .map(CacheLookup::into_value)
            .collect::<Vec<_>>();
        self.remove_many(keys).await?;
        Ok(values)
    }

    /// Reads one value and then invalidates it from every write-enabled backend.
    ///
    /// Like [`Self::take_many`], this operation is not atomic across unrelated
    /// backend systems. The read does not trigger backfill, and removal runs in
    /// reverse configured order.
    ///
    /// # Errors
    ///
    /// Returns the same lookup or removal failures as [`Self::take_many`].
    pub async fn take(&self, key: &K) -> Result<Option<Arc<V>>, Error> {
        let mut result = self.take_many(std::slice::from_ref(key)).await?;
        Ok(result.pop().unwrap_or(None))
    }

    async fn lookup_internal(&self, key: &K) -> Result<LookupScan<V>, Error> {
        let mut stale = None;
        let mut read_failures = Vec::new();
        let mut any_read = false;

        for (index, layer) in self.inner.layers.iter().enumerate() {
            if !layer.options.read {
                continue;
            }
            any_read = true;
            let started = Instant::now();

            match layer.backend.get(key).await {
                Ok(Lookup::Miss) => {
                    observe(layer, index, Operation::Get, "miss", started.elapsed());
                }
                Ok(Lookup::Stale(entry)) => {
                    observe(layer, index, Operation::Get, "stale", started.elapsed());
                    if stale.is_none() {
                        stale = Some(StaleCandidate {
                            value: entry.value,
                            backend: Arc::clone(&layer.name),
                            remaining_ttl: entry.remaining_ttl,
                        });
                    }
                }
                Ok(Lookup::Hit(entry)) => {
                    observe(layer, index, Operation::Get, "hit", started.elapsed());
                    let backfill_failures = self.backfill(key, &entry, index).await?;
                    return Ok(LookupScan {
                        lookup: CacheLookup::Hit {
                            value: entry.value,
                            backend: Arc::clone(&layer.name),
                            remaining_ttl: entry.remaining_ttl,
                            backfill_failures,
                            read_failures,
                        },
                        stale,
                    });
                }
                Err(source) => {
                    observe(layer, index, Operation::Get, "error", started.elapsed());
                    let cause = failure(Operation::Get, layer, source);
                    match layer.options.read_failure {
                        ReadFailurePolicy::Propagate => return Err(Error::Backend(cause)),
                        ReadFailurePolicy::SkipBackend => read_failures.push(cause),
                        ReadFailurePolicy::ServeStale => {
                            let Some(stale) = stale else {
                                return Err(Error::Backend(cause));
                            };
                            return Ok(LookupScan {
                                lookup: CacheLookup::Stale {
                                    value: stale.value,
                                    backend: stale.backend,
                                    remaining_ttl: stale.remaining_ttl,
                                    cause,
                                    read_failures,
                                },
                                stale: None,
                            });
                        }
                    }
                }
            }
        }

        if any_read {
            Ok(LookupScan {
                lookup: CacheLookup::Miss { read_failures },
                stale,
            })
        } else {
            Err(Error::NoBackendEnabled(Operation::Get))
        }
    }

    async fn lookup_many_internal(
        &self,
        keys: &[K],
        backfill: bool,
    ) -> Result<Vec<CacheLookup<V>>, Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut states = (0..keys.len())
            .map(|_| BatchLookupState::default())
            .collect::<Vec<_>>();
        let mut any_read = false;

        for (layer_index, layer) in self.inner.layers.iter().enumerate() {
            if !layer.options.read {
                continue;
            }
            if states.iter().all(|state| state.lookup.is_some()) {
                break;
            }
            any_read = true;
            self.lookup_many_layer(keys, &mut states, layer_index, layer)
                .await?;
        }

        if !any_read {
            return Err(Error::NoBackendEnabled(Operation::Get));
        }

        for state in &mut states {
            if state.lookup.is_none() {
                state.lookup = Some(CacheLookup::Miss {
                    read_failures: std::mem::take(&mut state.read_failures),
                });
            }
        }

        if backfill {
            for (item_index, state) in states.iter_mut().enumerate() {
                let Some(hit_index) = state.hit_index else {
                    continue;
                };
                let CacheLookup::Hit {
                    value,
                    remaining_ttl,
                    backfill_failures,
                    ..
                } = state.lookup.as_mut().expect("batch lookup was finalized")
                else {
                    continue;
                };
                let entry = CacheEntry::new(Arc::clone(value), *remaining_ttl);
                *backfill_failures = self.backfill(&keys[item_index], &entry, hit_index).await?;
            }
        }

        Ok(states
            .into_iter()
            .map(|state| state.lookup.expect("batch lookup was finalized"))
            .collect())
    }

    async fn lookup_many_layer(
        &self,
        keys: &[K],
        states: &mut [BatchLookupState<V>],
        layer_index: usize,
        layer: &CacheLayer<K, V>,
    ) -> Result<(), Error> {
        let unresolved = states
            .iter()
            .enumerate()
            .filter_map(|(index, state)| state.lookup.is_none().then_some(index))
            .collect::<Vec<_>>();
        let backend_keys = unresolved
            .iter()
            .map(|index| &keys[*index])
            .collect::<Vec<_>>();
        let started = Instant::now();
        let backend_result = match layer.backend.get_many(&backend_keys).await {
            Ok(result) => {
                observe(
                    layer,
                    layer_index,
                    Operation::Get,
                    "batch_success",
                    started.elapsed(),
                );
                result
            }
            Err(source) => {
                observe(
                    layer,
                    layer_index,
                    Operation::Get,
                    "batch_error",
                    started.elapsed(),
                );
                let cause = failure(Operation::Get, layer, source);
                return match layer.options.read_failure {
                    ReadFailurePolicy::Propagate => Err(Error::Backend(cause)),
                    ReadFailurePolicy::SkipBackend => {
                        for index in unresolved {
                            states[index].read_failures.push(cause.clone());
                        }
                        Ok(())
                    }
                    ReadFailurePolicy::ServeStale => serve_batch_stale(states, unresolved, cause),
                };
            }
        };
        validate_batch_len(
            Operation::Get,
            layer,
            unresolved.len(),
            backend_result.len(),
        )?;

        for (item_index, lookup) in unresolved.into_iter().zip(backend_result) {
            match lookup {
                Lookup::Stale(entry) if states[item_index].stale.is_none() => {
                    states[item_index].stale = Some(StaleCandidate {
                        value: entry.value,
                        backend: Arc::clone(&layer.name),
                        remaining_ttl: entry.remaining_ttl,
                    });
                }
                Lookup::Miss | Lookup::Stale(_) => {}
                Lookup::Hit(entry) => {
                    states[item_index].hit_index = Some(layer_index);
                    states[item_index].lookup = Some(CacheLookup::Hit {
                        value: entry.value,
                        backend: Arc::clone(&layer.name),
                        remaining_ttl: entry.remaining_ttl,
                        backfill_failures: Vec::new(),
                        read_failures: std::mem::take(&mut states[item_index].read_failures),
                    });
                }
            }
        }
        Ok(())
    }

    async fn backfill(
        &self,
        key: &K,
        entry: &CacheEntry<V>,
        hit_index: usize,
    ) -> Result<Vec<BackendFailure>, Error> {
        let mut failures = Vec::new();

        for (index, layer) in self.inner.layers[..hit_index].iter().enumerate() {
            if !layer.options.backfill {
                continue;
            }
            let Some(ttl) = layer.options.ttl.resolve_backfill(entry.remaining_ttl) else {
                observe(
                    layer,
                    index,
                    Operation::Backfill,
                    "ttl_skip",
                    Duration::ZERO,
                );
                continue;
            };

            let started = Instant::now();
            match layer.backend.set(key, Arc::clone(&entry.value), ttl).await {
                Ok(()) => observe(
                    layer,
                    index,
                    Operation::Backfill,
                    "success",
                    started.elapsed(),
                ),
                Err(source) => {
                    observe(
                        layer,
                        index,
                        Operation::Backfill,
                        "error",
                        started.elapsed(),
                    );
                    let failure = failure(Operation::Backfill, layer, source);
                    match layer.options.backfill_failure {
                        BackfillFailurePolicy::Propagate => {
                            return Err(Error::Backend(failure));
                        }
                        BackfillFailurePolicy::ReportAndContinue => failures.push(failure),
                    }
                }
            }
        }
        Ok(failures)
    }
    /// Gets a value or computes and caches it with default load policies.
    ///
    /// # Errors
    ///
    /// Returns lookup, loader, or cache write failures according to the default
    /// [`LoadOptions`].
    pub async fn get_or_load<F, Fut, E>(&self, key: &K, loader: F) -> Result<Arc<V>, Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
        E: StdError + Send + Sync + 'static,
    {
        self.get_or_load_with(key, LoadOptions::default(), loader)
            .await
    }

    /// Gets a value or computes and caches it with explicit load policies.
    ///
    /// # Errors
    ///
    /// Returns lookup, loader, or cache write failures according to `options`.
    pub async fn get_or_load_with<F, Fut, E>(
        &self,
        key: &K,
        options: LoadOptions,
        loader: F,
    ) -> Result<Arc<V>, Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
        E: StdError + Send + Sync + 'static,
    {
        self.get_or_load_inner(key, options, |_| None, loader).await
    }

    /// Gets or computes a value and dynamically selects its write TTL per backend.
    ///
    /// The selector has the same semantics as [`Self::set_with_ttl`]. Returning
    /// `None` uses [`LoadOptions::ttl`] as the fallback for that backend.
    /// The selector is evaluated only after the loader successfully produces a
    /// value and before that value is written.
    ///
    /// # Errors
    ///
    /// Returns lookup, loader, or cache write failures according to `options`.
    pub async fn get_or_load_with_ttl<F, Fut, E, T>(
        &self,
        key: &K,
        options: LoadOptions,
        ttl_for_backend: T,
        loader: F,
    ) -> Result<Arc<V>, Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
        E: StdError + Send + Sync + 'static,
        T: for<'a> Fn(TTLContext<'a, K, V>) -> Option<TTL>,
    {
        self.get_or_load_inner(key, options, ttl_for_backend, loader)
            .await
    }

    async fn get_or_load_inner<F, Fut, E, T>(
        &self,
        key: &K,
        options: LoadOptions,
        ttl_for_backend: T,
        loader: F,
    ) -> Result<Arc<V>, Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
        E: StdError + Send + Sync + 'static,
        T: for<'a> Fn(TTLContext<'a, K, V>) -> Option<TTL>,
    {
        let scan = self.lookup_internal(key).await?;
        if let Some(value) = scan.lookup.into_value() {
            return Ok(value);
        }
        let stale = scan.stale;

        let value = match loader().await {
            Ok(value) => Arc::new(value),
            Err(error) => {
                if options.loader_failure == LoaderFailurePolicy::ServeStale
                    && let Some(stale) = stale
                {
                    observe_load("loader_error_serve_stale");
                    return Ok(stale.value);
                }
                observe_load("loader_error");
                return Err(Error::loader(error));
            }
        };

        if let Err(error) = self
            .set_with_ttl(key, Arc::clone(&value), options.ttl, ttl_for_backend)
            .await
        {
            match options.write_failure {
                LoadWriteFailurePolicy::Propagate => return Err(error),
                LoadWriteFailurePolicy::ReturnValue => observe_load("write_error_return_value"),
            }
        }

        Ok(value)
    }
}

/// Builds a [`Cache`] while retaining the concrete `K` and `V` types.
pub struct CacheBuilder<K, V> {
    layers: Vec<CacheLayer<K, V>>,
}

impl<K, V> CacheBuilder<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    /// Creates an empty builder.
    #[must_use]
    pub const fn new() -> Self {
        Self { layers: Vec::new() }
    }

    /// Appends a backend with default options.
    #[must_use]
    pub fn backend<B>(self, name: impl Into<Arc<str>>, backend: B) -> Self
    where
        B: CacheBackend<K, V> + 'static,
    {
        self.backend_with(name, backend, BackendOptions::default())
    }

    /// Appends a backend with per-instance options.
    #[must_use]
    pub fn backend_with<B>(
        mut self,
        name: impl Into<Arc<str>>,
        backend: B,
        options: BackendOptions,
    ) -> Self
    where
        B: CacheBackend<K, V> + 'static,
    {
        self.layers.push(CacheLayer {
            name: name.into(),
            backend: Arc::new(backend),
            options,
        });
        self
    }

    /// Validates backend names and finishes the ordered chain.
    ///
    /// # Errors
    ///
    /// Returns [`crate::KapeError`] when the chain is empty or contains an empty or
    /// duplicate backend name.
    pub fn build(self) -> Result<Cache<K, V>, Error> {
        if self.layers.is_empty() {
            return Err(Error::NoBackends);
        }

        let mut seen_names = HashSet::with_capacity(self.layers.len());
        for layer in &self.layers {
            if layer.name.trim().is_empty() {
                return Err(Error::EmptyBackendName);
            }
            if !seen_names.insert(layer.name.as_ref()) {
                return Err(Error::DuplicateBackendName(layer.name.to_string()));
            }
        }

        let backend_names = self
            .layers
            .iter()
            .map(|layer| Arc::clone(&layer.name))
            .collect();

        Ok(Cache {
            inner: Arc::new(CacheInner {
                layers: self.layers,
                backend_names,
            }),
        })
    }
}

impl<K, V> Default for CacheBuilder<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
}

struct LookupScan<V> {
    lookup: CacheLookup<V>,
    stale: Option<StaleCandidate<V>>,
}

struct BatchLookupState<V> {
    stale: Option<StaleCandidate<V>>,
    read_failures: Vec<BackendFailure>,
    lookup: Option<CacheLookup<V>>,
    hit_index: Option<usize>,
}

impl<V> Default for BatchLookupState<V> {
    fn default() -> Self {
        Self {
            stale: None,
            read_failures: Vec::new(),
            lookup: None,
            hit_index: None,
        }
    }
}

struct StaleCandidate<V> {
    value: Arc<V>,
    backend: Arc<str>,
    remaining_ttl: RemainingTTL,
}

fn failure<K, V>(operation: Operation, layer: &CacheLayer<K, V>, source: Error) -> BackendFailure {
    BackendFailure {
        operation,
        backend: Arc::clone(&layer.name),
        source: source.into_source(),
    }
}

#[derive(Debug)]
struct BatchResultLengthError {
    expected: usize,
    actual: usize,
}

impl fmt::Display for BatchResultLengthError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "backend batch returned {} result(s), expected {}",
            self.actual, self.expected
        )
    }
}

impl StdError for BatchResultLengthError {}

fn validate_batch_len<K, V>(
    operation: Operation,
    layer: &CacheLayer<K, V>,
    expected: usize,
    actual: usize,
) -> Result<(), Error> {
    if expected == actual {
        Ok(())
    } else {
        Err(Error::Backend(failure(
            operation,
            layer,
            Error::backend(BatchResultLengthError { expected, actual }),
        )))
    }
}

fn serve_batch_stale<V>(
    states: &mut [BatchLookupState<V>],
    unresolved: Vec<usize>,
    cause: BackendFailure,
) -> Result<(), Error> {
    if unresolved
        .iter()
        .any(|index| states[*index].stale.is_none())
    {
        return Err(Error::Backend(cause));
    }
    for index in unresolved {
        let stale = states[index]
            .stale
            .take()
            .expect("all unresolved entries have stale candidates");
        states[index].lookup = Some(CacheLookup::Stale {
            value: stale.value,
            backend: stale.backend,
            remaining_ttl: stale.remaining_ttl,
            cause: cause.clone(),
            read_failures: std::mem::take(&mut states[index].read_failures),
        });
    }
    Ok(())
}

fn fanout_result(
    operation: Operation,
    any_enabled: bool,
    failures: Vec<BackendFailure>,
) -> Result<(), Error> {
    if !any_enabled {
        Err(Error::NoBackendEnabled(operation))
    } else if failures.is_empty() {
        Ok(())
    } else {
        Err(Error::PartialFailure {
            operation,
            failures,
        })
    }
}

#[cfg(feature = "tracing")]
fn observe<K, V>(
    layer: &CacheLayer<K, V>,
    index: usize,
    operation: Operation,
    outcome: &'static str,
    elapsed: Duration,
) {
    tracing::event!(
        target: "kape",
        tracing::Level::DEBUG,
        backend = %layer.name,
        backend.index = index,
        ?operation,
        outcome,
        elapsed_ms = elapsed.as_secs_f64() * 1_000.0,
    );
}

#[cfg(not(feature = "tracing"))]
fn observe<K, V>(
    _layer: &CacheLayer<K, V>,
    _index: usize,
    _operation: Operation,
    _outcome: &'static str,
    _elapsed: Duration,
) {
}

#[cfg(feature = "tracing")]
fn observe_load(outcome: &'static str) {
    tracing::event!(target: "kape", tracing::Level::DEBUG, operation = "load", outcome);
}

#[cfg(not(feature = "tracing"))]
fn observe_load(_outcome: &'static str) {}
