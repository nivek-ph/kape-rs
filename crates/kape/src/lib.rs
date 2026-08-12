#![doc = include_str!("../README.md")]

mod backend;
mod cache;
mod error;
mod policy;
mod value;

pub use backend::CacheBackend;
pub use cache::{Cache, CacheBuilder, CacheLookup};
pub use error::{BackendFailure, ErrorSource, KapeError, Operation};
pub use policy::{
    BackendOptions, BackendTTLPolicy, BackfillFailurePolicy, LoadOptions, LoadWriteFailurePolicy,
    LoaderFailurePolicy, ReadFailurePolicy,
};
pub use value::{
    BackendSetItem, CacheEntry, IterationEntry, IterationFreshness, IterationPage, Lookup,
    RemainingTTL, ResolvedTTL, SetItem, TTL, TTLContext,
};
