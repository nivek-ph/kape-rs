#![doc = include_str!("../README.md")]

mod backend;
mod cache;
mod error;
mod lookup;
mod write;

pub use backend::CacheBackend;
pub use cache::{Cache, CacheBuilder, CacheLookup};
pub use error::{BackendFailure, KapeError, Operation};
pub use lookup::{CacheEntry, Lookup};
pub use write::SetItem;
