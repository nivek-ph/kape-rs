#![doc = include_str!("../README.md")]

mod backend;
mod cache;
mod error;
mod set;

pub use backend::CacheBackend;
pub use cache::{Cache, CacheBuilder, CacheEntry, CacheHit};
pub use error::{BackendFailure, KapeError, KapeResult, Operation};
pub use set::SetItem;
#[doc(hidden)]
pub use set::validate_set_items;
