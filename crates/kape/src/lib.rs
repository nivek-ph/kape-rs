#![doc = include_str!("../README.md")]

mod backend;
mod cache;
mod error;
mod write;

pub use backend::CacheBackend;
pub use cache::{Cache, CacheBuilder, CacheEntry, CacheHit};
pub use error::{BackendFailure, KapeError, Operation};
pub use write::SetItem;
