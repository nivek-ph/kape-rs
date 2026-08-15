#![doc = include_str!("../README.md")]

mod backend;
mod error;
mod lookup;

pub use backend::MemoryBackend;
pub use error::MemoryError;
