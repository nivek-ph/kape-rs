#![doc = include_str!("../README.md")]

mod backend;
mod error;

pub use backend::MemoryBackend;
pub use error::MemoryError;
