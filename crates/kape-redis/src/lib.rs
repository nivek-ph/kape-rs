#![doc = include_str!("../README.md")]

mod backend;
mod codec;
mod error;

pub use backend::RedisBackend;
pub use codec::{RedisCodec, StringCodec, StringCodecError};
pub use error::RedisBackendError;
