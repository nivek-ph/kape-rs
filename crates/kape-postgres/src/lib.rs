#![doc = include_str!("../README.md")]

mod backend;
mod codec;
mod error;

pub use backend::PostgresBackend;
pub use codec::{PostgresCodec, StringCodec, StringCodecError};
pub use error::PostgresBackendError;
