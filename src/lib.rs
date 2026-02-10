mod auth;
mod client;
mod de;
mod error;
mod params;
mod proto;
mod result;
mod session;

pub use auth::TokenSource;
pub use client::{
    Client, ClientConfig, ReadOnlyBuilder, ReadOnlyQueryBuilder, ReadWriteBuilder, ReadWriteResult,
    RequestPriority, TimestampBound,
};
pub use error::{Error, Result};
pub use params::ToSpanner;

pub mod types {
    pub use crate::proto::google::spanner::v1::{KeyRange, KeySet, Type, TypeCode};
}
