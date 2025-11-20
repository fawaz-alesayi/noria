#![allow(missing_docs)]
#![deny(unused_extern_crates)]
#![allow(unreachable_pub)]
#![warn(rust_2018_idioms)]

#[macro_use]
extern crate serde_derive;

pub mod consensus;
pub mod data;
pub mod debug;
pub mod internal;

pub use crate::consensus::LocalAuthority;
pub use crate::data::{DataType, Modification, Operation, TableOperation};
pub use crate::debug::stats;
pub use crate::internal::*;
use ahash::RandomState;
use petgraph::graph::NodeIndex;
use std::collections::HashMap;
use std::sync::LazyLock;

/// Types commonly used when working with Noria.
pub mod prelude {
    pub use super::{DataType, Modification, Operation, TableOperation};
}

/// Represents the result of a recipe activation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActivationResult {
    /// Map of query names to `NodeIndex` handles for reads/writes.
    pub new_nodes: HashMap<String, NodeIndex>,
    /// List of leaf nodes that were removed.
    pub removed_leaves: Vec<NodeIndex>,
    /// Number of expressions the recipe added compared to the prior recipe.
    pub expressions_added: usize,
    /// Number of expressions the recipe removed compared to the prior recipe.
    pub expressions_removed: usize,
}

static TEXT_HASHER: LazyLock<RandomState> = LazyLock::new(|| {
    RandomState::with_seeds(
        0x3306_3306_3306_3306,
        0x6033_6033_6033_6033,
        0x0ddc_a11e_c0de_cafe,
        0x1234_5678_9abc_def0,
    )
});

/// Determine which shard a `DataType` belongs to.
#[inline]
pub fn shard_by(dt: &DataType, shards: usize) -> usize {
    match *dt {
        DataType::Int(n) => n as usize % shards,
        DataType::UnsignedInt(n) => n as usize % shards,
        DataType::BigInt(n) => n as usize % shards,
        DataType::UnsignedBigInt(n) => n as usize % shards,
        DataType::Text(..) | DataType::TinyText(..) => {
            let s: &str = dt.into();
            let hash = TEXT_HASHER.hash_one(s);
            hash as usize % shards
        }
        // send all NULL values to the first shard
        DataType::None => 0,
        ref x => {
            unimplemented!("asked to shard on value {:?}", x);
        }
    }
}
