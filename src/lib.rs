use distribution::EcdfError;
pub use manager::Manager;
use std::net::AddrParseError;
pub use units::Dscp;
pub use worker::{Worker, WorkerId};
pub use workload::*;

pub mod proto {
    tonic::include_proto!("emu");
}
pub mod cli;
pub mod distribution;
pub mod workload;

mod manager;
pub mod units;
pub(crate) mod util;
mod worker;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("missing required field {0}")]
    MissingField(&'static str),

    #[error("failed to parse IP address")]
    AddrParse(#[from] AddrParseError),

    #[error("invalid DSCP value {0}")]
    InvalidDscp(u32),

    #[error("no p2p workloads specified")]
    NoP2pWorkloads,

    #[error("eCDF error: {0}")]
    Ecdf(#[from] EcdfError),
}
