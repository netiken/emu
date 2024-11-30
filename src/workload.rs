use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

use crate::{
    distribution::{DistShape, Ecdf, EcdfError},
    proto,
    units::{BitsPerSec, Bytes, Dscp, Mbps, Microsecs, Nanosecs, Secs},
    Error, WorkerId,
};

// gRPC limits the maximum messages size to 4MB.
pub const SZ_GRPC_MAX: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunInput {
    pub spec: RunSpecification,
    pub profile: NetworkProfile,
}

impl TryFrom<proto::RunInput> for RunInput {
    type Error = Error;

    fn try_from(value: proto::RunInput) -> Result<Self, Self::Error> {
        let spec = value.run_spec.ok_or(Error::MissingField("run_spec"))?;
        let profile = value
            .network_profile
            .ok_or(Error::MissingField("network_profile"))?;
        let spec = RunSpecification::try_from(spec)?;
        let profile = NetworkProfile::try_from(profile)?;
        Ok(Self { spec, profile })
    }
}

impl From<RunInput> for proto::RunInput {
    fn from(value: RunInput) -> Self {
        Self {
            run_spec: Some(value.spec.into()),
            network_profile: Some(proto::NetworkProfile {
                profile_type: match value.profile {
                    NetworkProfile::AllToAll(all_to_all) => Some(
                        proto::network_profile::ProfileType::AllToAll(proto::AllToAll {
                            rtt_us: all_to_all.rtt.into_inner(),
                            bandwidth_mbps: all_to_all.bandwidth.into_inner(),
                        }),
                    ),
                },
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSpecification {
    pub p2p_workloads: Vec<P2PWorkload>,
    pub size_distribution: Arc<Ecdf>,
    pub output_buckets: Vec<Bytes>,
}

impl RunSpecification {
    /// Gets the minimum number of nr workers necessary for this run specification
    pub fn nr_workers(&self) -> Result<usize, Error> {
        let mut unique_nr_workers = HashSet::<WorkerId>::new();
        self.p2p_workloads.iter().for_each(|workload| {
            unique_nr_workers.insert(workload.src);
            unique_nr_workers.insert(workload.dst);
        });
        Ok(unique_nr_workers.len())
    }

    /// Gets the running duration of this experiment, not including the probe duration.
    pub fn run_duration(&self) -> Result<Secs, Error> {
        Ok(self
            .p2p_workloads
            .iter()
            .map(|workload| workload.duration)
            .max()
            .unwrap())
    }
}

impl TryFrom<proto::RunSpecification> for RunSpecification {
    type Error = Error;

    fn try_from(proto: proto::RunSpecification) -> Result<Self, Self::Error> {
        let p2p_workloads = proto
            .p2p_workloads
            .into_iter()
            .map(P2PWorkload::try_from)
            .collect::<Result<_, _>>()?;
        let size_distribution = proto
            .size_distribution
            .ok_or(Error::MissingField("size_distribution"))?;
        let &proto::CdfPoint { x: max, .. } = size_distribution
            .points
            .last()
            .ok_or(Error::Ecdf(EcdfError::NoValues))?;
        let max = max.ceil() as usize;
        if max >= SZ_GRPC_MAX {
            return Err(Error::MaxMessageSize(max));
        }
        let size_distribution = Arc::new(Ecdf::try_from(size_distribution)?);
        let output_buckets = proto.output_buckets.into_iter().map(Bytes::new).collect();
        Ok(Self {
            p2p_workloads,
            size_distribution,
            output_buckets,
        })
    }
}

impl TryFrom<proto::Ecdf> for Ecdf {
    type Error = EcdfError;

    fn try_from(value: proto::Ecdf) -> Result<Self, Self::Error> {
        let ecdf = value
            .points
            .into_iter()
            .map(|point| (point.x, point.y))
            .collect();
        Self::from_ecdf(ecdf)
    }
}

impl From<Ecdf> for proto::Ecdf {
    fn from(ecdf: Ecdf) -> Self {
        Self {
            points: ecdf
                .points()
                .map(|(x, y)| proto::CdfPoint { x, y })
                .collect(),
        }
    }
}

impl From<RunSpecification> for proto::RunSpecification {
    fn from(spec: RunSpecification) -> Self {
        Self {
            p2p_workloads: spec
                .p2p_workloads
                .into_iter()
                .map(proto::P2pWorkload::from)
                .collect(),
            size_distribution: Some(proto::Ecdf::from((*spec.size_distribution).clone())),
            output_buckets: spec
                .output_buckets
                .into_iter()
                .map(|b| b.into_inner())
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetworkProfile {
    AllToAll(AllToAll),
}

impl NetworkProfile {
    pub fn ideal_latency(&self, size: Bytes) -> Nanosecs {
        match self {
            NetworkProfile::AllToAll(profile) => profile.ideal_latency(size),
        }
    }
}

impl TryFrom<proto::NetworkProfile> for NetworkProfile {
    type Error = Error;

    fn try_from(proto: proto::NetworkProfile) -> Result<Self, Self::Error> {
        let profile = proto
            .profile_type
            .ok_or(Error::MissingField("profile_type"))?;
        let profile = match profile {
            proto::network_profile::ProfileType::AllToAll(all_to_all) => {
                NetworkProfile::AllToAll(AllToAll {
                    rtt: Microsecs::new(all_to_all.rtt_us),
                    bandwidth: Mbps::new(all_to_all.bandwidth_mbps),
                })
            }
        };
        Ok(profile)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllToAll {
    pub rtt: Microsecs,
    pub bandwidth: Mbps,
}

impl AllToAll {
    pub fn ideal_latency(&self, size: Bytes) -> Nanosecs {
        let rtt = Into::<Nanosecs>::into(self.rtt).into_inner() as f64;
        let bandwidth = Into::<BitsPerSec>::into(self.bandwidth).into_inner() as f64 / 1e9;
        let size = size.into_inner() as f64 * 8.0;
        let latency = rtt + size / bandwidth;
        Nanosecs::new(latency.round() as u64)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2PWorkload {
    pub src: WorkerId,
    pub dst: WorkerId,
    pub dscp: Dscp,
    pub delta_distribution_shape: DistShape,
    pub target_rate: Mbps,
    #[serde(default)]
    pub start: Secs,
    pub duration: Secs,
    pub nr_connections: usize,
}

impl TryFrom<proto::P2pWorkload> for P2PWorkload {
    type Error = Error;

    fn try_from(proto: proto::P2pWorkload) -> Result<Self, Self::Error> {
        let src = proto.src.ok_or(Error::MissingField("src"))?;
        let src = WorkerId::new(src);
        let dst = proto.dst.ok_or(Error::MissingField("dst"))?;
        let dst = WorkerId::new(dst);
        let dscp = proto.dscp.ok_or(Error::MissingField("dscp"))?;
        let dscp = Dscp::try_new(dscp).map_err(|_| Error::InvalidDscp(dscp))?;
        let delta_distribution_shape = proto
            .delta_distribution_shape
            .ok_or(Error::MissingField("delta_distribution_shape"))?
            .shape
            .ok_or(Error::MissingField("shape"))?;
        let delta_distribution_shape = match delta_distribution_shape {
            proto::dist_shape::Shape::Exponential(_) => DistShape::Exponential,
            proto::dist_shape::Shape::LogNormal(shape) => {
                DistShape::LogNormal { sigma: shape.sigma }
            }
        };
        let start = Secs::new(proto.start_secs);
        let target_rate = Mbps::new(proto.target_rate_mbps);
        let duration = Secs::new(proto.duration_secs);
        Ok(Self {
            src,
            dst,
            dscp,
            delta_distribution_shape,
            target_rate,
            start,
            duration,
            nr_connections: proto.nr_connections as usize,
        })
    }
}

impl From<P2PWorkload> for proto::P2pWorkload {
    fn from(value: P2PWorkload) -> Self {
        Self {
            src: Some(value.src.into_inner()),
            dst: Some(value.dst.into_inner()),
            dscp: Some(value.dscp.into_inner()),
            delta_distribution_shape: Some(proto::DistShape {
                shape: match value.delta_distribution_shape {
                    DistShape::Exponential => Some(proto::dist_shape::Shape::Exponential(
                        proto::ExponentialShape {},
                    )),
                    DistShape::LogNormal { sigma } => {
                        Some(proto::dist_shape::Shape::LogNormal(proto::LogNormalShape {
                            sigma,
                        }))
                    }
                    _ => unimplemented!(),
                },
            }),
            start_secs: value.start.into_inner(),
            target_rate_mbps: value.target_rate.into_inner(),
            duration_secs: value.duration.into_inner(),
            nr_connections: value.nr_connections as u32,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingRequest {
    pub src: WorkerId,
    pub dst: WorkerId,
}

impl TryFrom<proto::PingRequest> for PingRequest {
    type Error = Error;

    fn try_from(proto: proto::PingRequest) -> Result<Self, Self::Error> {
        let src = proto.src.ok_or(Error::MissingField("src"))?;
        let dst = proto.dst.ok_or(Error::MissingField("dst"))?;
        Ok(Self {
            src: WorkerId::new(src),
            dst: WorkerId::new(dst),
        })
    }
}

impl From<PingRequest> for proto::PingRequest {
    fn from(value: PingRequest) -> Self {
        Self {
            src: Some(value.src.into_inner()),
            dst: Some(value.dst.into_inner()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingResponse {
    pub times: Vec<Microsecs>,
}

impl TryFrom<proto::PingResponse> for PingResponse {
    type Error = Error;

    fn try_from(value: proto::PingResponse) -> Result<Self, Self::Error> {
        let times = value
            .times_us
            .into_iter()
            .map(Microsecs::new)
            .collect::<Vec<_>>();
        Ok(Self { times })
    }
}

impl From<PingResponse> for proto::PingResponse {
    fn from(value: PingResponse) -> Self {
        Self {
            times_us: value.times.into_iter().map(|t| t.into_inner()).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_profile_computes_ideal_correctly() {
        let profile = NetworkProfile::AllToAll(AllToAll {
            rtt: Microsecs::new(100),
            bandwidth: Mbps::new(10_000),
        });
        let size = Bytes::new(1_000_000_000);
        assert_eq!(profile.ideal_latency(size), Nanosecs::new(800_100_000))
    }
}
