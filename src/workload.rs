use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::{
    distribution::{DistShape, Ecdf, EcdfError},
    proto,
    units::{Dscp, Mbps, Secs},
    Error, WorkerId,
};

// gRPC limits the maximum messages size to 4MB.
pub const SZ_GRPC_MAX: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSpecification {
    pub p2p_workloads: Vec<P2PWorkload>,
    pub size_distribution: Arc<Ecdf>,
    pub probe_rate: Mbps,
    pub probe_duration: Secs,
}

impl RunSpecification {
    /// Gets the minimum number of nr workers necessary for this run specification
    pub fn get_nr_workers(&self) -> Result<usize, Error> {
        let mut unique_nr_workers = HashSet::<WorkerId>::new();
        self.p2p_workloads.iter().for_each(|workload| { 
            unique_nr_workers.insert(workload.src);
            unique_nr_workers.insert(workload.dst);
        });
        Ok(unique_nr_workers.len())
    }

    /// Gets the duration of this experiment
    pub fn get_duration(&self) -> Result<Secs, Error> {
        Ok(self.p2p_workloads.iter().map(|workload| workload.duration).max().unwrap())
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
        let probe_rate = Mbps::new(proto.probe_rate_mbps);
        let probe_duration = Secs::new(proto.probe_duration_secs);
        Ok(Self {
            p2p_workloads,
            size_distribution,
            probe_rate,
            probe_duration,
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
            probe_rate_mbps: spec.probe_rate.into_inner(),
            probe_duration_secs: spec.probe_duration.into_inner(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2PWorkload {
    pub src: WorkerId,
    pub dst: WorkerId,
    pub dscp: Dscp,
    pub delta_distribution_shape: DistShape,
    pub target_rate: Mbps,
    pub duration: Secs,
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
        let target_rate = Mbps::new(proto.target_rate_mbps);
        let duration = Secs::new(proto.duration_secs);
        Ok(Self {
            src,
            dst,
            dscp,
            delta_distribution_shape,
            target_rate,
            duration,
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
            target_rate_mbps: value.target_rate.into_inner(),
            duration_secs: value.duration.into_inner(),
        }
    }
}
