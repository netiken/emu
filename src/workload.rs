use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    distribution::{DistShape, Ecdf, EcdfError},
    proto,
    units::{Dscp, Mbps, Nanosecs, Secs},
    Error, WorkerId,
};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RunSpecification {
    pub p2p_workloads: Vec<P2PWorkload>,
    pub size_distributions: HashMap<String, Arc<Ecdf>>,
}

impl TryFrom<proto::RunSpecification> for RunSpecification {
    type Error = Error;

    fn try_from(proto: proto::RunSpecification) -> Result<Self, Self::Error> {
        let p2p_workloads = proto
            .p2p_workloads
            .into_iter()
            .map(P2PWorkload::try_from)
            .collect::<Result<_, _>>()?;
        let size_distributions = proto
            .size_distributions
            .into_iter()
            .map(|(k, v)| Result::<_, Self::Error>::Ok((k, Arc::new(Ecdf::try_from(v)?))))
            .collect::<Result<HashMap<_, _>, _>>()?;
        Ok(Self {
            p2p_workloads,
            size_distributions,
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
            size_distributions: spec
                .size_distributions
                .into_iter()
                .map(|(k, v)| (k, proto::Ecdf::from((*v).clone())))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2PWorkload {
    pub src: WorkerId,
    pub dst: WorkerId,
    pub dscp: Dscp,
    pub rate: Mbps,
    pub size_distribution_name: String,
    pub delta_distribution_shape: DistShape,
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
        let rate = Mbps::new(proto.rate_mbps);
        let size_distribution_name = proto.size_distribution_name;
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
        let duration = Secs::new(proto.duration_secs);
        Ok(Self {
            src,
            dst,
            dscp,
            rate,
            size_distribution_name,
            delta_distribution_shape,
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
            rate_mbps: value.rate.into_inner(),
            size_distribution_name: value.size_distribution_name,
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
            duration_secs: value.duration.into_inner(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunResults {
    pub samples: Vec<Sample>,
}

impl RunResults {
    pub fn extend(&mut self, other: Self) {
        self.samples.extend(other.samples);
    }
}

impl TryFrom<proto::RunResults> for RunResults {
    type Error = Error;

    fn try_from(proto: proto::RunResults) -> Result<Self, Self::Error> {
        let samples = proto
            .samples
            .into_iter()
            .map(Sample::try_from)
            .collect::<Result<_, _>>()?;
        Ok(Self { samples })
    }
}

impl From<RunResults> for proto::RunResults {
    fn from(value: RunResults) -> Self {
        Self {
            samples: value.samples.into_iter().map(proto::Sample::from).collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Sample {
    pub src: WorkerId,
    pub dst: WorkerId,
    pub dscp: Dscp,
    pub latency: Nanosecs,
}

impl TryFrom<proto::Sample> for Sample {
    type Error = Error;

    fn try_from(proto: proto::Sample) -> Result<Self, Self::Error> {
        let src = proto.src.ok_or(Error::MissingField("src"))?;
        let src = WorkerId::new(src);
        let dst = proto.dst.ok_or(Error::MissingField("dst"))?;
        let dst = WorkerId::new(dst);
        let dscp = proto.dscp.ok_or(Error::MissingField("dscp"))?;
        let dscp = Dscp::try_new(dscp).map_err(|_| Error::InvalidDscp(dscp))?;
        let latency_ns = proto.latency_ns.ok_or(Error::MissingField("latency_ns"))?;
        let latency = Nanosecs::new(latency_ns);
        Ok(Self {
            src,
            dst,
            dscp,
            latency,
        })
    }
}

impl From<Sample> for proto::Sample {
    fn from(value: Sample) -> Self {
        Self {
            src: Some(value.src.into_inner()),
            dst: Some(value.dst.into_inner()),
            dscp: Some(value.dscp.into_inner()),
            latency_ns: Some(value.latency.into_inner()),
        }
    }
}
