use dashmap::DashMap;
use infinity_sampler::SamplingReservoir;
use rand::prelude::*;
use tokio::{
    task,
    time::{self, Duration, Instant},
};
use tonic::{
    transport::{Channel, Endpoint},
    Request, Response, Status,
};

use crate::{
    distribution::{DistShape, Ecdf, GenF64},
    proto::{self, emu_worker_client::EmuWorkerClient, emu_worker_server::EmuWorker},
    units::{BitsPerSec, Bytes, Dscp, Mbps, Nanosecs, Secs},
    util::http::HttpConnector,
    Error, RunResults, RunSpecification, Sample,
};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

const NR_PATH_SAMPLES: usize = 2_usize.pow(14);

#[nutype::nutype(derive(
    Debug,
    Display,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    FromStr,
    Serialize,
    Deserialize
))]
pub struct WorkerId(u32);

#[derive(Debug)]
pub struct Worker {
    id: WorkerId,
    wid2addr: DashMap<WorkerId, WorkerAddress>,
}

impl Worker {
    pub fn new(id: WorkerId) -> Self {
        Self {
            id,
            wid2addr: DashMap::new(),
        }
    }
}

#[tonic::async_trait]
impl EmuWorker for Worker {
    async fn introduce_peers(
        &self,
        request: Request<proto::WorkerAddressMap>,
    ) -> Result<Response<()>, tonic::Status> {
        for (wid, addr) in request.into_inner().workers {
            let wid = WorkerId::new(wid);
            let addr =
                WorkerAddress::try_from(addr).map_err(|e| Status::from_error(Box::new(e)))?;
            self.wid2addr.insert(wid, addr);
        }
        Ok(Response::new(()))
    }

    async fn run(
        &self,
        request: Request<proto::RunSpecification>,
    ) -> Result<Response<proto::RunResults>, tonic::Status> {
        let spec = RunSpecification::try_from(request.into_inner())
            .map_err(|e| Status::from_error(Box::new(e)))?;
        let mut handles = Vec::new();
        for workload in spec.p2p_workloads {
            if workload.src != self.id {
                continue;
            }
            // Prepare the point-to-point context.
            let address = self
                .wid2addr
                .get(&workload.dst)
                .ok_or_else(|| {
                    Status::not_found(format!(
                        "Worker {} does not know about worker {}",
                        self.id, workload.dst
                    ))
                })?
                .value()
                .to_owned();
            let sizes = spec
                .size_distributions
                .get(&workload.size_distribution_name)
                .ok_or_else(|| {
                    Status::not_found(format!(
                        "Size distribution {} not found",
                        workload.size_distribution_name
                    ))
                })?;
            let ctx = P2PContext {
                src: self.id,
                dst: workload.dst,
                dst_addr: address,
                dscp: workload.dscp,
                rate: workload.rate,
                sizes: Arc::clone(sizes),
                deltas: workload.delta_distribution_shape,
                duration: workload.duration,
            };
            // Initiate the point-to-point workload.
            let handle = task::spawn(run_p2p_workload(ctx));
            handles.push(handle)
        }
        let mut results = RunResults::default();
        for handle in handles {
            let results_ = handle
                .await
                .map_err(|e| Status::from_error(Box::new(e)))??;
            results.extend(results_);
        }
        Ok(Response::new(results.into()))
    }

    async fn generic_rpc(
        &self,
        _: Request<proto::GenericRequestResponse>,
    ) -> Result<Response<proto::GenericRequestResponse>, tonic::Status> {
        let response = proto::GenericRequestResponse { data: Vec::new() };
        Ok(Response::new(response))
    }
}

// Runs a point-to-point workload in open-loop.
async fn run_p2p_workload(ctx: P2PContext) -> Result<RunResults, Status> {
    let client = ctx.connect().await?;
    let mut handles = Vec::new();
    let mut rng = StdRng::from_entropy();
    let deltas = ctx
        .delta_distribution()
        .map_err(|e| Status::invalid_argument(format!("invalid delta distribution: {}", e)))?;
    let mut reservoir = SamplingReservoir::<(Bytes, Nanosecs), NR_PATH_SAMPLES>::new();
    let duration = Duration::from_secs(ctx.duration.into_inner() as u64);
    let now = Instant::now();
    let mut cur = now;
    while cur - now < duration {
        // Generate the next RPC time.
        let delta = deltas.gen(&mut rng).round() as u64;
        cur += Duration::from_nanos(delta);
        // Generate the next RPC request payload.
        let size = ctx.sizes.sample(&mut rng).round() as usize;
        let mut data = vec![0; size];
        rng.fill(&mut data[..]);
        // Schedule the next RPC request.
        let mut client = client.clone();
        let handle = task::spawn(async move {
            let mut interval = time::interval(cur - now);
            interval.tick().await; // ticks immediately
            interval.tick().await; // ticks after `cur - now`
            let now = Instant::now();
            let _ = client
                .generic_rpc(Request::new(proto::GenericRequestResponse { data }))
                .await;
            let latency = now.elapsed();
            (
                Bytes::new(size as u64),
                Nanosecs::new(latency.as_nanos() as u64),
            )
        });
        handles.push(handle);
    }
    for handle in handles {
        let pair = handle.await.map_err(|e| Status::from_error(Box::new(e)))?;
        reservoir.sample(pair);
    }
    Ok(RunResults {
        samples: reservoir
            .into_ordered_iter()
            .map(|(size, latency)| Sample {
                src: ctx.src,
                dst: ctx.dst,
                dscp: ctx.dscp,
                size,
                latency,
            })
            .collect(),
    })
}

#[derive(Debug, Clone)]
struct P2PContext {
    src: WorkerId,
    dst: WorkerId,
    dst_addr: WorkerAddress,
    dscp: Dscp,
    rate: Mbps,
    sizes: Arc<Ecdf>,
    deltas: DistShape,
    duration: Secs,
}

impl P2PContext {
    fn delta_distribution(&self) -> anyhow::Result<Box<dyn GenF64 + Send>> {
        let mean_size = Bytes::new(self.sizes.mean().round() as u64);
        let mean_delta = mean_delta_for_rate(self.rate, mean_size);
        self.deltas.to_kind(mean_delta.into_inner() as f64).to_gen()
    }

    async fn connect(&self) -> Result<EmuWorkerClient<Channel>, Status> {
        let addr = self.dst_addr.socket_addr();
        let endpoint = format!("http://{}", addr);
        let tos = self.dscp.into_inner() << 2;
        let mut connector = HttpConnector::new();
        connector.set_tos(Some(tos));
        let channel = Endpoint::try_from(endpoint)
            .map_err(|e| Status::from_error(Box::new(e)))?
            .connect_with_connector(connector)
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;
        Ok(EmuWorkerClient::new(channel))
    }
}

fn mean_delta_for_rate(rate: impl Into<BitsPerSec>, mean_size: impl Into<Bytes>) -> Nanosecs {
    let rate = rate.into().into_inner() as f64;
    let mean_size = mean_size.into().into_inner() as f64;
    let mean_delta = (rate / 8.0 / mean_size).recip() * 1e9;
    Nanosecs::new(mean_delta as u64)
}

#[derive(Debug, Clone, Copy)]
pub struct WorkerRegistration {
    pub id: WorkerId,
    pub address: WorkerAddress,
}

impl TryFrom<proto::WorkerRegistration> for WorkerRegistration {
    type Error = Error;

    fn try_from(proto: proto::WorkerRegistration) -> Result<Self, Self::Error> {
        let id = proto.id.ok_or(Error::MissingField("id"))?;
        let id = WorkerId::new(id);
        let address = proto.address.ok_or(Error::MissingField("address"))?;
        let address = WorkerAddress::try_from(address)?;
        Ok(Self { id, address })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WorkerAddress {
    inner: SocketAddr,
}

impl WorkerAddress {
    pub fn socket_addr(&self) -> SocketAddr {
        self.inner
    }
}

impl TryFrom<proto::WorkerAddress> for WorkerAddress {
    type Error = Error;

    fn try_from(proto: proto::WorkerAddress) -> Result<Self, Self::Error> {
        let ip_address: IpAddr = proto.ip_address.parse()?;
        let port = proto.port as u16;
        Ok(Self {
            inner: SocketAddr::new(ip_address, port),
        })
    }
}