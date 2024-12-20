#![allow(non_snake_case)]

use dashmap::DashMap;
use derivative::Derivative;
use metrics::Histogram;
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
    units::{BitsPerSec, Bytes, Dscp, Mbps, Microsecs, Nanosecs, Secs},
    util::http::HttpConnector,
    Error, NetworkProfile, PingRequest, PingResponse, RunInput,
};
use std::{
    collections::HashMap,
    mem::MaybeUninit,
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

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
    generation: Arc<AtomicU32>,
}

impl Worker {
    pub fn new(id: WorkerId) -> Self {
        Self {
            id,
            wid2addr: DashMap::new(),
            generation: Arc::new(AtomicU32::new(0)),
        }
    }

    fn addr_or_err(&self, wid: WorkerId) -> Result<WorkerAddress, Status> {
        self.wid2addr.get(&wid).map(|a| *a).ok_or_else(|| {
            Status::not_found(format!(
                "Worker {} does not know about worker {}",
                self.id, wid
            ))
        })
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

    async fn check(&self, _: Request<()>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn stop(&self, _: Request<()>) -> Result<Response<()>, Status> {
        self.generation.fetch_add(1, Ordering::Relaxed);
        Ok(Response::new(()))
    }

    async fn run(&self, request: Request<proto::RunInput>) -> Result<Response<()>, Status> {
        let RunInput { spec, profile } = RunInput::try_from(request.into_inner())
            .map_err(|e| Status::from_error(Box::new(e)))?;
        let mut handles = Vec::new();
        let mut histograms = HashMap::<Dscp, Arc<DashMap<usize, Histogram>>>::new();
        for workload in spec.p2p_workloads {
            if workload.src != self.id {
                continue;
            }
            // Prepare the point-to-point context.
            let address = self.addr_or_err(workload.dst)?;
            let histograms = histograms
                .entry(workload.dscp)
                .or_insert_with(|| Arc::new(DashMap::new()));
            let rate_per_connection = divvy_rate(workload.target_rate, workload.nr_connections);
            for _ in 0..workload.nr_connections {
                let ctx = RunContext {
                    client: connect(address, Some(workload.dscp)).await?,
                    dscp: workload.dscp,
                    sizes: Arc::clone(&spec.size_distribution),
                    deltas: workload.delta_distribution_shape,
                    target_rate: rate_per_connection,
                    start: workload.start,
                    duration: workload.duration,
                    histograms: Arc::clone(histograms),
                    network_profile: profile.clone(),
                    output_buckets: spec.output_buckets.clone(),
                    generation: self.generation.load(Ordering::Relaxed),
                    cur_generation: Arc::clone(&self.generation),
                };
                // Initiate the workload.
                let handle = task::spawn(async move { ctx.run().await });
                handles.push(handle)
            }
        }
        for handle in handles {
            handle
                .await
                .map_err(|e| Status::from_error(Box::new(e)))??;
        }
        Ok(Response::new(()))
    }

    async fn ping(
        &self,
        request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        let ping = PingRequest::try_from(request.into_inner())
            .map_err(|e| Status::from_error(Box::new(e)))?;
        let address = self.addr_or_err(ping.dst)?;
        let mut client = connect(address, None).await?;
        let mut times = Vec::new();
        for _ in 0..10 {
            let now = Instant::now();
            client
                .generic_rpc(Request::new(proto::GenericRequestResponse {
                    data: Vec::new(),
                }))
                .await
                .expect("Failed to send RPC");
            let latency = Microsecs::new(now.elapsed().as_micros() as u64);
            times.push(latency);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let response = PingResponse { times };
        Ok(Response::new(response.into()))
    }

    async fn generic_rpc(
        &self,
        _: Request<proto::GenericRequestResponse>,
    ) -> Result<Response<proto::GenericRequestResponse>, tonic::Status> {
        let response = proto::GenericRequestResponse { data: Vec::new() };
        Ok(Response::new(response))
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
struct RunContext {
    client: EmuWorkerClient<Channel>,
    dscp: Dscp,
    sizes: Arc<Ecdf>,
    deltas: DistShape,
    target_rate: Mbps,
    start: Secs,
    duration: Secs,
    #[derivative(Debug = "ignore")]
    histograms: Arc<DashMap<usize, Histogram>>,
    network_profile: NetworkProfile,
    output_buckets: Vec<Bytes>,
    generation: u32,
    cur_generation: Arc<AtomicU32>,
}

impl RunContext {
    async fn run(&self) -> Result<(), Status> {
        // Wait until the start time.
        time::sleep(Duration::from_secs(self.start.into_inner() as u64)).await;

        // Intialize the workload.
        let mut rng = StdRng::from_entropy();
        let deltas = delta_distribution(&self.sizes, self.deltas, self.target_rate)
            .map_err(|e| Status::invalid_argument(format!("invalid delta distribution: {}", e)))?;
        let duration = Duration::from_secs(self.duration.into_inner() as u64);
        let mut now = Instant::now();
        let end = now + duration;

        // Generate RPCs.
        while now < end && !self.should_stop() {
            // Generate the next RPC request payload.
            let size = Distribution::<f64>::sample(&*self.sizes, &mut rng);
            let data = mk_uninit_bytes(size as usize);
            let ideal_latency = self.network_profile.ideal_latency(Bytes::new(size as u64));

            // Wait until the next RPC time.
            let delta = deltas.gen(&mut rng).round() as u64;
            let timer = async_timer::new_timer(Duration::from_nanos(delta));
            timer.await;

            // Start the next RPC.
            let mut client = self.client.clone();
            let size = size as u64;
            let output_bucket = self
                .output_buckets
                .iter()
                .enumerate()
                .find_map(|(i, b)| (size <= b.into_inner()).then_some(i))
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "Unable to find output bucket for size {size}."
                    ))
                })?;
            let histogram = self
                .histograms
                .entry(output_bucket)
                .or_insert_with(|| {
                    metrics::histogram!(
                        "slowdown",
                        "dscp" => self.dscp.to_string(),
                        "bucket" => output_bucket.to_string()
                    )
                })
                .clone();
            task::spawn(async move {
                let now = Instant::now();
                client
                    .generic_rpc(Request::new(proto::GenericRequestResponse { data }))
                    .await
                    .expect("Failed to send RPC");
                let latency = now.elapsed();
                let slowdown = latency.as_nanos() as f64 / ideal_latency.into_inner() as f64;
                histogram.record(slowdown);
            });
            now = Instant::now();
        }
        Ok(())
    }

    #[inline]
    fn should_stop(&self) -> bool {
        self.generation != self.cur_generation.load(Ordering::Relaxed)
    }
}

async fn connect(
    addr: WorkerAddress,
    dscp: Option<Dscp>,
) -> Result<EmuWorkerClient<Channel>, Status> {
    let addr = addr.socket_addr();
    let endpoint = format!("http://{}", addr);
    let mut connector = HttpConnector::new();
    if let Some(dscp) = dscp {
        let tos = dscp.into_inner() << 2;
        connector.set_tos(Some(tos));
    }
    let channel = Endpoint::try_from(endpoint)
        .map_err(|e| Status::from_error(Box::new(e)))?
        .connect_with_connector(connector)
        .await
        .map_err(|e| Status::from_error(Box::new(e)))?;
    Ok(EmuWorkerClient::new(channel))
}

#[inline]
fn mk_uninit_bytes(size: usize) -> Vec<u8> {
    let mut vec = Vec::<MaybeUninit<u8>>::with_capacity(size);
    unsafe {
        vec.set_len(size);
        std::mem::transmute::<Vec<MaybeUninit<u8>>, Vec<u8>>(vec)
    }
}

fn delta_distribution(
    sizes: &Arc<Ecdf>,
    shape: DistShape,
    rate: Mbps,
) -> anyhow::Result<Box<dyn GenF64 + Send>> {
    let mean_size = Bytes::new(sizes.mean().round() as u64);
    let mean_delta = mean_delta_for_rate(rate, mean_size);
    shape.to_kind(mean_delta.into_inner() as f64).to_gen()
}

fn mean_delta_for_rate(rate: impl Into<BitsPerSec>, mean_size: impl Into<Bytes>) -> Nanosecs {
    let rate = rate.into().into_inner() as f64;
    let mean_size = mean_size.into().into_inner() as f64;
    let mean_delta = (rate / 8.0 / mean_size).recip() * 1e9;
    Nanosecs::new(mean_delta as u64)
}

fn divvy_rate(rate: Mbps, nr_connections: usize) -> Mbps {
    let rate = rate.into_inner() as f64;
    let nr_connections = nr_connections as f64;
    Mbps::new((rate / nr_connections).round() as u32)
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
