#![allow(non_snake_case)]

use dashmap::DashMap;
use derivative::Derivative;
use metrics::Histogram;
use rand::prelude::*;
use tokio::{
    task,
    time::{Duration, Instant},
};
use tonic::{
    transport::{Channel, Endpoint},
    Request, Response, Status,
};

use crate::{
    distribution::{DistShape, Ecdf, GenF64, SampleWithBucket},
    proto::{self, emu_worker_client::EmuWorkerClient, emu_worker_server::EmuWorker},
    units::{BitsPerSec, Bytes, Dscp, Mbps, Nanosecs, Secs},
    util::http::HttpConnector,
    Error, RunSpecification,
};
use std::{
    collections::HashMap,
    mem::MaybeUninit,
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
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
    is_stopped: Arc<AtomicBool>,
}

impl Worker {
    pub fn new(id: WorkerId) -> Self {
        Self {
            id,
            wid2addr: DashMap::new(),
            is_stopped: Arc::new(AtomicBool::new(true)),
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

    async fn check(&self, _: Request<()>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn stop(&self, _: Request<()>) -> Result<Response<()>, Status> {
        self.is_stopped.store(true, Ordering::Relaxed);
        Ok(Response::new(()))
    }

    async fn run(
        &self,
        request: Request<proto::RunSpecification>,
    ) -> Result<Response<()>, tonic::Status> {
        self.is_stopped.store(false, Ordering::Relaxed);
        let spec = RunSpecification::try_from(request.into_inner())
            .map_err(|e| Status::from_error(Box::new(e)))?;
        let mut handles = Vec::new();
        let mut histograms = HashMap::<Dscp, Histogram>::new();
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
            let histogram = histograms.entry(workload.dscp).or_insert_with(
                || metrics::histogram!("slowdown", "dscp" => workload.dscp.to_string()),
            );
            let ctx = P2PContext {
                src: self.id,
                dst: workload.dst,
                client: connect(address, workload.dscp).await?,
                sizes: Arc::clone(&spec.size_distribution),
                deltas: workload.delta_distribution_shape,
                target_rate: workload.target_rate,
                duration: workload.duration,
                probe_rate: workload.probe_rate,
                probe_duration: workload.probe_duration,
                histogram: histogram.clone(),
                should_stop: Arc::clone(&self.is_stopped),
            };
            // Initiate the point-to-point workload.
            let handle = task::spawn(run_p2p(ctx));
            handles.push(handle)
        }
        for handle in handles {
            handle
                .await
                .map_err(|e| Status::from_error(Box::new(e)))??;
        }
        Ok(Response::new(()))
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
async fn run_p2p(ctx: P2PContext) -> Result<(), Status> {
    let mut rng = StdRng::from_entropy();
    let bucket2min = ctx.probe(&mut rng).await?;
    ctx.work(&mut rng, &bucket2min).await
}

fn mk_uninit_bytes(size: usize) -> Vec<u8> {
    let mut vec = Vec::<MaybeUninit<u8>>::with_capacity(size);
    unsafe {
        vec.set_len(size);
        std::mem::transmute::<Vec<MaybeUninit<u8>>, Vec<u8>>(vec)
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
struct P2PContext {
    src: WorkerId,
    dst: WorkerId,
    client: EmuWorkerClient<Channel>,
    sizes: Arc<Ecdf>,
    deltas: DistShape,
    target_rate: Mbps,
    duration: Secs,
    probe_rate: Mbps,
    probe_duration: Secs,
    #[derivative(Debug = "ignore")]
    histogram: Histogram,
    should_stop: Arc<AtomicBool>,
}

impl P2PContext {
    async fn probe(&self, rng: &mut StdRng) -> Result<HashMap<usize, Duration>, Status> {
        let mut handles = Vec::new();
        let deltas = self
            .probe_delta_distribution()
            .map_err(|e| Status::invalid_argument(format!("invalid delta distribution: {}", e)))?;
        let duration = Duration::from_secs(self.probe_duration.into_inner() as u64);
        let mut now = Instant::now();
        let end = now + duration;
        while now < end && !self.should_stop() {
            // Generate the next RPC request payload.
            let SampleWithBucket {
                bucket,
                sample: size,
            } = Distribution::<SampleWithBucket>::sample(&*self.sizes, rng);
            let data = mk_uninit_bytes(size as usize);

            // Wait until the next RPC time.
            let delta = deltas.gen(rng).round() as u64;
            let timer = async_timer::new_timer(Duration::from_nanos(delta));
            timer.await;

            // Start the next RPC.
            let mut client = self.client.clone();
            handles.push(task::spawn(async move {
                let now = Instant::now();
                client
                    .generic_rpc(Request::new(proto::GenericRequestResponse { data }))
                    .await
                    .expect("Failed to send RPC");
                (bucket, now.elapsed())
            }));
            now = Instant::now();
        }
        let mut bucket2min = HashMap::new();
        for handle in handles {
            let (bucket, elapsed) = handle.await.map_err(|e| Status::from_error(Box::new(e)))?;
            let min = bucket2min.entry(bucket).or_insert(elapsed);
            *min = std::cmp::min(*min, elapsed);
        }
        Ok(bucket2min)
    }

    async fn work(
        &self,
        rng: &mut StdRng,
        bucket2min: &HashMap<usize, Duration>,
    ) -> Result<(), Status> {
        let deltas = self
            .target_delta_distribution()
            .map_err(|e| Status::invalid_argument(format!("invalid delta distribution: {}", e)))?;
        let duration = Duration::from_secs(self.duration.into_inner() as u64);
        let mut now = Instant::now();
        let end = now + duration;
        while now < end && !self.should_stop() {
            // Generate the next RPC request payload.
            let SampleWithBucket {
                bucket,
                sample: size,
            } = Distribution::<SampleWithBucket>::sample(&*self.sizes, rng);
            let data = mk_uninit_bytes(size as usize);
            let min_latency = *bucket2min.get(&bucket).ok_or_else(|| {
                Status::failed_precondition(format!(
                    "Unable to find min latency for bucket {bucket}. Use a longer probe duration."
                ))
            })?;

            // Wait until the next RPC time.
            let delta = deltas.gen(rng).round() as u64;
            let timer = async_timer::new_timer(Duration::from_nanos(delta));
            timer.await;

            // Start the next RPC.
            let mut client = self.client.clone();
            let histogram = self.histogram.clone();
            task::spawn(async move {
                let now = Instant::now();
                client
                    .generic_rpc(Request::new(proto::GenericRequestResponse { data }))
                    .await
                    .expect("Failed to send RPC");
                let latency = now.elapsed();
                let slowdown = latency.as_nanos() as f64 / min_latency.as_nanos() as f64;
                histogram.record(slowdown);
            });
            now = Instant::now();
        }
        Ok(())
    }

    fn target_delta_distribution(&self) -> anyhow::Result<Box<dyn GenF64 + Send>> {
        let mean_size = Bytes::new(self.sizes.mean().round() as u64);
        let mean_delta = mean_delta_for_rate(self.target_rate, mean_size);
        self.deltas.to_kind(mean_delta.into_inner() as f64).to_gen()
    }

    fn probe_delta_distribution(&self) -> anyhow::Result<Box<dyn GenF64 + Send>> {
        let mean_size = Bytes::new(self.sizes.mean().round() as u64);
        let mean_delta = mean_delta_for_rate(self.probe_rate, mean_size);
        DistShape::Constant
            .to_kind(mean_delta.into_inner() as f64)
            .to_gen()
    }

    #[inline]
    fn should_stop(&self) -> bool {
        self.should_stop.load(Ordering::Relaxed)
    }
}

async fn connect(addr: WorkerAddress, dscp: Dscp) -> Result<EmuWorkerClient<Channel>, Status> {
    let addr = addr.socket_addr();
    let endpoint = format!("http://{}", addr);
    let tos = dscp.into_inner() << 2;
    let mut connector = HttpConnector::new();
    connector.set_tos(Some(tos));
    let channel = Endpoint::try_from(endpoint)
        .map_err(|e| Status::from_error(Box::new(e)))?
        .connect_with_connector(connector)
        .await
        .map_err(|e| Status::from_error(Box::new(e)))?;
    Ok(EmuWorkerClient::new(channel))
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
