#![allow(non_snake_case)]

use dashmap::DashMap;
use derivative::Derivative;
use metrics::Histogram;
use rand::prelude::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
    task,
    time::{self, Duration, Instant},
};
use tonic::{Request, Response, Status};

use crate::{
    distribution::{DistShape, Ecdf, GenF64},
    proto::{self, emu_worker_server::EmuWorker},
    units::{BitsPerSec, Bytes, Dscp, Mbps, Microsecs, Nanosecs, Secs},
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
            let rate_per_connection = divvy_rate(workload.target_rate, workload.nr_workers);
            for _ in 0..workload.nr_workers {
                let ctx = RunContext {
                    addr: address.data_addr(),
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
        let mut times = Vec::new();
        for _ in 0..10 {
            let mut stream = TcpStream::connect(address.data_addr()).await?;
            let now = Instant::now();
            stream.write_all(&[0; 1]).await?;
            let mut ack = [0u8; 1];
            stream.read_exact(&mut ack).await?;
            let latency = Microsecs::new(now.elapsed().as_micros() as u64);
            times.push(latency);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let response = PingResponse { times };
        Ok(Response::new(response.into()))
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
struct RunContext {
    addr: SocketAddr,
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
            let addr = self.addr;
            let dscp = self.dscp;
            task::spawn(async move {
                let socket = TcpSocket::new_v4().expect("Failed to create socket");
                socket
                    .set_tos(dscp.into_inner() << 2)
                    .expect("Failed to set TOS");
                socket.set_reuseaddr(true).expect("Failed to set reuseaddr");
                let mut stream = socket.connect(addr).await.expect("Failed to connect");
                let now = Instant::now();
                stream
                    .write_all(&data)
                    .await
                    .expect("Failed to write to stream");
                let mut ack = [0u8; 1];
                stream
                    .read_exact(&mut ack)
                    .await
                    .expect("Failed to read ACK");
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

#[inline]
fn mk_uninit_bytes(size: usize) -> Vec<u8> {
    let sz_prefix = std::mem::size_of::<u64>();
    let sz_total = sz_prefix + size;
    let mut vec = Vec::<MaybeUninit<u8>>::with_capacity(sz_total);
    unsafe {
        vec.set_len(sz_total);
        let mut vec = std::mem::transmute::<Vec<MaybeUninit<u8>>, Vec<u8>>(vec);
        let prefix = (size as u64).to_le_bytes();
        vec[..sz_prefix].copy_from_slice(&prefix);
        vec
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
    pub ip_address: IpAddr,
    pub control_port: u16,
    pub data_port: u16,
}

impl WorkerAddress {
    pub fn control_addr(&self) -> SocketAddr {
        SocketAddr::new(self.ip_address, self.control_port)
    }

    pub fn data_addr(&self) -> SocketAddr {
        SocketAddr::new(self.ip_address, self.data_port)
    }
}

impl TryFrom<proto::WorkerAddress> for WorkerAddress {
    type Error = Error;

    fn try_from(proto: proto::WorkerAddress) -> Result<Self, Self::Error> {
        let ip_address: IpAddr = proto.ip_address.parse()?;
        let control_port = proto.control_port as u16;
        let data_port = proto.data_port as u16;
        Ok(Self {
            ip_address,
            control_port,
            data_port,
        })
    }
}
