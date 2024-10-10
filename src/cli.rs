use std::{fs, net::SocketAddr, path::PathBuf, process};

use crate::{
    proto::{
        emu_manager_client::EmuManagerClient, emu_manager_server::EmuManagerServer,
        emu_worker_server::EmuWorkerServer, RunSpecification, WorkerAddress, WorkerRegistration,
    },
    Manager, Worker, WorkerId,
};
use clap::Subcommand;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::{
    signal, task,
    time::{self, Duration},
};
use tonic::{transport::Server, Code, Request, Status};

const DEFAULT_BUCKETS: &[f64] = &[
    1.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0, 22.0, 24.0, 26.0, 28.0, 30.0,
    40.0, 50.0, 60.0, 70.0,
];

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    Manager {
        #[arg(short, long)]
        port: u16,
    },
    Worker {
        #[arg(short, long)]
        id: WorkerId,

        #[arg(short, long)]
        advertise_addr: SocketAddr,

        #[arg(short, long)]
        manager_addr: SocketAddr,

        #[arg(long, default_value = "0.0.0.0:9000")]
        metrics_addr: SocketAddr,
    },
    Check {
        #[arg(short, long)]
        manager_addr: SocketAddr,
    },
    Run {
        #[arg(short, long)]
        spec: PathBuf,

        #[arg(short, long)]
        manager_addr: SocketAddr,
    },
}

impl Command {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            Command::Manager { port } => {
                let manager = Manager::default();
                let addr = format!("0.0.0.0:{}", port).parse()?;
                Server::builder()
                    .add_service(EmuManagerServer::new(manager))
                    .serve(addr)
                    .await?;
            }
            Command::Worker {
                id,
                advertise_addr,
                manager_addr,
                metrics_addr,
            } => {
                init_metrics(metrics_addr, DEFAULT_BUCKETS)?;
                let handle = task::spawn(async move {
                    let worker = Worker::new(id);
                    let addr = format!("0.0.0.0:{}", advertise_addr.port()).parse()?;
                    Server::builder()
                        .add_service(EmuWorkerServer::new(worker))
                        .serve(addr)
                        .await?;
                    anyhow::Result::<()>::Ok(())
                });
                time::sleep(Duration::from_millis(10)).await; // wait for server startup
                register_worker(id, advertise_addr, manager_addr).await?;
                handle.await??;
            }
            Command::Check { manager_addr } => {
                let nr_workers = check(manager_addr).await?;
                println!("Manager is up, and {} workers are up.", nr_workers);
            }
            Command::Run { spec, manager_addr } => {
                let spec = fs::read_to_string(spec)?;
                let spec: crate::RunSpecification = serde_json::from_str(&spec)?;
                run(spec, manager_addr).await?;
            }
        }
        Ok(())
    }
}

pub async fn check(manager_addr: SocketAddr) -> anyhow::Result<usize> {
    let mut client = EmuManagerClient::connect(format!("http://{}", manager_addr)).await?;
    let response = client.check(Request::new(())).await?;
    Ok(response.get_ref().nr_workers.unwrap() as usize)
}

pub async fn run(spec: crate::RunSpecification, manager_addr: SocketAddr) -> anyhow::Result<()> {
    let mut client = EmuManagerClient::connect(format!("http://{}", manager_addr)).await?;
    let mut client_ = client.clone();
    task::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl-C");
        println!("Ctrl-C received, aborting...");
        client_
            .stop(Request::new(()))
            .await
            .expect("Failed to stop worker");
        process::abort();
    });
    let spec: RunSpecification = spec.into();
    let request = Request::new(spec);
    let _response = client.run(request).await?;
    Ok(())
}

async fn register_worker(
    id: WorkerId,
    advertise_addr: SocketAddr,
    manager_addr: SocketAddr,
) -> Result<(), tonic::Status> {
    const MAX_ATTEMPTS: usize = 6;
    const RETRY_DELAY: Duration = Duration::from_secs(10);
    for _ in 0..MAX_ATTEMPTS {
        match try_register_worker(id, advertise_addr, manager_addr).await {
            Ok(_) => {
                println!("Worker registered successfully.");
                return Ok(());
            }
            Err(e) if e.code() == Code::Unavailable => {
                println!(
                    "Manager unavailable, retrying in {} seconds.",
                    RETRY_DELAY.as_secs()
                );
                time::sleep(RETRY_DELAY).await;
            }
            e => return e,
        }
    }
    println!("Failed to register worker after {} attempts.", MAX_ATTEMPTS);
    Err(Status::unavailable("Manager unavailable"))
}

async fn try_register_worker(
    id: WorkerId,
    advertise_addr: SocketAddr,
    manager_addr: SocketAddr,
) -> Result<(), tonic::Status> {
    let mut client = EmuManagerClient::connect(format!("http://{}", manager_addr))
        .await
        .map_err(|e| Status::from_error(Box::new(e)))?;
    let address = WorkerAddress {
        ip_address: advertise_addr.ip().to_string(),
        port: advertise_addr.port() as u32,
    };
    let request = Request::new(WorkerRegistration {
        id: Some(id.into_inner()),
        address: Some(address),
    });
    client.register_worker(request).await?;
    Ok(())
}

fn init_metrics(addr: SocketAddr, buckets: &[f64]) -> anyhow::Result<()> {
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .set_buckets(buckets)?
        .install()?;
    Ok(())
}
