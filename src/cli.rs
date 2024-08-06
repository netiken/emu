use std::{fs, net::SocketAddr, path::PathBuf};

use crate::{
    proto::{
        emu_manager_client::EmuManagerClient, emu_manager_server::EmuManagerServer,
        emu_worker_server::EmuWorkerServer, RunSpecification, WorkerAddress, WorkerRegistration,
    },
    Manager, Worker, WorkerId,
};
use clap::Subcommand;
use tokio::{
    task,
    time::{self, Duration},
};
use tonic::{transport::Server, Request};

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
    },
    Run {
        #[arg(short, long)]
        spec: PathBuf,

        #[arg(short, long)]
        manager_addr: SocketAddr,

        #[arg(short, long)]
        out: PathBuf,
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
            } => {
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
            Command::Run {
                spec,
                manager_addr,
                out,
            } => {
                let spec = fs::read_to_string(spec)?;
                let spec: crate::RunSpecification = serde_json::from_str(&spec)?;
                let results = run(spec, manager_addr).await?;
                let mut wtr = csv::Writer::from_path(&out)?;
                for record in results.samples {
                    wtr.serialize(record)?;
                }
                wtr.flush()?;
            }
        }
        Ok(())
    }
}

async fn run(
    spec: crate::RunSpecification,
    manager_addr: SocketAddr,
) -> anyhow::Result<crate::RunResults> {
    let mut client = EmuManagerClient::connect(format!("http://{}", manager_addr)).await?;
    let spec: RunSpecification = spec.into();
    let request = Request::new(spec);
    let response = client.run(request).await?;
    Ok(response.into_inner().try_into()?)
}

async fn register_worker(
    id: WorkerId,
    advertise_addr: SocketAddr,
    manager_addr: SocketAddr,
) -> anyhow::Result<()> {
    let mut client = EmuManagerClient::connect(format!("http://{}", manager_addr)).await?;
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
