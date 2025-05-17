use std::{
    fs,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    process,
};

use crate::{
    proto::{
        emu_manager_client::EmuManagerClient, emu_manager_server::EmuManagerServer,
        emu_worker_server::EmuWorkerServer, RunInput, WorkerAddress, WorkerRegistration,
    },
    Manager, Worker, WorkerId,
};
use clap::Subcommand;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    signal, task,
    time::{self, Duration},
};
use tonic::{transport::Server, Code, Request, Status};

const DEFAULT_BUCKETS: &str = "1.0,2.0,4.0,6.0,8.0,10.0,15.0,20.0,\
                               25.0,30.0,35.0,40.0,45.0,50.0,60.0,70.0,\
                               80.0,90.0,100.0,1000.0";

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
        advertise_ip: IpAddr,

        #[arg(short, long)]
        control_port: u16,

        #[arg(short, long)]
        data_port: u16,

        #[arg(short, long)]
        manager_addr: SocketAddr,

        #[arg(long, default_value = "0.0.0.0:9000")]
        metrics_addr: SocketAddr,

        #[arg(short, long, value_delimiter=',', default_value = DEFAULT_BUCKETS)]
        buckets: Vec<f64>,
    },
    Check {
        #[arg(short, long)]
        manager_addr: SocketAddr,
    },
    Run {
        #[arg(short, long)]
        spec: PathBuf,

        #[arg(short, long)]
        profile: PathBuf,

        #[arg(short, long)]
        manager_addr: SocketAddr,
    },
    Stop {
        #[arg(short, long)]
        manager_addr: SocketAddr,
    },
    Ping {
        #[arg(short, long)]
        manager_addr: SocketAddr,

        #[arg(short, long)]
        src: WorkerId,

        #[arg(short, long)]
        dst: WorkerId,
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
                advertise_ip,
                control_port,
                data_port,
                manager_addr,
                metrics_addr,
                buckets,
            } => {
                init_metrics(metrics_addr, &buckets)?;
                // Start the gRPC control server.
                let control_server = task::spawn(async move {
                    let worker = Worker::new(id);
                    let addr = format!("0.0.0.0:{}", control_port).parse()?;
                    Server::builder()
                        .add_service(EmuWorkerServer::new(worker))
                        .serve(addr)
                        .await?;
                    anyhow::Result::<()>::Ok(())
                });
                // Start the TCP data server.
                let data_server = task::spawn(data_server(data_port));
                time::sleep(Duration::from_millis(10)).await; // wait for servers to startup
                register_worker(id, advertise_ip, control_port, data_port, manager_addr).await?;

                // Join tasks and propagate errors
                let (control_result, data_result) = tokio::join!(control_server, data_server);
                control_result??;
                data_result??;
            }
            Command::Check { manager_addr } => {
                let nr_workers = check(manager_addr).await?;
                println!("Manager is up, and {} workers are up.", nr_workers);
            }
            Command::Run {
                spec,
                profile,
                manager_addr,
            } => {
                let spec: crate::RunSpecification =
                    serde_json::from_str(&fs::read_to_string(spec)?)?;
                let profile: crate::NetworkProfile =
                    serde_json::from_str(&fs::read_to_string(profile)?)?;
                let input = crate::RunInput { spec, profile };
                run(input, manager_addr).await?;
            }
            Command::Stop { manager_addr } => {
                let mut client =
                    EmuManagerClient::connect(format!("http://{}", manager_addr)).await?;
                client.stop(Request::new(())).await?;
            }
            Command::Ping {
                manager_addr,
                src,
                dst,
            } => {
                let mut client =
                    EmuManagerClient::connect(format!("http://{}", manager_addr)).await?;
                let ping = crate::PingRequest { src, dst };
                let response = client.ping(Request::new(ping.into())).await?;
                let response = response.into_inner();
                println!("Ping microseconds: {:?}", response.times_us);
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

pub async fn run(input: crate::RunInput, manager_addr: SocketAddr) -> anyhow::Result<()> {
    let mut client = EmuManagerClient::connect(format!("http://{}", manager_addr)).await?;
    let mut client_ = client.clone();
    task::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => println!("Ctrl-C received, aborting..."),
            Err(e) => {
                println!("Failed to listen for Ctrl-C: {e}");
                process::abort();
            }
        }

        match client_.stop(Request::new(())).await {
            Ok(_) => process::exit(0),
            Err(e) => {
                println!("Failed to stop worker: {e}");
                process::abort();
            }
        }
    });
    let input: RunInput = input.into();
    let request = Request::new(input);
    let _response = client.run(request).await?;
    Ok(())
}

async fn register_worker(
    id: WorkerId,
    advertise_ip: IpAddr,
    control_port: u16,
    data_port: u16,
    manager_addr: SocketAddr,
) -> Result<(), tonic::Status> {
    const MAX_ATTEMPTS: usize = 6;
    const RETRY_DELAY: Duration = Duration::from_secs(10);
    for _ in 0..MAX_ATTEMPTS {
        match try_register_worker(id, advertise_ip, control_port, data_port, manager_addr).await {
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
    advertise_ip: IpAddr,
    control_port: u16,
    data_port: u16,
    manager_addr: SocketAddr,
) -> Result<(), tonic::Status> {
    let mut client = EmuManagerClient::connect(format!("http://{}", manager_addr))
        .await
        .map_err(|e| Status::from_error(Box::new(e)))?;
    let address = WorkerAddress {
        ip_address: advertise_ip.to_string(),
        control_port: control_port as u32,
        data_port: data_port as u32,
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

async fn data_server(port: u16) -> anyhow::Result<()> {
    let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    
    // Create a channel for error propagation
    let (tx, mut rx) = tokio::sync::mpsc::channel::<anyhow::Error>(100);
    
    // Spawn a task to accept connections
    let accept_task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(socket, addr).await {
                            let _ = tx.send(e).await;
                        }
                    });
                }
                Err(e) => {
                    let _ = tx.send(anyhow::anyhow!("Accept error: {}", e)).await;
                    break;
                }
            }
        }
    });
    
    // Wait for the first error to occur
    if let Some(err) = rx.recv().await {
        accept_task.abort(); // Stop accepting new connections
        return Err(err);
    }
    
    Ok(())
}

async fn handle_connection(mut socket: tokio::net::TcpStream, addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let mut prefix = [0u8; std::mem::size_of::<u64>()];
    // Read the message size.
    socket
        .read_exact(&mut prefix)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read size prefix from {}: {}", addr, e))?;
    let size = u64::from_le_bytes(prefix) as usize;

    // Read the data in chunks.
    let mut bytes_remaining = size;
    let mut buf = [0u8; 4096];
    while bytes_remaining > 0 {
        let bytes_to_read = std::cmp::min(bytes_remaining, buf.len());
        socket.read_exact(&mut buf[..bytes_to_read]).await
            .map_err(|e| anyhow::anyhow!("Error reading data from {}: {}", addr, e))?;
        bytes_remaining -= bytes_to_read;
    }
    
    socket.write_all(&[0; 1]).await
        .map_err(|e| anyhow::anyhow!("Error writing ACK to {}: {}", addr, e))?;
    Ok(())
}
