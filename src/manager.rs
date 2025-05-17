use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::task;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::proto::emu_manager_server::EmuManager;
use crate::proto::emu_worker_client::EmuWorkerClient;
use crate::worker::{WorkerAddress, WorkerId, WorkerRegistration};
use crate::{proto, PingRequest, RunInput};

#[derive(Debug, Default)]
pub struct Manager {
    wid2addr: DashMap<WorkerId, WorkerAddress>,
    wid2client: DashMap<WorkerId, EmuWorkerClient<Channel>>,
    running: AtomicBool,
}

#[tonic::async_trait]
impl EmuManager for Manager {
    async fn register_worker(
        &self,
        registration: Request<proto::WorkerRegistration>,
    ) -> Result<Response<()>, Status> {
        let proto = registration.into_inner();
        let reg =
            WorkerRegistration::try_from(proto).map_err(|e| Status::from_error(Box::new(e)))?;
        self.wid2addr.insert(reg.id, reg.address);
        let client = EmuWorkerClient::connect(format!("http://{}", reg.address.control_addr()))
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;
        self.wid2client.insert(reg.id, client);
        Ok(Response::new(()))
    }

    async fn check(&self, _: Request<()>) -> Result<Response<proto::CheckResponse>, Status> {
        let mut n = 0;
        for entry in self.wid2client.iter() {
            let mut client = entry.value().clone();
            if client.check(()).await.is_ok() {
                n += 1;
            }
        }
        Ok(Response::new(proto::CheckResponse {
            nr_workers: Some(n),
        }))
    }

    async fn run(&self, request: Request<proto::RunInput>) -> Result<Response<()>, Status> {
        if self
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(Status::failed_precondition(
                "workload already running; call stop before running again",
            ));
        }

        let result: Result<(), Status> = async {
            let spec = RunInput::try_from(request.into_inner())
                .map_err(|e| Status::from_error(Box::new(e)))?;
            self.introduce_peers_to_workers().await?;
            self.run_workers(spec).await
        }
        .await;

        self.running.store(false, Ordering::SeqCst);

        result?;
        Ok(Response::new(()))
    }

    async fn stop(&self, _: Request<()>) -> Result<Response<()>, Status> {
        self.stop_workers().await?;
        self.running.store(false, Ordering::SeqCst);
        Ok(Response::new(()))
    }

    async fn ping(
        &self,
        request: Request<proto::PingRequest>,
    ) -> Result<Response<proto::PingResponse>, Status> {
        let ping = PingRequest::try_from(request.into_inner())
            .map_err(|e| Status::from_error(Box::new(e)))?;
        self.introduce_peers_to_workers().await?;
        let mut client = self
            .wid2client
            .get(&ping.src)
            .ok_or_else(|| Status::not_found("worker not found"))?
            .clone();
        client.ping(Request::new(ping.into())).await
    }
}

impl Manager {
    async fn introduce_peers_to_workers(&self) -> Result<(), Status> {
        let worker_address_map = {
            let workers = self
                .wid2addr
                .iter()
                .map(|entry| {
                    let (wid, addr) = entry.pair();
                    let ip_address = addr.ip_address.to_string();
                    let control_port = addr.control_port as u32;
                    let data_port = addr.data_port as u32;
                    (
                        wid.into_inner(),
                        proto::WorkerAddress {
                            ip_address,
                            control_port,
                            data_port,
                        },
                    )
                })
                .collect();
            proto::WorkerAddressMap { workers }
        };
        let mut handles = Vec::new();
        for entry in self.wid2client.iter() {
            let mut client = entry.value().clone();
            let worker_address_map = worker_address_map.clone();
            let handle = task::spawn(async move {
                client
                    .introduce_peers(Request::new(worker_address_map))
                    .await
                    .map_err(|e| Status::from_error(Box::new(e)))?;
                Result::<_, Status>::Ok(())
            });
            handles.push(handle);
        }
        for handle in handles {
            handle
                .await
                .map_err(|e| Status::from_error(Box::new(e)))??;
        }
        Ok(())
    }

    async fn run_workers(&self, input: RunInput) -> Result<(), Status> {
        let mut handles = Vec::new();
        for entry in self.wid2client.iter() {
            let mut client = entry.value().clone();
            let spec = input.clone();
            let handle = task::spawn(async move {
                client
                    .run(Request::new(spec.into()))
                    .await
                    .map_err(|e| Status::from_error(Box::new(e)))?;
                Result::<_, Status>::Ok(())
            });
            handles.push(handle);
        }
        for handle in handles {
            handle
                .await
                .map_err(|e| Status::from_error(Box::new(e)))??;
        }
        Ok(())
    }

    async fn stop_workers(&self) -> Result<(), Status> {
        let mut handles = Vec::new();
        for entry in self.wid2client.iter() {
            let mut client = entry.value().clone();
            let handle = task::spawn(async move {
                client
                    .stop(Request::new(()))
                    .await
                    .map_err(|e| Status::from_error(Box::new(e)))?;
                Result::<_, Status>::Ok(())
            });
            handles.push(handle);
        }
        for handle in handles {
            handle
                .await
                .map_err(|e| Status::from_error(Box::new(e)))??;
        }
        Ok(())
    }
}
