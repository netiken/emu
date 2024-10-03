use dashmap::DashMap;
use tokio::task;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::proto::emu_manager_server::EmuManager;
use crate::proto::emu_worker_client::EmuWorkerClient;
use crate::worker::{WorkerAddress, WorkerId, WorkerRegistration};
use crate::{proto, RunSpecification};

#[derive(Debug, Default)]
pub struct Manager {
    wid2addr: DashMap<WorkerId, WorkerAddress>,
    wid2client: DashMap<WorkerId, EmuWorkerClient<Channel>>,
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
        let client = EmuWorkerClient::connect(format!("http://{}", reg.address.socket_addr()))
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

    async fn run(&self, request: Request<proto::RunSpecification>) -> Result<Response<()>, Status> {
        let spec = RunSpecification::try_from(request.into_inner())
            .map_err(|e| Status::from_error(Box::new(e)))?;
        self.introduce_peers_to_workers().await?;
        self.run_workers(spec).await?;
        Ok(Response::new(()))
    }

    async fn stop(&self, _: Request<()>) -> Result<Response<()>, Status> {
        self.stop_workers().await?;
        Ok(Response::new(()))
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
                    let ip_address = addr.socket_addr().ip().to_string();
                    let port = addr.socket_addr().port() as u32;
                    (wid.into_inner(), proto::WorkerAddress { ip_address, port })
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

    async fn run_workers(&self, spec: RunSpecification) -> Result<(), Status> {
        let mut handles = Vec::new();
        for entry in self.wid2client.iter() {
            let mut client = entry.value().clone();
            let spec = spec.clone();
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
