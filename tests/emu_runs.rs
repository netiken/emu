use emu::{cli::Command, WorkerId};
use tokio::{
    task,
    time::{self, Duration},
};

const BASE_PORT: u16 = 50_000;
const NR_WORKERS: usize = 3;

#[tokio::test]
async fn emu_runs() -> anyhow::Result<()> {
    // Start the manager.
    let _manager = task::spawn(Command::Manager { port: BASE_PORT }.run());

    // Start some workers.
    let _workers = (0..NR_WORKERS)
        .map(|i| {
            task::spawn(
                Command::Worker {
                    id: WorkerId::new(i as u32),
                    advertise_addr: format!("127.0.0.1:{}", BASE_PORT + i as u16 + 1)
                        .parse()
                        .unwrap(),
                    manager_addr: format!("127.0.0.1:{BASE_PORT}").parse().unwrap(),
                }
                .run(),
            )
        })
        .collect::<Vec<_>>();

    // Wait for everything to start up.
    time::sleep(Duration::from_millis(100)).await;

    // Run a simple specification and output results.
    const MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");
    let spec = format!("{}/tests/spec.json", MANIFEST_DIR);
    let out = format!("{}/tests/results.csv", MANIFEST_DIR);
    Command::Run {
        spec: spec.into(),
        manager_addr: format!("127.0.0.1:{BASE_PORT}").parse().unwrap(),
        out: out.into(),
    }
    .run()
    .await?;
    Ok(())
}
