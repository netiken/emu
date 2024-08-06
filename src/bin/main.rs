use clap::Parser;
use emu::cli::Command;

#[derive(Debug, Clone, Parser)]
struct Opt {
    #[command(subcommand)]
    command: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();
    opt.command.run().await
}
