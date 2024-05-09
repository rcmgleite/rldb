use clap::Parser;
use rldb::{server::run, storage_engine::in_memory::InMemory};
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(name = "rldb-server")]
#[command(about = "rldb-server tcp server", long_about = None)]
struct Cli {
    #[arg(short)]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();

    let storage_engine = InMemory::default();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await?;
    run(listener, storage_engine).await?;

    Ok(())
}
