use std::path::PathBuf;

use clap::Parser;
use rldb::server::Server;

#[derive(Debug, Parser)]
#[command(name = "rldb-server")]
#[command(about = "rldb-server tcp server", long_about = None)]
struct Cli {
    #[arg(long)]
    config_path: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();

    let mut server = Server::from_config(args.config_path).await?;
    server.run().await?;

    Ok(())
}
