use std::path::PathBuf;

use clap::Parser;
use rldb::{server::Server, telemetry::initialize_jaeger_subscriber};

#[derive(Debug, Parser)]
#[command(name = "rldb-server")]
#[command(about = "rldb-server tcp server", long_about = None)]
struct Cli {
    #[arg(long)]
    config_path: PathBuf,
    #[arg(short, long, default_value = "false")]
    tracing_jaeger: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    if args.tracing_jaeger {
        initialize_jaeger_subscriber("http://localhost:4317/v1/traces");
    } else {
        tracing_subscriber::fmt::init();
    }

    let mut server = Server::from_config(args.config_path).await?;
    server.run(tokio::signal::ctrl_c()).await?;

    Ok(())
}
