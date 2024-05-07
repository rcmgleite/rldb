use clap::Parser;
use rldb::server::run;
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

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await?;
    run(listener).await?;

    Ok(())
}
