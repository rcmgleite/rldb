use clap::{Parser, Subcommand};
use rldb::client::DbClient;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Parser)]
#[command(name = "rldb-client")]
#[command(about = "rldb-client tcp client", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command()]
    Ping {
        #[arg(short)]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Ping { port } => {
            let mut client = DbClient::connect(format!("127.0.0.1:{}", port)).await?;
            let response = client.ping().await?;

            let mut stdout = tokio::io::stdout();
            stdout.write_all(&response).await?;
        }
    }

    Ok(())
}
