use bytes::Bytes;
use clap::{Parser, Subcommand};
use rldb::{
    client::{db_client::DbClient, Client},
    persistency::storage::Value,
};
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
    #[command()]
    Get {
        #[arg(short)]
        port: u16,

        #[arg(short)]
        key: Bytes,
    },
    #[command()]
    Put {
        #[arg(short)]
        port: u16,

        #[arg(short)]
        key: Bytes,

        #[arg(short)]
        value: Bytes,

        #[arg(short)]
        context: Option<String>,
    },
    #[command()]
    JoinCluster {
        #[arg(short)]
        port: u16,

        #[arg(long)]
        known_cluster_node: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();

    match args.command {
        Commands::Ping { port } => {
            let mut client = DbClient::new(format!("127.0.0.1:{}", port));
            client.connect().await?;
            let response = client.ping().await?;
            let mut stdout = tokio::io::stdout();
            let payload = serde_json::to_string(&response)?;
            stdout.write_all(payload.as_bytes()).await?;
        }
        Commands::Get { port, key } => {
            let mut client = DbClient::new(format!("127.0.0.1:{}", port));
            client.connect().await?;
            let response = client.get(key).await?;
            let mut stdout = tokio::io::stdout();
            let payload = serde_json::to_string(&response)?;
            stdout.write_all(payload.as_bytes()).await?;
        }
        Commands::Put {
            port,
            key,
            value,
            context,
        } => {
            let mut client = DbClient::new(format!("127.0.0.1:{}", port));
            client.connect().await?;
            // FIXME: The API has to receive the CRC instead of computing it...
            let value = Value::new_unchecked(value);
            let response = client.put(key, value, context, false).await?;
            let mut stdout = tokio::io::stdout();
            let payload = serde_json::to_string(&response)?;
            stdout.write_all(payload.as_bytes()).await?;
        }
        Commands::JoinCluster {
            port,
            known_cluster_node,
        } => {
            let mut client = DbClient::new(format!("127.0.0.1:{}", port));
            client.connect().await?;
            let response = client.join_cluster(known_cluster_node).await?;
            let mut stdout = tokio::io::stdout();
            let payload = serde_json::to_string(&response)?;
            stdout.write_all(payload.as_bytes()).await?;
        }
    }

    Ok(())
}
