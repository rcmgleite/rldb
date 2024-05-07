use tokio::{
    io::{AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream},
};
use tracing::{event, instrument, Level};

use crate::cmd::Command;

struct Listener {
    listener: TcpListener,
}

impl Listener {
    async fn run(&self) -> anyhow::Result<()> {
        event!(Level::INFO, "Listener started");

        loop {
            let (tcp_stream, _) = self.listener.accept().await?;
            event!(
                Level::DEBUG,
                "accepted new tcp connection: {:?}",
                tcp_stream
            );
            tokio::spawn(handle_connection(tcp_stream));
        }
    }
}

#[instrument(level = "debug")]
async fn handle_connection(tcp_stream: TcpStream) -> anyhow::Result<()> {
    let mut buffered_tcp_stream = BufStream::new(tcp_stream);

    loop {
        let cmd = Command::try_from_buf_read(&mut buffered_tcp_stream).await?;
        cmd.execute(&mut buffered_tcp_stream).await?;

        buffered_tcp_stream.flush().await?;
    }
}

pub async fn run(listener: TcpListener) -> anyhow::Result<()> {
    let server = Listener { listener };
    server.run().await
}
