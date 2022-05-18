use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use kv::{CommandRequest, CommandResponse};
use tokio::net::TcpStream;
use tracing::info;


#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    
    let addr = "127.0.0.1:9527";
    // connect to the server
    let stream = TcpStream::connect(addr).await?;

    // Use AsyncProstStream to process TCP frame
    let mut client = AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();

    //generate a HSET command
    let cmd = CommandRequest::new_hset("table1", "hello", "world".into());

    //Send the command
    client.send(cmd).await?;
    if let Some(Ok(data)) = client.next().await{
        info!("Got response {:?}", data);
    }
    Ok(())

}