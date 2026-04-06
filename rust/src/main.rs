//! Http server

use anyhow::Result;
use tokio::net::TcpListener;
use tokio::io::BufStream;

use codecrafters_redis::handler::handle;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Listening {listener:?}");
    
    loop {
        let (stream, _) = listener.accept().await?;
        let mut stream = BufStream::new(stream);

        tokio::spawn(async move {
            let r = handle(&mut stream).await;
            println!("{r:?}");
        });
    }
}


