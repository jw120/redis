//! Handler for TCP/IP requests

use anyhow::{Result, anyhow, bail};
use tracing::{info, trace};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};


/// Dummy handler function
pub async fn handle<S>(mut stream: S) -> Result<()> where S: AsyncBufReadExt + AsyncWriteExt + Unpin,
{
    let commands = read_command(&mut stream).await?;
    for command in &commands {
        let response = dispatch_command(command)?;
        stream.write_all(&response).await?;
        stream.flush().await?;
        info!("Wrote '{}'",  String::from_utf8_lossy(&response));        
    }
    Ok(())
}

/// Read a command from the stream
async fn read_command<S>(stream: &mut S) -> Result<Vec<Vec<u8>>> where S: AsyncBufReadExt + Unpin,
{
    let elements = read_number_line(stream, '*').await?;
    
    let mut commands: Vec<Vec<u8>> = Vec::new();
    for _ in 0..elements {
        let v = read_bulk_string(stream).await?;
        commands.push(v);
    }

    trace!("returning {} commands", commands.len());
    Ok(commands)
    
}

/// Read a bulk string from the stream
async fn read_bulk_string<S>(stream: &mut S) -> Result<Vec<u8>> where S: AsyncBufReadExt +Unpin {

    let size: usize = read_number_line(stream, '$').await?;

    let mut buf: Vec<u8> = vec![0; size];
    stream.read_exact(&mut buf).await?;
    trace!("returning '{:?}'", String::from_utf8_lossy(&buf));
    Ok(buf)
}

/// Read a line of format char + unsigned integer + \r\n
async fn read_number_line<S>(stream: &mut S, c: char) -> Result<usize> where S: AsyncBufReadExt + Unpin {

    let mut line_buffer = String::new();
    stream.read_line(&mut line_buffer).await?;
    let Some(without_prefix) = line_buffer.strip_prefix(c) else {
        bail!("Expected {c}, got '{}'", line_buffer);
    };
    let Some(without_suffix) = without_prefix.strip_suffix("\r\n") else {
        bail!("Expected line end, got '{}'", line_buffer);
    };
    let n: usize = without_suffix.parse()?;
    trace!("returning {n}");
    Ok(n)
}

/// Dispatch the command
fn dispatch_command(s: &[u8]) -> Result<Vec<u8>> {
    match s {
        b"PING" => Ok(Vec::from(b"+PONG\r\n")),
        _ => Err(anyhow!("Unknown command '{}'",  String::from_utf8_lossy(s))),
    }
}


