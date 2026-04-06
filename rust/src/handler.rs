//! Handler for TCP/IP requests

use anyhow::{Result, bail};

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};


/// Dummy handler function
pub async fn handle(stream: impl AsyncBufRead + Unpin) -> Result<()> {
    let _ = read_command(stream).await?;
    // let mut line_buffer = String::new();

    // loop {
    //     line_buffer.clear();
    //     stream.read_line(&mut line_buffer).await?;
    //     let trimmed: &str = line_buffer.trim();
        
    //     if trimmed.is_empty() {
    //         break;
    //     }
    //     println!("got line: '{trimmed}'");
    // }

    Ok(())
}

/// Read a command from the stream
async fn read_command(mut stream: impl AsyncBufReadExt + Unpin) -> Result<Vec<Vec<u8>>> {
    // First line of command is *N\r\m where N is the number of elements
    let mut line_buffer = String::new();
    stream.read_line(&mut line_buffer).await?;
    let Some(without_prefix) = line_buffer.strip_prefix('*') else {
        bail!("Expected *, got '{}'", line_buffer);
    };
    let Some(without_suffix) = without_prefix.strip_suffix("\r\n") else {
        bail!("Expected line end, got '{}'", line_buffer);
    };
    let elements: usize = without_suffix.parse()?;
    println!("Found {elements} elements");
    
    // Read the elements
    let mut commands: Vec<Vec<u8>> = Vec::new();
    for _ in 0..elements {
        let v = read_bulk_string(&mut stream).await?;
        commands.push(v);
    }

    Ok(commands)
    
}

/// Read a bulk string from the stream
async fn read_bulk_string(mut stream: impl AsyncBufReadExt + Unpin) -> Result<Vec<u8>> {
     // First line of command is $N\r\m where N is the number of bytes
    let mut line_buffer = String::new();
    stream.read_line(&mut line_buffer).await?;
    let Some(without_prefix) = line_buffer.strip_prefix('$') else {
        bail!("Expected *, got '{}'", line_buffer);
    };
    let Some(without_suffix) = without_prefix.strip_suffix("\r\n") else {
        bail!("Expected line end, got '{}'", line_buffer);
    };
    let size: usize = without_suffix.parse()?;
    println!("Found {size} size");

    let mut buf: Vec<u8> = vec![0; size];
    stream.read_exact(&mut buf).await?;
    println!("String {:?}", String::from_utf8_lossy(&buf));
    Ok(buf)
}
