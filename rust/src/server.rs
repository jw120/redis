//! TCP/IP server

use anyhow::{bail, Result};
use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpListener;
use tracing::{debug, info, trace};

use crate::commands::dispatch;
use crate::database::Database;
use crate::resp::{parse_char_integer, parse_sized_bulk_string};

/// Start and our TCP/IP server
pub async fn run(addr: &str) -> Result<()> {
    let db = Database::initialise();
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {listener:?}");

    loop {
        let (stream, _) = listener.accept().await?;
        let mut buf_stream = BufStream::new(stream);
        let db_handle = db.handle();

        tokio::spawn(async move {
            let result = handle(&db_handle, &mut buf_stream).await;
            if let Err(e) = result {
                println!("Connection closed on error: {e}");
            }
        });
    }
}

/// Handle each stream
async fn handle<S>(db: &Database, mut stream: S) -> Result<()>
where
    S: AsyncBufReadExt + AsyncWriteExt + Unpin,
{
    while let Some(command) = read_command(&mut stream).await? {
        let response = dispatch(db, &command)?;
        stream.write_all(&response).await?;
        stream.flush().await?;
        debug!("Wrote '{}'", String::from_utf8_lossy(&response));
    }
    Ok(())
}

/// Read a command from the stream
/// Returns None if reached EOF before anything is read
async fn read_command<S>(stream: &mut S) -> Result<Option<Vec<Bytes>>>
where
    S: AsyncBufReadExt + Unpin,
{
    let Some(n) = read_char_integer(b'*', stream).await? else {
        return Ok(None);
    };

    let mut words: Vec<Bytes> = Vec::new();
    for _ in 0..n {
        words.push(read_bulk_string(stream).await?);
    }

    trace!("returning {} words", words.len());
    Ok(Some(words))
}

/// Read a bulk string from the stream
async fn read_bulk_string<S>(stream: &mut S) -> Result<Bytes>
where
    S: AsyncBufReadExt + Unpin,
{
    let Some(n) = read_char_integer(b'$', stream).await? else {
        bail!("Unexpected EOF in read_bulk_string");
    };

    let mut buffer: Vec<u8> = vec![0; n + 2]; // space for the trailing \r\n
    stream.read_exact(&mut buffer).await?;

    let (string, residual) = parse_sized_bulk_string(n, Bytes::from(buffer))?;
    if !residual.is_empty() {
        bail!("Excess bytes in read_bulk_string");
    }

    Ok(string)
}

/// Read a line with a char + integer + \r\n
/// Returns None if reached EOF
async fn read_char_integer<S>(prefix: u8, stream: &mut S) -> Result<Option<usize>>
where
    S: AsyncBufReadExt + Unpin,
{
    let mut line_buffer = String::new();

    let bytes_read = stream.read_line(&mut line_buffer).await?;
    if bytes_read == 0 {
        return Ok(None); // EOF
    }
    let (n, residual) = parse_char_integer(prefix, Bytes::from(line_buffer))?;
    if !residual.is_empty() {
        bail!("Extra found after elements in read_char_integer");
    }
    Ok(Some(n))
}
