//! Handle commands

use anyhow::{Result, anyhow, bail};
use bytes::Bytes;


/// Dispatch the command
pub fn dispatch(s: &[Bytes]) -> Result<Bytes> {
    let Some(command) = s.first() else {
        bail!("No command in dispatch");
    };
    if command.eq_ignore_ascii_case(b"PING") {
        Ok(Bytes::from("+PONG\r\n"))
    } else {
        Err(anyhow!("Unknown command in dispatch"))
    }
}


