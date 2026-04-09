//! Handle commands

use std::str;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use tracing::debug;

use crate::database::{Database, Direction};
use crate::resp;

/// Dispatch the command
pub fn dispatch(db: &Database, s: &[Bytes]) -> Result<Bytes> {
    debug!("dispatch {}", &format_byte_array(s));
    let Some((command, arguments)) = s.split_first() else {
        bail!("No command in dispatch");
    };
    let command = command.to_ascii_uppercase();

    match (command.as_slice(), arguments) {
        // Ping command
        (b"PING", []) => Ok(Bytes::from("+PONG\r\n")),

        // Echo command 
        (b"ECHO", [arg]) => Ok(resp::encode_bulk_string(arg.clone())),

        // Get command - simple version
        (b"GET", [key]) => match db.get(key)? {
            Some(value) => Ok(resp::encode_bulk_string(value)),
            None => Ok(resp::encode_null_bulk_string()),
        },

        // Set command - simple version
        (b"SET", [key, value]) => {
            db.set(key, value, None);
            Ok(resp::encode_simple_string(Bytes::from("OK")))
        }

        // Set command - with expiration time
        (b"SET", [key, value, tag, units]) => {
            let units: u64 = str::from_utf8(units)?.parse()?;
            let duration = match tag.to_ascii_uppercase().as_slice() {
                b"EX" => Duration::from_secs(units),
                b"PX" => Duration::from_millis(units),
                _ => bail!("Unrecognized tag in set command"),
            };
            let expiry = SystemTime::now() + duration;

            db.set(key, value, Some(expiry));
            Ok(resp::encode_simple_string(Bytes::from("OK")))
        }

        // Push a string to the right of a list
        (b"RPUSH", [key, value]) => {
            let n = db.push(Direction::Right, key, value.clone())?;
            Ok(resp::encode_integer(n))
        }

        // Command - dummy implementation so redis-cli does not trip and fall
        (b"COMMAND", _) => Ok(resp::encode_simple_string(Bytes::from("OK"))),

        // Unknown command or wrong number of arguments
        _ => Err(anyhow!("Unknown command in dispatch")),
    }
}

fn format_byte_array(s: &[Bytes]) -> String {
    // let v: Vec<String> = s.iter().map(|bs| String::from_utf8_lossy(bs)).collect();
    let mut v: Vec<String> = Vec::new();
    for b in s {
        v.push(String::from_utf8_lossy(b).to_string());
    }
    v.join(" ")
}
