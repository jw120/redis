//! Handle commands

use std::str::FromStr;
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
        (b"ECHO", [arg]) => Ok(resp::encode_bulk_string(arg)),

        // Get command - simple version
        (b"GET", [key]) => match db.get(key)? {
            Some(value) => Ok(resp::encode_bulk_string(&value)),
            None => Ok(resp::encode_null_bulk_string()),
        },

        // Set command - simple version
        (b"SET", [key, value]) => {
            db.set(key, value, None);
            Ok(resp::encode_simple_string(&Bytes::from("OK")))
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
            Ok(resp::encode_simple_string(&Bytes::from("OK")))
        }

        // Delete a value
        (b"DEL", [key]) => {
            db.del(key)?;
            Ok(resp::encode_simple_string(&Bytes::from("OK")))
        }

        // Push a string to the right of a list
        (b"RPUSH", [key, args @ ..]) => {
            let n = db.push(Direction::Right, key, args)?;
            Ok(resp::encode_integer(n))
        }

        // Push a string to the left of a list
        (b"LPUSH", [key, args @ ..]) => {
            let n = db.push(Direction::Left, key, args)?;
            Ok(resp::encode_integer(n))
        }

        // Pop one element from the left of the list
        (b"LPOP", [key]) => {
            let array = db.pop(key, 1)?;
            match &array[..] {
                [] => Ok(resp::encode_null_bulk_string()),
                [s] => Ok(resp::encode_bulk_string(s)),
                _ => Err(anyhow!("Internal failure"))
            }
        }

        // Pop elements from the left of the list
        (b"LPOP", [key, count]) => {
            let count = bytes_to_int::<usize>(count)?;
            let array = db.pop(key, count)?;
            Ok(resp::encode_array_of_strings(&array))
        }

        // Return an array range
        (b"LRANGE", [key, start, stop]) => {
            let start = bytes_to_int::<i64>(start)?;
            let stop = bytes_to_int::<i64>(stop)?;
            let array = db.lrange(key, start, stop)?;
            Ok(resp::encode_array_of_strings(&array))
        }

        // Return length of a list
        (b"LLEN", [key]) => {
            let n = db.length(key)?;
            Ok(resp::encode_integer(n))
        }

        // Command - dummy implementation so redis-cli does not trip and fall
        (b"COMMAND", _) => Ok(resp::encode_simple_string(&Bytes::from("OK"))),

        // Unknown command or wrong number of arguments
        _ => Err(anyhow!("Unknown commmand in dispatch: '{}'", &String::from_utf8_lossy(&command)))
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

fn bytes_to_int<T: FromStr>(b: &Bytes) -> Result<T> {
    let Ok(s) = str::from_utf8(b) else {
        bail!("ERR found non-utf8 characters instead of an integer");
    };
    let Ok(i) = s.parse() else {
        bail!("ERR value is not an integer or is out of range");
    };
    Ok(i)
}
