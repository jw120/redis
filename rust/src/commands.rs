//! Handle commands

use anyhow::{Result, anyhow, bail};
use bytes::Bytes;
use tracing::debug;

use crate::database::Database;
use crate::resp::{encode_bulk_string, encode_null_bulk_string, encode_simple_string};

/// Dispatch the command
pub fn dispatch(db: &Database, s: &[Bytes]) -> Result<Bytes> {
    debug!("dispatch {}", &format_byte_array(s));
    let Some(command) = s.first() else {
        bail!("No command in dispatch");
    };
    if command.eq_ignore_ascii_case(b"PING") {
        if s.len() == 1 {
            Ok(Bytes::from("+PONG\r\n"))
        } else {
            Err(anyhow!("Expected no arguments for PING command"))
        } 
    } else if command.eq_ignore_ascii_case(b"ECHO") {
        if s.len() == 2 {
            Ok(encode_bulk_string(s[1].clone()))
        } else {
            Err(anyhow!("Expected one arguments for ECHO command"))
        } 
    } else if command.eq_ignore_ascii_case(b"GET") {
        if s.len() == 2 {
            match db.get(s[1].clone()) {
                Some(value) => Ok(encode_bulk_string(value)),
                None => Ok(encode_null_bulk_string()),
            }
        } else {
            Err(anyhow!("Expected one arguments for GET command"))
        } 
    } else if command.eq_ignore_ascii_case(b"SET") {
        if s.len() == 3 {
            db.set(s[1].clone(), s[2].clone());
            Ok(encode_simple_string(Bytes::from("OK")))
        } else {
            Err(anyhow!("Expected one arguments for GET command"))
        } 
    } else if command.eq_ignore_ascii_case(b"COMMAND"){
        // Just ignore this - produced from testing tool
        Ok(encode_simple_string(Bytes::from("OK")))
    } else {
        Err(anyhow!("Unknown command in dispatch"))
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
