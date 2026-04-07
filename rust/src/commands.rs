//! Handle commands

use anyhow::{Result, anyhow, bail};
use bytes::Bytes;

use crate::db::Database;
use crate::resp::{encode_bulk_string, encode_null_bulk_string};

/// Dispatch the command
pub fn dispatch(db: &Database, s: &[Bytes]) -> Result<Bytes> {
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
    } else {
        Err(anyhow!("Unknown command in dispatch"))
    }
}


