//! "Resp" format handling

use anyhow::{Result, bail};
use bytes::{Bytes, BytesMut, BufMut};

// To do - consider negative numbers

/// Encode as a simple string
pub fn encode_simple_string(s: Bytes) -> Bytes {
    let mut buffer = BytesMut::new();
    buffer.put_u8(b'+');
    buffer.put(s);
    buffer.put_slice(b"\r\n");
    buffer.freeze()
}

/// Encode as a bulk string
pub fn encode_bulk_string(s: Bytes) -> Bytes {
    let mut buffer = BytesMut::new();
    buffer.put_u8(b'$');
    buffer.put_slice(s.len().to_string().as_bytes());
    buffer.put_slice(b"\r\n");
    buffer.put(s);
    buffer.put_slice(b"\r\n");
    buffer.freeze()
}

/// Encode a null bulk string
pub fn encode_null_bulk_string() -> Bytes {
    Bytes::from("$-1\r\n")
}

/// Parse a bulk string: $ + number + \r\n + string + \r\n
/// Returns string found and residual bytes on success
pub fn parse_bulk_string(s: Bytes) -> Result<(Bytes, Bytes)> {
    let (n, t) = parse_char_integer(b'$', s)?;
    parse_sized_bulk_string(n, t)
}

/// Parse the sized part of a bulk string: string + \r\n
/// Returns string found and residual bytes on success
pub fn parse_sized_bulk_string(n: usize, s: Bytes) -> Result<(Bytes, Bytes)> {
    let string = s.slice(..n);
    let newline = s.slice(n..n+2);
    if newline != "\r\n" {
      bail!("Missing newline after string in parse_bulk_string");
    }
    let residual = s.slice(n+2..);
    Ok((string, residual))
}

/// Parse an integer.
/// Returns the integer and residual bytes on success
fn parse_integer(s: Bytes) -> Result<(usize, Bytes)> {
    let mut i: usize = 0;
    let mut x: usize = 0;
    while let Some(c) = s.get(i) {
        if *c >= b'0' && *c <= b'9' {
            i += 1;
            x = x * 10 + (*c - b'0') as usize;
        } else {
            break;
        }
    }
    if i == 0 {
        bail!("Number not found in parse_integer");
    }
    Ok((x, s.slice(i..)))
}


/// Parse a line of format: char + usize + \r\n
/// Returns number and residual bytes on success
pub fn parse_char_integer(prefix: u8, s: Bytes) -> Result<(usize, Bytes)> {
    let Some(c) = s.first() else {
        bail!("Empty in parse_char_integer");
    };
    if *c != prefix {
        bail!("Wrong prefix in parse_char_integer");
    }
    let (n, remaining) = parse_integer(s.slice(1..))?;
    let Some(residual) = remaining.strip_prefix(b"\r\n") else {
        bail!("Missing line ending in parse_char_integer");
    };
    Ok((n, s.slice_ref(residual)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_simple_string() {
        assert_eq!(encode_simple_string(Bytes::from("cat")), Bytes::from("+cat\r\n"));
    }
    
    #[test]
    fn test_encode_bulk_string() {
        assert_eq!(encode_bulk_string(Bytes::from("cat")), Bytes::from("$3\r\ncat\r\n"));
    }

    #[test]
    fn test_parse_bulk_string() {
        assert_eq!(parse_bulk_string(Bytes::from("$3\r\ncat\r\ndog")).unwrap(), (Bytes::from("cat"), Bytes::from("dog")));
    }

    #[test]
    fn test_round_trip_bulk_string() {
        let s = Bytes::from("abc23 a1!");
        assert_eq!(parse_bulk_string(encode_bulk_string(s.clone())).unwrap(), (s, Bytes::new()));
    }

    
    #[test]
    fn test_parse_char_integer() {
        assert_eq!(parse_char_integer(b'$', Bytes::from("$123\r\n@")).unwrap(), (123, Bytes::from("@")));
        assert_eq!(parse_char_integer(b'!', Bytes::from("!1234\r\n")).unwrap(), (1234, Bytes::new()));
    }
    
    #[test]
    fn test_parse_integer() {
        assert_eq!(parse_integer(Bytes::from("123x")).unwrap(), (123, Bytes::from("x")));
        assert_eq!(parse_integer(Bytes::from("1234")).unwrap(), (1234, Bytes::new()));
    }

}
