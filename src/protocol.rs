/* 
Protocol

Inspired from https://redis.io/docs/latest/develop/reference/protocol-spec


| Prefix | Type              | Example                            | Meaning                             |
| ------ | ----------------- | ---------------------------------- | ----------------------------------- |
| `+`    | **Simple string** | `+OK\r\n`                          | short success message               |
| `-`    | **Error**         | `-ERR not found\r\n`               | indicates failure                   |
| `$`    | **Bulk string**   | `$5\r\nhello\r\n`                  | binary-safe data with length prefix |
| `*`    | **Array**         | `*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n` | array of elements (commands)        |



One way:
*2\r\n$3\r\nGET\r\n$16\r\nkey000000000001\r\n

- *2 → array with 2 items
- $3 → first item is 3 bytes → "GET"
- $16 → second item is 16 bytes → "key000000000001"

The server will reply:

- $5\r\nhello\r\n 

or

- $-1\r\n  # null, value not found


 */

 #[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
}

use std::io::{self, BufRead};

pub fn parse_resp<R: BufRead>(reader: &mut R) -> io::Result<RespValue> {
    let mut first = [0u8; 1];
    reader.read_exact(&mut first)?;
    match first[0] {
        b'+' => {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            Ok(RespValue::SimpleString(line.trim_end().to_string()))
        }
        b'-' => {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            Ok(RespValue::Error(line.trim_end().to_string()))
        }
        b':' => {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            let n: i64 = line.trim_end().parse().unwrap_or(0);
            Ok(RespValue::Integer(n))
        }
        b'$' => {
            let mut len_line = String::new();
            reader.read_line(&mut len_line)?;
            let len: i64 = len_line.trim_end().parse().unwrap_or(-1);
            if len < 0 {
                return Ok(RespValue::BulkString(Vec::new()));
            }
            let mut buf = vec![0u8; len as usize];
            reader.read_exact(&mut buf)?;
            let mut crlf = [0u8; 2];
            reader.read_exact(&mut crlf)?;
            Ok(RespValue::BulkString(buf))
        }
        b'*' => {
            let mut len_line = String::new();
            reader.read_line(&mut len_line)?;
            let count: i64 = len_line.trim_end().parse().unwrap_or(0);
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let v = parse_resp(reader)?;
                items.push(v);
            }
            Ok(RespValue::Array(items))
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown RESP type")),
    }
}

use std::io::Write;

pub fn write_resp<W: Write>(writer: &mut W, value: &RespValue) -> io::Result<()> {
    match value {
        RespValue::SimpleString(s) => {
            write!(writer, "+{}\r\n", s)?;
        }
        RespValue::Error(s) => {
            write!(writer, "-{}\r\n", s)?;
        }
        RespValue::Integer(i) => {
            write!(writer, ":{}\r\n", i)?;
        }
        RespValue::BulkString(data) => {
            write!(writer, "${}\r\n", data.len())?;
            writer.write_all(data)?;
            write!(writer, "\r\n")?;
        }
        RespValue::Array(values) => {
            write!(writer, "*{}\r\n", values.len())?;
            for v in values {
                write_resp(writer, v)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_get_command() {
        let input = b"*2\r\n$3\r\nGET\r\n$15\r\nkey000000000001\r\n";
        let mut reader = io::BufReader::new(&input[..]);
        let val = parse_resp(&mut reader).unwrap();
        assert_eq!(
            val,
            RespValue::Array(vec![
                RespValue::BulkString(b"GET".to_vec()),
                RespValue::BulkString(b"key000000000001".to_vec())
            ])
        );
    }

    #[test]
    fn test_write_bulk_string() {
        let mut out = Vec::new();
        let val = RespValue::BulkString(b"hello".to_vec());
        write_resp(&mut out, &val).unwrap();
        assert_eq!(out, b"$5\r\nhello\r\n");
    }
}


