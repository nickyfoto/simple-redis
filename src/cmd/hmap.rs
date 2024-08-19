use super::{
    extract_args, validate_command, CommandError, CommandExecutor, HGet, HGetAll, HSet, RESP_OK,
};

use crate::{Backend, RespArray, RespFrame, RespMap, RespNull};

impl CommandExecutor for HGet {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.hget(&self.key, &self.field) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl CommandExecutor for HSet {
    fn execute(self, backend: &Backend) -> RespFrame {
        backend.hset(self.key, self.field, self.value.clone());
        RESP_OK.clone()
    }
}

impl CommandExecutor for HGetAll {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.hgetall(&self.key) {
            Some(hmap) => RespFrame::Map(RespMap(hmap.into_iter().collect())),
            None => RespArray::new([]).into(),
        }
    }
}

impl TryFrom<RespArray> for HGet {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hget"], 2)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field))) => Ok(HGet {
                key: String::from_utf8(key.0)?,
                field: String::from_utf8(field.0)?,
            }),
            _ => Err(CommandError::InvalidCommand("Invalid key or field".into())),
        }
    }
}

impl TryFrom<RespArray> for HSet {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hset"], 3)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field)), Some(value)) => {
                Ok(HSet {
                    key: String::from_utf8(key.0)?,
                    field: String::from_utf8(field.0)?,
                    value,
                })
            }
            _ => Err(CommandError::InvalidCommand(
                "Invalid key, field or value".into(),
            )),
        }
    }
}

impl TryFrom<RespArray> for HGetAll {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["hgetall"], 1)?;

        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(HGetAll {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidCommand("Invalid key".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bytes::BytesMut;

    use crate::RespDecode;

    use super::*;

    #[test]
    fn test_hget_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$4\r\nhget\r\n$3\r\nkey\r\n$5\r\nfield\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let cmd: HGet = frame.try_into()?;
        assert_eq!(cmd.key, "key");
        assert_eq!(cmd.field, "field");
        Ok(())
    }

    #[test]
    fn test_hset_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*4\r\n$4\r\nhset\r\n$3\r\nkey\r\n$5\r\nfield\r\n$5\r\nvalue\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let cmd: HSet = frame.try_into()?;
        assert_eq!(cmd.key, "key");
        assert_eq!(cmd.field, "field");
        assert_eq!(cmd.value, RespFrame::BulkString(b"value".into()));
        Ok(())
    }

    #[test]
    fn test_hset_hget_hgetall_commands() -> Result<()> {
        let backend = Backend::new();
        let cmd = HSet {
            key: "map".to_string(),
            field: "hello".to_string(),
            value: RespFrame::BulkString(b"world".into()),
        };
        let resp = cmd.execute(&backend);
        assert_eq!(resp, RESP_OK.clone());

        let cmd = HSet {
            key: "map".to_string(),
            field: "foo".to_string(),
            value: RespFrame::BulkString(b"bar".into()),
        };
        cmd.execute(&backend);

        let cmd = HGet {
            key: "map".to_string(),
            field: "hello".to_string(),
        };
        let resp = cmd.execute(&backend);
        assert_eq!(resp, RespFrame::BulkString(b"world".into()));

        let cmd = HGetAll {
            key: "map".to_string(),
        };
        let resp = cmd.execute(&backend);
        let mut expected = RespMap::new();
        expected.insert("hello".to_string(), RespFrame::BulkString(b"world".into()));
        expected.insert("foo".to_string(), RespFrame::BulkString(b"bar".into()));
        assert_eq!(resp, expected.into());

        Ok(())
    }
}
