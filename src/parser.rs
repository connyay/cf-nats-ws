use crate::error::{NatsError, Result};
use crate::protocol;
use crate::types::Message;
use bytes::{Buf, BufMut, BytesMut};

// Type alias to reduce complexity
// (subject, reply, sid, total_size, optional_hdr_size)
type MsgArgTuple = (String, Option<String>, u64, usize, Option<usize>);

#[derive(Debug, Clone)]
pub enum Op {
    Info(String),
    Msg(Message, u64),
    HMsg(Message, u64),
    Ping,
    Pong,
    Ok,
    Err(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum State {
    OpStart,
    OpH,
    OpHm,
    OpHms,
    OpHmsg,
    OpHmsgSpace,
    OpI,
    OpIn,
    OpInf,
    OpInfo,
    OpInfoSpace,
    OpM,
    OpMs,
    OpMsg,
    OpMsgSpace,
    OpP,
    OpPi,
    OpPin,
    OpPing,
    OpPo,
    OpPon,
    OpPong,
    OpPlus,
    OpPlusO,
    OpPlusOk,
    OpMinus,
    OpMinusE,
    OpMinusEr,
    OpMinusErr,
    OpMinusErrSpace,
    MsgArg,
    HMsgArg,
    MsgData,
    InfoArg,
    ErrArg,
}

pub struct Parser {
    state: State,
    buffer: BytesMut,
    arg_buffer: Vec<u8>,
    msg_arg: Option<MsgArgTuple>,
    msg_needed: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ping() {
        let mut parser = Parser::new();
        let ops = parser.parse(b"PING\r\n").unwrap();
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0], Op::Ping));
    }

    #[test]
    fn test_parse_pong() {
        let mut parser = Parser::new();
        let ops = parser.parse(b"PONG\r\n").unwrap();
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0], Op::Pong));
    }

    #[test]
    fn test_parse_ok() {
        let mut parser = Parser::new();
        let ops = parser.parse(b"+OK\r\n").unwrap();
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0], Op::Ok));
    }

    #[test]
    fn test_parse_err() {
        let mut parser = Parser::new();
        let ops = parser.parse(b"-ERR 'Unknown Error'\r\n").unwrap();
        assert_eq!(ops.len(), 1);
        if let Op::Err(msg) = &ops[0] {
            assert_eq!(msg, "'Unknown Error'");
        } else {
            panic!("Expected Op::Err");
        }
    }

    #[test]
    fn test_parse_info() {
        let mut parser = Parser::new();
        let info_json = r#"{"server_id":"test","version":"2.0.0"}"#;
        let input = format!("INFO {}\r\n", info_json);
        let ops = parser.parse(input.as_bytes()).unwrap();
        assert_eq!(ops.len(), 1);
        if let Op::Info(json) = &ops[0] {
            assert!(json.contains("server_id"));
            assert!(json.contains("test"));
        } else {
            panic!("Expected Op::Info");
        }
    }

    #[test]
    fn test_parse_msg_without_reply() {
        let mut parser = Parser::new();
        let input = b"MSG test.subject 1 5\r\nhello\r\n";
        let ops = parser.parse(input).unwrap();
        assert_eq!(ops.len(), 1);
        if let Op::Msg(msg, sid) = &ops[0] {
            assert_eq!(msg.subject, "test.subject");
            assert_eq!(*sid, 1);
            assert_eq!(msg.data, b"hello");
            assert!(msg.reply.is_none());
        } else {
            panic!("Expected Op::Msg");
        }
    }

    #[test]
    fn test_parse_msg_with_reply() {
        let mut parser = Parser::new();
        // MSG <subject> <sid> <reply-to> <#bytes>
        let input = b"MSG test.subject 1 reply.to 5\r\nhello\r\n";
        let ops = parser.parse(input).unwrap();
        assert_eq!(ops.len(), 1);
        if let Op::Msg(msg, sid) = &ops[0] {
            assert_eq!(msg.subject, "test.subject");
            assert_eq!(msg.reply, Some("reply.to".to_string()));
            assert_eq!(*sid, 1);
            assert_eq!(msg.data, b"hello");
        } else {
            panic!("Expected Op::Msg");
        }
    }

    #[test]
    fn test_parse_hmsg_without_reply() {
        let mut parser = Parser::new();
        // HMSG format: HMSG <subject> <sid> <hdr_size> <total_size>\r\n<headers><payload>\r\n
        let headers = b"NATS/1.0\r\nX-Test: value\r\n\r\n";
        let payload = b"hello";
        let total_size = headers.len() + payload.len();
        let input = format!("HMSG test.subject 1 {} {}\r\n", headers.len(), total_size);
        let mut full_input = input.into_bytes();
        full_input.extend_from_slice(headers);
        full_input.extend_from_slice(payload);
        full_input.extend_from_slice(b"\r\n");

        let ops = parser.parse(&full_input).unwrap();
        assert_eq!(ops.len(), 1);
        if let Op::HMsg(msg, sid) = &ops[0] {
            assert_eq!(msg.subject, "test.subject");
            assert_eq!(*sid, 1);
            assert_eq!(msg.data, b"hello");
            assert!(msg.reply.is_none());
            assert!(msg.headers.is_some());
        } else {
            panic!("Expected Op::HMsg");
        }
    }

    #[test]
    fn test_parse_hmsg_with_reply() {
        let mut parser = Parser::new();
        let headers = b"NATS/1.0\r\n\r\n";
        let payload = b"test";
        let total_size = headers.len() + payload.len();
        // HMSG <subject> <sid> <reply-to> <hdr_size> <total_size>
        let input = format!(
            "HMSG test.subject 1 reply.inbox {} {}\r\n",
            headers.len(),
            total_size
        );
        let mut full_input = input.into_bytes();
        full_input.extend_from_slice(headers);
        full_input.extend_from_slice(payload);
        full_input.extend_from_slice(b"\r\n");

        let ops = parser.parse(&full_input).unwrap();
        assert_eq!(ops.len(), 1);
        if let Op::HMsg(msg, sid) = &ops[0] {
            assert_eq!(msg.subject, "test.subject");
            assert_eq!(msg.reply, Some("reply.inbox".to_string()));
            assert_eq!(*sid, 1);
        } else {
            panic!("Expected Op::HMsg");
        }
    }

    #[test]
    fn test_parse_multiple_ops() {
        let mut parser = Parser::new();
        let input = b"PING\r\nPONG\r\n+OK\r\n";
        let ops = parser.parse(input).unwrap();
        assert_eq!(ops.len(), 3);
        assert!(matches!(ops[0], Op::Ping));
        assert!(matches!(ops[1], Op::Pong));
        assert!(matches!(ops[2], Op::Ok));
    }

    #[test]
    fn test_parse_case_insensitive() {
        let mut parser = Parser::new();
        let ops = parser.parse(b"ping\r\n").unwrap();
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0], Op::Ping));

        let mut parser = Parser::new();
        let ops = parser.parse(b"Pong\r\n").unwrap();
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0], Op::Pong));

        let mut parser = Parser::new();
        let ops = parser.parse(b"info {\"test\":true}\r\n").unwrap();
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0], Op::Info(_)));
    }

    #[test]
    fn test_parse_incremental() {
        let mut parser = Parser::new();

        // Send partial data
        let ops = parser.parse(b"PIN").unwrap();
        assert!(ops.is_empty());

        // Send rest of the data
        let ops = parser.parse(b"G\r\n").unwrap();
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0], Op::Ping));
    }

    #[test]
    fn test_parse_msg_incremental() {
        let mut parser = Parser::new();

        // Send header
        let ops = parser.parse(b"MSG test.subject 1 5\r\n").unwrap();
        assert!(ops.is_empty());

        // Send payload
        let ops = parser.parse(b"hello\r\n").unwrap();
        assert_eq!(ops.len(), 1);
        if let Op::Msg(msg, _) = &ops[0] {
            assert_eq!(msg.data, b"hello");
        } else {
            panic!("Expected Op::Msg");
        }
    }

    #[test]
    fn test_parse_empty_msg() {
        let mut parser = Parser::new();
        let input = b"MSG test.subject 1 0\r\n\r\n";
        let ops = parser.parse(input).unwrap();
        assert_eq!(ops.len(), 1);
        if let Op::Msg(msg, _) = &ops[0] {
            assert!(msg.data.is_empty());
        } else {
            panic!("Expected Op::Msg");
        }
    }

    #[test]
    fn test_parse_whitespace_tolerance() {
        let mut parser = Parser::new();
        // Leading whitespace should be ignored
        let ops = parser.parse(b"  \r\n  PING\r\n").unwrap();
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0], Op::Ping));
    }

    #[test]
    fn test_parse_invalid_start_byte() {
        let mut parser = Parser::new();
        let result = parser.parse(b"X\r\n");
        assert!(result.is_err());
        if let Err(NatsError::Parse(msg)) = result {
            assert!(msg.contains("Unexpected byte"));
        } else {
            panic!("Expected parse error");
        }
    }

    #[test]
    fn test_parser_default() {
        let parser = Parser::default();
        assert_eq!(parser.state, State::OpStart);
    }

    #[test]
    fn test_parse_large_msg() {
        let mut parser = Parser::new();
        let payload = "x".repeat(10000);
        let input = format!("MSG big.subject 99 {}\r\n{}\r\n", payload.len(), payload);
        let ops = parser.parse(input.as_bytes()).unwrap();
        assert_eq!(ops.len(), 1);
        if let Op::Msg(msg, sid) = &ops[0] {
            assert_eq!(msg.subject, "big.subject");
            assert_eq!(*sid, 99);
            assert_eq!(msg.data.len(), 10000);
        } else {
            panic!("Expected Op::Msg");
        }
    }
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser {
    pub fn new() -> Self {
        Self {
            state: State::OpStart,
            buffer: BytesMut::with_capacity(4096),
            arg_buffer: Vec::new(),
            msg_arg: None,
            msg_needed: 0,
        }
    }

    pub fn parse(&mut self, data: &[u8]) -> Result<Vec<Op>> {
        self.buffer.put_slice(data);
        let mut ops = Vec::new();

        while !self.buffer.is_empty() {
            match self.state {
                State::OpStart => {
                    if let Some(&b) = self.buffer.first() {
                        self.buffer.advance(1);
                        self.state = match b {
                            b'H' | b'h' => State::OpH,
                            b'I' | b'i' => State::OpI,
                            b'M' | b'm' => State::OpM,
                            b'P' | b'p' => State::OpP,
                            b'+' => State::OpPlus,
                            b'-' => State::OpMinus,
                            b'\r' | b'\n' | b' ' | b'\t' => State::OpStart,
                            _ => {
                                return Err(NatsError::Parse(format!(
                                    "Unexpected byte in OpStart: {b}"
                                )));
                            }
                        };
                    } else {
                        break;
                    }
                }
                State::OpH => self.transition(b'M', b'm', State::OpHm)?,
                State::OpHm => self.transition(b'S', b's', State::OpHms)?,
                State::OpHms => self.transition(b'G', b'g', State::OpHmsg)?,
                State::OpHmsg => self.transition(b' ', b'\t', State::OpHmsgSpace)?,
                State::OpHmsgSpace => {
                    self.arg_buffer.clear();
                    self.state = State::HMsgArg;
                }
                State::OpI => self.transition(b'N', b'n', State::OpIn)?,
                State::OpIn => self.transition(b'F', b'f', State::OpInf)?,
                State::OpInf => self.transition(b'O', b'o', State::OpInfo)?,
                State::OpInfo => self.transition(b' ', b'\t', State::OpInfoSpace)?,
                State::OpInfoSpace => {
                    self.arg_buffer.clear();
                    self.state = State::InfoArg;
                }
                State::InfoArg => {
                    if let Some(pos) = self.find_crlf() {
                        let arg = self.buffer.split_to(pos);
                        self.buffer.advance(2); // Skip \r\n
                        self.arg_buffer.extend_from_slice(&arg);

                        let json = protocol::extract_info_json(&self.arg_buffer)?;
                        ops.push(Op::Info(json));
                        self.arg_buffer.clear();
                        self.state = State::OpStart;
                    } else {
                        break;
                    }
                }
                State::OpM => self.transition(b'S', b's', State::OpMs)?,
                State::OpMs => self.transition(b'G', b'g', State::OpMsg)?,
                State::OpMsg => self.transition(b' ', b'\t', State::OpMsgSpace)?,
                State::OpMsgSpace => {
                    self.arg_buffer.clear();
                    self.state = State::MsgArg;
                }
                State::MsgArg => {
                    if let Some(pos) = self.find_crlf() {
                        let arg = self.buffer.split_to(pos);
                        self.buffer.advance(2); // Skip \r\n
                        self.arg_buffer.extend_from_slice(&arg);

                        let arg_str = std::str::from_utf8(&self.arg_buffer).map_err(|e| {
                            NatsError::Parse(format!("Invalid UTF-8 in MSG arg: {e}"))
                        })?;

                        let parsed = protocol::parse_msg_arg(arg_str)?;
                        self.msg_needed = parsed.3;
                        self.msg_arg = Some((parsed.0, parsed.1, parsed.2, parsed.3, None));
                        self.arg_buffer.clear();
                        self.state = State::MsgData;
                    } else {
                        break;
                    }
                }
                State::HMsgArg => {
                    if let Some(pos) = self.find_crlf() {
                        let arg = self.buffer.split_to(pos);
                        self.buffer.advance(2); // Skip \r\n
                        self.arg_buffer.extend_from_slice(&arg);

                        let arg_str = std::str::from_utf8(&self.arg_buffer).map_err(|e| {
                            NatsError::Parse(format!("Invalid UTF-8 in HMSG arg: {e}"))
                        })?;

                        let parsed = protocol::parse_hmsg_arg(arg_str)?;
                        self.msg_needed = parsed.4;
                        self.msg_arg =
                            Some((parsed.0, parsed.1, parsed.2, parsed.4, Some(parsed.3)));
                        self.arg_buffer.clear();
                        self.state = State::MsgData;
                    } else {
                        break;
                    }
                }
                State::MsgData => {
                    if self.buffer.len() >= self.msg_needed + 2 {
                        let data = self.buffer.split_to(self.msg_needed);
                        self.buffer.advance(2); // Skip \r\n

                        if let Some((subject, reply, sid, _total_size, hdr_size)) =
                            self.msg_arg.take()
                        {
                            if let Some(hdr_size) = hdr_size {
                                // HMSG with headers
                                use crate::headers::Headers;
                                let headers = if hdr_size > 0 {
                                    let hdr_data = &data[..hdr_size];
                                    Some(Headers::decode(hdr_data).unwrap_or_default())
                                } else {
                                    None
                                };
                                let payload = data[hdr_size..].to_vec();
                                let msg = Message {
                                    subject,
                                    reply,
                                    data: payload,
                                    headers,
                                };
                                ops.push(Op::HMsg(msg, sid));
                            } else {
                                // Regular MSG
                                let msg = Message {
                                    subject,
                                    reply,
                                    data: data.to_vec(),
                                    headers: None,
                                };
                                ops.push(Op::Msg(msg, sid));
                            }
                        }

                        self.msg_needed = 0;
                        self.state = State::OpStart;
                    } else {
                        break;
                    }
                }
                State::OpP => {
                    if let Some(&b) = self.buffer.first() {
                        self.buffer.advance(1);
                        self.state = match b {
                            b'I' | b'i' => State::OpPi,
                            b'O' | b'o' => State::OpPo,
                            _ => {
                                return Err(NatsError::Parse(format!(
                                    "Unexpected byte after P: {b}"
                                )));
                            }
                        };
                    } else {
                        break;
                    }
                }
                State::OpPi => self.transition(b'N', b'n', State::OpPin)?,
                State::OpPin => self.transition(b'G', b'g', State::OpPing)?,
                State::OpPing => {
                    self.consume_to_crlf()?;
                    ops.push(Op::Ping);
                    self.state = State::OpStart;
                }
                State::OpPo => self.transition(b'N', b'n', State::OpPon)?,
                State::OpPon => self.transition(b'G', b'g', State::OpPong)?,
                State::OpPong => {
                    self.consume_to_crlf()?;
                    ops.push(Op::Pong);
                    self.state = State::OpStart;
                }
                State::OpPlus => self.transition(b'O', b'o', State::OpPlusO)?,
                State::OpPlusO => self.transition(b'K', b'k', State::OpPlusOk)?,
                State::OpPlusOk => {
                    self.consume_to_crlf()?;
                    ops.push(Op::Ok);
                    self.state = State::OpStart;
                }
                State::OpMinus => self.transition(b'E', b'e', State::OpMinusE)?,
                State::OpMinusE => self.transition(b'R', b'r', State::OpMinusEr)?,
                State::OpMinusEr => self.transition(b'R', b'r', State::OpMinusErr)?,
                State::OpMinusErr => self.transition(b' ', b'\t', State::OpMinusErrSpace)?,
                State::OpMinusErrSpace => {
                    self.arg_buffer.clear();
                    self.state = State::ErrArg;
                }
                State::ErrArg => {
                    if let Some(pos) = self.find_crlf() {
                        let arg = self.buffer.split_to(pos);
                        self.buffer.advance(2); // Skip \r\n
                        self.arg_buffer.extend_from_slice(&arg);

                        let err_msg = String::from_utf8_lossy(&self.arg_buffer).to_string();
                        ops.push(Op::Err(err_msg));
                        self.arg_buffer.clear();
                        self.state = State::OpStart;
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(ops)
    }

    fn transition(
        &mut self,
        expected_upper: u8,
        expected_lower: u8,
        next_state: State,
    ) -> Result<()> {
        if let Some(&b) = self.buffer.first() {
            if b == expected_upper || b == expected_lower {
                self.buffer.advance(1);
                self.state = next_state;
                Ok(())
            } else {
                Err(NatsError::Parse(format!(
                    "Expected {} or {}, got {} in state {:?}",
                    expected_upper as char, expected_lower as char, b as char, self.state
                )))
            }
        } else {
            // Buffer is empty, we need more data - stay in current state
            Ok(())
        }
    }

    fn find_crlf(&self) -> Option<usize> {
        (0..self.buffer.len().saturating_sub(1))
            .find(|&i| self.buffer[i] == b'\r' && self.buffer[i + 1] == b'\n')
    }

    fn consume_to_crlf(&mut self) -> Result<()> {
        if let Some(pos) = self.find_crlf() {
            self.buffer.advance(pos + 2);
            Ok(())
        } else {
            Ok(())
        }
    }
}
