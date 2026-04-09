use crate::error::{NatsError, Result};

pub const CR_LF: &[u8] = b"\r\n";
pub const PING: &[u8] = b"PING\r\n";
pub const PONG: &[u8] = b"PONG\r\n";

fn validate_name(value: &str, kind: &str) -> Result<()> {
    if value.is_empty() {
        return Err(NatsError::InvalidSubject(format!("{kind} cannot be empty")));
    }
    if let Some(b) = value
        .bytes()
        .find(|b| matches!(b, b' ' | b'\r' | b'\n' | b'\t' | b'\0'))
    {
        let name = match b {
            b' ' => "spaces",
            b'\r' | b'\n' => "CR/LF",
            b'\t' => "tabs",
            b'\0' => "null bytes",
            _ => unreachable!(),
        };
        return Err(NatsError::InvalidSubject(format!(
            "{kind} cannot contain {name}"
        )));
    }
    Ok(())
}

pub fn validate_subject(subject: &str) -> Result<()> {
    validate_name(subject, "subject")
}

pub fn validate_queue_group(queue: &str) -> Result<()> {
    validate_name(queue, "queue group")
}

pub fn validate_subscribe_subject(subject: &str) -> Result<()> {
    validate_name(subject, "subject")?;

    if subject.contains("..") {
        return Err(NatsError::InvalidSubject(
            "subject cannot contain empty tokens".to_string(),
        ));
    }

    let tokens: Vec<&str> = subject.split('.').collect();
    let last_idx = tokens.len() - 1;

    for (i, token) in tokens.iter().enumerate() {
        if token.contains('*') && *token != "*" {
            return Err(NatsError::InvalidSubject(
                "wildcard '*' must be a separate token".to_string(),
            ));
        }
        if token.contains('>') {
            if *token != ">" {
                return Err(NatsError::InvalidSubject(
                    "wildcard '>' must be a separate token".to_string(),
                ));
            }
            if i != last_idx {
                return Err(NatsError::InvalidSubject(
                    "wildcard '>' must be the last token".to_string(),
                ));
            }
        }
    }

    Ok(())
}

pub fn build_connect_cmd(connect_info: &crate::types::ConnectInfo) -> Result<Vec<u8>> {
    let json = serde_json::to_string(connect_info)?;
    let mut cmd = Vec::new();
    cmd.extend_from_slice(b"CONNECT ");
    cmd.extend_from_slice(json.as_bytes());
    cmd.extend_from_slice(CR_LF);
    Ok(cmd)
}

pub fn build_pub_cmd(subject: &str, reply: Option<&str>, data: &[u8]) -> Result<Vec<u8>> {
    validate_subject(subject)?;
    if let Some(reply) = reply {
        validate_subject(reply)?;
    }

    // "PUB " + subject + " " + reply? + " " + size_digits + "\r\n" + data + "\r\n"
    let cap = 4 + subject.len() + reply.map_or(0, |r| r.len() + 1) + 10 + 4 + data.len();
    let mut cmd = Vec::with_capacity(cap);
    cmd.extend_from_slice(b"PUB ");
    cmd.extend_from_slice(subject.as_bytes());

    if let Some(reply) = reply {
        cmd.push(b' ');
        cmd.extend_from_slice(reply.as_bytes());
    }

    cmd.push(b' ');
    cmd.extend_from_slice(data.len().to_string().as_bytes());
    cmd.extend_from_slice(CR_LF);
    cmd.extend_from_slice(data);
    cmd.extend_from_slice(CR_LF);

    Ok(cmd)
}

pub fn build_hpub_cmd(
    subject: &str,
    reply: Option<&str>,
    headers: &[u8],
    data: &[u8],
) -> Result<Vec<u8>> {
    validate_subject(subject)?;
    if let Some(reply) = reply {
        validate_subject(reply)?;
    }

    let cap =
        5 + subject.len() + reply.map_or(0, |r| r.len() + 1) + 20 + 4 + headers.len() + data.len();
    let mut cmd = Vec::with_capacity(cap);
    cmd.extend_from_slice(b"HPUB ");
    cmd.extend_from_slice(subject.as_bytes());

    if let Some(reply) = reply {
        cmd.push(b' ');
        cmd.extend_from_slice(reply.as_bytes());
    }

    let hdr_len = headers.len();
    let total_len = hdr_len + data.len();

    cmd.push(b' ');
    cmd.extend_from_slice(hdr_len.to_string().as_bytes());
    cmd.push(b' ');
    cmd.extend_from_slice(total_len.to_string().as_bytes());
    cmd.extend_from_slice(CR_LF);
    cmd.extend_from_slice(headers);
    cmd.extend_from_slice(data);
    cmd.extend_from_slice(CR_LF);

    Ok(cmd)
}

pub fn build_sub_cmd(subject: &str, queue: Option<&str>, sid: u64) -> Result<Vec<u8>> {
    validate_subscribe_subject(subject)?;
    if let Some(queue) = queue {
        validate_queue_group(queue)?;
    }

    let mut cmd = Vec::new();
    cmd.extend_from_slice(b"SUB ");
    cmd.extend_from_slice(subject.as_bytes());

    if let Some(queue) = queue {
        cmd.push(b' ');
        cmd.extend_from_slice(queue.as_bytes());
    }

    cmd.push(b' ');
    cmd.extend_from_slice(sid.to_string().as_bytes());
    cmd.extend_from_slice(CR_LF);

    Ok(cmd)
}

pub fn build_unsub_cmd(sid: u64, max_msgs: Option<u64>) -> Vec<u8> {
    let mut cmd = Vec::new();
    cmd.extend_from_slice(b"UNSUB ");
    cmd.extend_from_slice(sid.to_string().as_bytes());

    if let Some(max) = max_msgs {
        cmd.push(b' ');
        cmd.extend_from_slice(max.to_string().as_bytes());
    }

    cmd.extend_from_slice(CR_LF);
    cmd
}

pub fn extract_info_json(data: &[u8]) -> Result<String> {
    let s = std::str::from_utf8(data)
        .map_err(|e| NatsError::Parse(format!("Invalid UTF-8 in INFO: {e}")))?;

    if let Some(start) = s.find('{')
        && let Some(end) = s.rfind('}')
    {
        return Ok(s[start..=end].to_string());
    }

    Err(NatsError::Parse("Invalid INFO message format".to_string()))
}

pub fn parse_msg_arg(arg: &str) -> Result<(String, Option<String>, u64, usize)> {
    let parts: Vec<&str> = arg.split_whitespace().collect();

    match parts.len() {
        3 => {
            let subject = parts[0].to_string();
            let sid = parts[1]
                .parse::<u64>()
                .map_err(|_| NatsError::Parse("Invalid SID".to_string()))?;
            let size = parts[2]
                .parse::<usize>()
                .map_err(|_| NatsError::Parse("Invalid message size".to_string()))?;
            Ok((subject, None, sid, size))
        }
        // MSG <subject> <sid> <reply-to> <#bytes>
        4 => {
            let subject = parts[0].to_string();
            let sid = parts[1]
                .parse::<u64>()
                .map_err(|_| NatsError::Parse("Invalid SID".to_string()))?;
            let reply = Some(parts[2].to_string());
            let size = parts[3]
                .parse::<usize>()
                .map_err(|_| NatsError::Parse("Invalid message size".to_string()))?;
            Ok((subject, reply, sid, size))
        }
        _ => Err(NatsError::Parse("Invalid MSG arguments".to_string())),
    }
}

pub fn parse_hmsg_arg(arg: &str) -> Result<(String, Option<String>, u64, usize, usize)> {
    let parts: Vec<&str> = arg.split_whitespace().collect();

    match parts.len() {
        4 => {
            // HMSG <subject> <sid> <hdr_size> <total_size>
            let subject = parts[0].to_string();
            let sid = parts[1]
                .parse::<u64>()
                .map_err(|_| NatsError::Parse("Invalid SID".to_string()))?;
            let hdr_size = parts[2]
                .parse::<usize>()
                .map_err(|_| NatsError::Parse("Invalid header size".to_string()))?;
            let total_size = parts[3]
                .parse::<usize>()
                .map_err(|_| NatsError::Parse("Invalid total size".to_string()))?;
            Ok((subject, None, sid, hdr_size, total_size))
        }
        // HMSG <subject> <sid> <reply-to> <hdr_size> <total_size>
        5 => {
            let subject = parts[0].to_string();
            let sid = parts[1]
                .parse::<u64>()
                .map_err(|_| NatsError::Parse("Invalid SID".to_string()))?;
            let reply = Some(parts[2].to_string());
            let hdr_size = parts[3]
                .parse::<usize>()
                .map_err(|_| NatsError::Parse("Invalid header size".to_string()))?;
            let total_size = parts[4]
                .parse::<usize>()
                .map_err(|_| NatsError::Parse("Invalid total size".to_string()))?;
            Ok((subject, reply, sid, hdr_size, total_size))
        }
        _ => Err(NatsError::Parse("Invalid HMSG arguments".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_pub_cmd() {
        let cmd = build_pub_cmd("test.subject", None, b"hello").unwrap();
        assert_eq!(cmd, b"PUB test.subject 5\r\nhello\r\n");

        let cmd = build_pub_cmd("test.subject", Some("reply.to"), b"hello").unwrap();
        assert_eq!(cmd, b"PUB test.subject reply.to 5\r\nhello\r\n");
    }

    #[test]
    fn test_build_pub_cmd_empty_payload() {
        let cmd = build_pub_cmd("test.subject", None, b"").unwrap();
        assert_eq!(cmd, b"PUB test.subject 0\r\n\r\n");
    }

    #[test]
    fn test_build_pub_cmd_large_payload() {
        let payload = vec![b'x'; 1000];
        let cmd = build_pub_cmd("test.subject", None, &payload).unwrap();
        assert!(cmd.starts_with(b"PUB test.subject 1000\r\n"));
        assert!(cmd.ends_with(b"\r\n"));
    }

    #[test]
    fn test_build_hpub_cmd() {
        let headers = b"NATS/1.0\r\n\r\n";
        let data = b"hello";
        let cmd = build_hpub_cmd("test.subject", None, headers, data).unwrap();

        // Verify format: HPUB <subject> <hdr_len> <total_len>\r\n<headers><data>\r\n
        assert!(cmd.starts_with(b"HPUB test.subject "));
        assert!(cmd.ends_with(b"\r\n"));

        // Check lengths
        let hdr_len = headers.len();
        let total_len = hdr_len + data.len();
        let expected_start = format!("HPUB test.subject {} {}\r\n", hdr_len, total_len);
        assert!(cmd.starts_with(expected_start.as_bytes()));
    }

    #[test]
    fn test_build_hpub_cmd_with_reply() {
        let headers = b"NATS/1.0\r\nX-Key: value\r\n\r\n";
        let data = b"payload";
        let cmd = build_hpub_cmd("test.subject", Some("reply.inbox"), headers, data).unwrap();

        assert!(cmd.starts_with(b"HPUB test.subject reply.inbox"));
    }

    #[test]
    fn test_build_sub_cmd() {
        let cmd = build_sub_cmd("test.subject", None, 1).unwrap();
        assert_eq!(cmd, b"SUB test.subject 1\r\n");

        let cmd = build_sub_cmd("test.subject", Some("queue"), 2).unwrap();
        assert_eq!(cmd, b"SUB test.subject queue 2\r\n");
    }

    #[test]
    fn test_build_sub_cmd_large_sid() {
        let cmd = build_sub_cmd("test.subject", None, u64::MAX).unwrap();
        let expected = format!("SUB test.subject {}\r\n", u64::MAX);
        assert_eq!(cmd, expected.as_bytes());
    }

    #[test]
    fn test_validate_subject_rejects_empty() {
        assert!(validate_subject("").is_err());
    }

    #[test]
    fn test_validate_subject_rejects_spaces() {
        assert!(validate_subject("foo bar").is_err());
    }

    #[test]
    fn test_validate_subject_rejects_tabs() {
        assert!(validate_subject("foo\tbar").is_err());
    }

    #[test]
    fn test_validate_subject_rejects_cr_lf() {
        assert!(validate_subject("foo\r\nbar").is_err());
        assert!(validate_subject("foo\rbar").is_err());
        assert!(validate_subject("foo\nbar").is_err());
    }

    #[test]
    fn test_validate_subject_rejects_null() {
        assert!(validate_subject("foo\0bar").is_err());
    }

    #[test]
    fn test_validate_subject_accepts_valid() {
        assert!(validate_subject("foo.bar").is_ok());
        assert!(validate_subject("foo.*").is_ok());
        assert!(validate_subject("foo.>").is_ok());
    }

    #[test]
    fn test_validate_queue_group_rejects_empty() {
        assert!(validate_queue_group("").is_err());
    }

    #[test]
    fn test_validate_queue_group_rejects_invalid() {
        assert!(validate_queue_group("q group").is_err());
        assert!(validate_queue_group("q\r\n").is_err());
        assert!(validate_queue_group("q\0").is_err());
    }

    #[test]
    fn test_build_pub_cmd_rejects_invalid_subject() {
        assert!(build_pub_cmd("", None, b"data").is_err());
        assert!(build_pub_cmd("foo bar", None, b"data").is_err());
        assert!(build_pub_cmd("foo\r\nPONG\r\n", None, b"data").is_err());
    }

    #[test]
    fn test_build_pub_cmd_rejects_invalid_reply() {
        assert!(build_pub_cmd("valid", Some("bad reply"), b"data").is_err());
        assert!(build_pub_cmd("valid", Some("bad\r\n"), b"data").is_err());
    }

    #[test]
    fn test_build_sub_cmd_rejects_invalid_queue() {
        assert!(build_sub_cmd("valid", Some("bad queue"), 1).is_err());
        assert!(build_sub_cmd("valid", Some(""), 1).is_err());
    }

    #[test]
    fn test_build_unsub_cmd() {
        let cmd = build_unsub_cmd(1, None);
        assert_eq!(cmd, b"UNSUB 1\r\n");

        let cmd = build_unsub_cmd(5, Some(10));
        assert_eq!(cmd, b"UNSUB 5 10\r\n");
    }

    #[test]
    fn test_build_unsub_cmd_max_msgs_zero() {
        let cmd = build_unsub_cmd(1, Some(0));
        assert_eq!(cmd, b"UNSUB 1 0\r\n");
    }

    #[test]
    fn test_extract_info_json() {
        let data = b"INFO {\"server_id\":\"test\",\"version\":\"2.0.0\"}";
        let json = extract_info_json(data).unwrap();
        assert!(json.contains("server_id"));
        assert!(json.contains("test"));
    }

    #[test]
    fn test_extract_info_json_with_whitespace() {
        let data = b"  {\"key\":\"value\"}  ";
        let json = extract_info_json(data).unwrap();
        assert_eq!(json, "{\"key\":\"value\"}");
    }

    #[test]
    fn test_extract_info_json_invalid() {
        let data = b"INFO no_json_here";
        let result = extract_info_json(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_info_json_nested() {
        let data = b"INFO {\"nested\":{\"inner\":\"value\"}}";
        let json = extract_info_json(data).unwrap();
        assert!(json.contains("nested"));
        assert!(json.contains("inner"));
    }

    #[test]
    fn test_parse_msg_arg() {
        let (subj, reply, sid, size) = parse_msg_arg("test.subject 1 5").unwrap();
        assert_eq!(subj, "test.subject");
        assert_eq!(reply, None);
        assert_eq!(sid, 1);
        assert_eq!(size, 5);

        // MSG <subject> <sid> <reply-to> <#bytes>
        let (subj, reply, sid, size) = parse_msg_arg("test.subject 2 reply.to 10").unwrap();
        assert_eq!(subj, "test.subject");
        assert_eq!(reply, Some("reply.to".to_string()));
        assert_eq!(sid, 2);
        assert_eq!(size, 10);
    }

    #[test]
    fn test_parse_msg_arg_invalid_args() {
        // Too few arguments
        let result = parse_msg_arg("subject 1");
        assert!(result.is_err());

        // Too many arguments
        let result = parse_msg_arg("subject reply 1 5 extra");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_msg_arg_invalid_sid() {
        let result = parse_msg_arg("subject not_a_number 5");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_msg_arg_invalid_size() {
        let result = parse_msg_arg("subject 1 not_a_number");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_hmsg_arg() {
        // Without reply
        let (subj, reply, sid, hdr_size, total_size) =
            parse_hmsg_arg("test.subject 1 10 15").unwrap();
        assert_eq!(subj, "test.subject");
        assert_eq!(reply, None);
        assert_eq!(sid, 1);
        assert_eq!(hdr_size, 10);
        assert_eq!(total_size, 15);

        // HMSG <subject> <sid> <reply-to> <hdr_size> <total_size>
        let (subj, reply, sid, hdr_size, total_size) =
            parse_hmsg_arg("test.subject 2 reply.inbox 20 30").unwrap();
        assert_eq!(subj, "test.subject");
        assert_eq!(reply, Some("reply.inbox".to_string()));
        assert_eq!(sid, 2);
        assert_eq!(hdr_size, 20);
        assert_eq!(total_size, 30);
    }

    #[test]
    fn test_parse_hmsg_arg_invalid() {
        // Too few arguments
        let result = parse_hmsg_arg("subject 1 10");
        assert!(result.is_err());

        // Too many arguments
        let result = parse_hmsg_arg("subject reply 1 10 15 extra");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_hmsg_arg_invalid_numbers() {
        let result = parse_hmsg_arg("subject abc 10 15");
        assert!(result.is_err());

        let result = parse_hmsg_arg("subject 1 abc 15");
        assert!(result.is_err());

        let result = parse_hmsg_arg("subject 1 10 abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_protocol_constants() {
        assert_eq!(PING, b"PING\r\n");
        assert_eq!(PONG, b"PONG\r\n");
        assert_eq!(CR_LF, b"\r\n");
    }

    #[test]
    fn test_validate_subscribe_subject_valid() {
        assert!(validate_subscribe_subject("foo.bar").is_ok());
        assert!(validate_subscribe_subject("foo.*").is_ok());
        assert!(validate_subscribe_subject("foo.>").is_ok());
        assert!(validate_subscribe_subject(">").is_ok());
        assert!(validate_subscribe_subject("*").is_ok());
        assert!(validate_subscribe_subject("foo.*.bar").is_ok());
        assert!(validate_subscribe_subject("foo.*.bar.>").is_ok());
    }

    #[test]
    fn test_validate_subscribe_subject_rejects_empty_tokens() {
        assert!(validate_subscribe_subject("foo..bar").is_err());
        assert!(validate_subscribe_subject("..").is_err());
        assert!(validate_subscribe_subject("foo..").is_err());
    }

    #[test]
    fn test_validate_subscribe_subject_rejects_partial_wildcard() {
        assert!(validate_subscribe_subject("foo*.bar").is_err());
        assert!(validate_subscribe_subject("foo.ba*").is_err());
        assert!(validate_subscribe_subject("foo.*r").is_err());
    }

    #[test]
    fn test_validate_subscribe_subject_rejects_gt_not_last() {
        assert!(validate_subscribe_subject("foo.>.bar").is_err());
        assert!(validate_subscribe_subject(">.foo").is_err());
    }

    #[test]
    fn test_validate_subscribe_subject_rejects_partial_gt() {
        assert!(validate_subscribe_subject("foo.bar>").is_err());
        assert!(validate_subscribe_subject("foo.>bar").is_err());
    }

    #[test]
    fn test_validate_subscribe_subject_inherits_base_validation() {
        assert!(validate_subscribe_subject("").is_err());
        assert!(validate_subscribe_subject("foo bar").is_err());
        assert!(validate_subscribe_subject("foo\tbar").is_err());
        assert!(validate_subscribe_subject("foo\r\n").is_err());
        assert!(validate_subscribe_subject("foo\0bar").is_err());
    }

    #[test]
    fn test_build_sub_cmd_rejects_invalid_wildcard_subjects() {
        assert!(build_sub_cmd("foo*.bar", None, 1).is_err());
        assert!(build_sub_cmd("foo.>.bar", None, 1).is_err());
        assert!(build_sub_cmd("foo..bar", None, 1).is_err());
    }

    #[test]
    fn test_build_connect_cmd() {
        let connect_info = crate::types::ConnectInfo::default();
        let cmd = build_connect_cmd(&connect_info).unwrap();
        assert!(cmd.starts_with(b"CONNECT "));
        assert!(cmd.ends_with(b"\r\n"));
    }
}
