//! Shared JSON-RPC inspection for HTTP and WebSocket mock requests.
//! Mixed batches are forwarded intact; only entirely local batches are handled.

use std::borrow::Cow;

use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MockResult {
    Forward,
    Silent,
    Reply(String),
}

#[derive(Deserialize)]
struct Request<'a> {
    #[serde(rename = "jsonrpc", deserialize_with = "version_2")]
    _version: (),
    #[serde(borrow)]
    method: Cow<'a, str>,
    #[serde(rename = "params", default, deserialize_with = "structured_params")]
    _params: (),
    // A missing ID is a notification; an explicit null ID still gets a reply.
    #[serde(default, borrow, deserialize_with = "present_id")]
    id: Option<&'a RawValue>,
}

fn version_2<'de, D: Deserializer<'de>>(d: D) -> Result<(), D::Error> {
    struct Version;
    impl serde::de::Visitor<'_> for Version {
        type Value = ();
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("JSON-RPC version 2.0")
        }
        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<(), E> {
            if value == "2.0" {
                Ok(())
            } else {
                Err(E::custom("unsupported JSON-RPC version"))
            }
        }
    }
    d.deserialize_str(Version)
}

fn structured_params<'de, D: Deserializer<'de>>(d: D) -> Result<(), D::Error> {
    let raw = <&RawValue>::deserialize(d)?;
    if matches!(raw.get().as_bytes().first(), Some(b'[' | b'{')) {
        Ok(())
    } else {
        Err(serde::de::Error::custom(
            "params must be an array or object",
        ))
    }
}

fn present_id<'de, D: Deserializer<'de>>(d: D) -> Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(d).map(Some)
}

impl Request<'_> {
    fn is_local(&self) -> bool {
        self.method == "eth_syncing"
            && self.id.is_none_or(|id| {
                matches!(id.get().as_bytes().first(), Some(b'"' | b'-' | b'0'..=b'9'))
                    || id.get() == "null"
            })
    }

    fn append_reply(&self, out: &mut String) -> bool {
        let Some(id) = self.id else { return false };
        out.push_str(r#"{"jsonrpc":"2.0","id":"#);
        out.push_str(id.get());
        out.push_str(r#","result":false}"#);
        true
    }
}

pub(crate) fn mock_syncing(bytes: &[u8]) -> MockResult {
    // Escaped method names are valid JSON, so a substring prefilter is unsafe.
    let first = bytes.iter().find(|b| !b.is_ascii_whitespace());
    if first == Some(&b'[') {
        let Ok(requests) = serde_json::from_slice::<Vec<Request<'_>>>(bytes) else {
            return MockResult::Forward;
        };
        if requests.is_empty() || !requests.iter().all(Request::is_local) {
            return MockResult::Forward;
        }
        let capacity = requests
            .iter()
            .filter_map(|r| r.id)
            .map(|id| id.get().len() + 42)
            .sum::<usize>();
        if capacity == 0 {
            return MockResult::Silent;
        }
        let mut reply = String::with_capacity(capacity + 2);
        reply.push('[');
        for req in &requests {
            if req.id.is_some() {
                if reply.len() > 1 {
                    reply.push(',');
                }
                req.append_reply(&mut reply);
            }
        }
        reply.push(']');
        MockResult::Reply(reply)
    } else {
        let Ok(req) = serde_json::from_slice::<Request<'_>>(bytes) else {
            return MockResult::Forward;
        };
        if !req.is_local() {
            return MockResult::Forward;
        }
        let mut reply = String::with_capacity(req.id.map_or(0, |id| id.get().len() + 42));
        if req.append_reply(&mut reply) {
            MockResult::Reply(reply)
        } else {
            MockResult::Silent
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_ids_and_decodes_escaped_methods() {
        for id in ["null", "900719925474099312345", r#""a\\b\"c""#, "-1"] {
            let input = format!(r#"{{"jsonrpc":"2.0","method":"eth_\u0073yncing","id":{id}}}"#);
            let MockResult::Reply(reply) = mock_syncing(input.as_bytes()) else {
                panic!("{input}")
            };
            assert!(reply.contains(&format!("\"id\":{id}")));
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&reply).unwrap()["result"],
                false
            );
        }
        assert!(matches!(
            mock_syncing(br#"{"jsonrpc":"2.\u0030","method":"eth_syncing","params":{},"id":0}"#),
            MockResult::Reply(_)
        ));
    }

    #[test]
    fn notifications_are_silent_and_batches_omit_them() {
        let notification = r#"{"jsonrpc":"2.0","method":"eth_syncing"}"#;
        assert_eq!(mock_syncing(notification.as_bytes()), MockResult::Silent);
        assert_eq!(
            mock_syncing(format!("[{notification}]").as_bytes()),
            MockResult::Silent
        );
        let batch =
            format!(r#"[{notification},{{"jsonrpc":"2.0","method":"eth_syncing","id":null}}]"#);
        let MockResult::Reply(reply) = mock_syncing(batch.as_bytes()) else {
            panic!()
        };
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&reply).unwrap();
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0]["id"].is_null());
    }

    #[test]
    fn forwards_mixed_invalid_and_unknown_requests_without_rewriting() {
        for input in [
            "[]",
            "null",
            "{}",
            "[",
            "[1]",
            r#"{"jsonrpc":"1.0","method":"eth_syncing","id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","id":true}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","params":null,"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","params":1,"id":1}"#,
            r#"{"method":"eth_syncing","id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","method":"eth_call","id":1}"#,
            r#"[{"jsonrpc":"2.0","method":"eth_syncing","id":1},{"jsonrpc":"2.0","method":"eth_call","id":2}]"#,
        ] {
            assert_eq!(
                mock_syncing(input.as_bytes()),
                MockResult::Forward,
                "{input}"
            );
        }
    }
}
