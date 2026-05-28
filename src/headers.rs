use http::HeaderMap;

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

pub fn strip_hop_by_hop<T>(headers: &mut HeaderMap<T>) {
    for &name in HOP_BY_HOP {
        headers.remove(name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn h(pairs: &[(&'static str, &'static str)]) -> HeaderMap<HeaderValue> {
        let mut m = HeaderMap::new();
        for (k, v) in pairs {
            m.insert(*k, HeaderValue::from_static(v));
        }
        m
    }

    #[test]
    fn removes_known_hop_by_hop() {
        let mut m = h(&[
            ("host", "example.com"),
            ("connection", "keep-alive"),
            ("content-length", "42"),
            ("transfer-encoding", "chunked"),
            ("upgrade", "websocket"),
            ("content-type", "application/json"),
        ]);
        strip_hop_by_hop(&mut m);
        assert!(!m.contains_key("host"));
        assert!(!m.contains_key("connection"));
        assert!(!m.contains_key("content-length"));
        assert!(!m.contains_key("transfer-encoding"));
        assert!(!m.contains_key("upgrade"));
        assert!(m.contains_key("content-type"));
    }

    #[test]
    fn is_case_insensitive_via_canonical_lookup() {
        let mut m = h(&[("Host", "x"), ("CONNECTION", "close")]);
        strip_hop_by_hop(&mut m);
        assert!(m.is_empty());
    }

    #[test]
    fn empty_map_is_noop() {
        let mut m: HeaderMap<HeaderValue> = HeaderMap::new();
        strip_hop_by_hop(&mut m);
        assert!(m.is_empty());
    }

    #[test]
    fn preserves_unrelated_headers() {
        let mut m = h(&[
            ("authorization", "Bearer x"),
            ("x-custom", "1"),
            ("user-agent", "ethryx"),
        ]);
        strip_hop_by_hop(&mut m);
        assert_eq!(m.len(), 3);
    }
}
