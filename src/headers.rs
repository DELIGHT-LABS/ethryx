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
