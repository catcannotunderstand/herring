//! 最小限の HTTP。リクエストラインと `X-Req` ヘッダだけを見る。
//!
//! ルート:
//!   `/`            すぐ返す
//!   `/wait?ms=N`   N ミリ秒の「I/O 待ち」（DB 問い合わせなどの代わり）。CPU は使わない
//!   `/cpu?ms=N`    N ミリ秒ぶんの CPU 計算（スレッドの CPU 時間で数える）

use crate::trace;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Hello,
    Wait(u64),
    Cpu(u64),
}

#[derive(Debug, Clone)]
pub struct Request {
    pub path: String,
    /// クライアントが付けた通し番号。クライアント側の記録と突き合わせるのに使う
    pub req_id: Option<u64>,
    pub route: Route,
}

/// ヘッダの終わり（\r\n\r\n）まで受信できていれば true
pub fn header_complete(buf: &[u8]) -> bool {
    buf.windows(4).any(|w| w == b"\r\n\r\n")
}

pub fn parse(buf: &[u8]) -> Request {
    let text = String::from_utf8_lossy(buf);
    let mut lines = text.split("\r\n");
    let path = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .unwrap_or("/")
        .to_string();
    let req_id = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.trim().eq_ignore_ascii_case("x-req"))
        .and_then(|(_, v)| v.trim().parse().ok());
    let ms = |p: &str| {
        p.split_once('?')
            .and_then(|(_, q)| q.split('&').find_map(|kv| kv.strip_prefix("ms=")))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    };
    let route = if path.starts_with("/wait") {
        Route::Wait(ms(&path))
    } else if path.starts_with("/cpu") {
        Route::Cpu(ms(&path))
    } else {
        Route::Hello
    };
    Request {
        path,
        req_id,
        route,
    }
}

/// request イベントの arg（"req_id path"）
pub fn describe(req: &Request) -> String {
    format!(
        "{} {}",
        req.req_id
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into()),
        req.path
    )
}

pub fn response(body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .into_bytes()
}

/// ms ミリ秒ぶん、本当に CPU を使って計算する。
/// 壁時計ではなくスレッドの CPU 時間で数えるので、CPU を取り合うと壁時計ではもっと長くかかる。
pub fn burn_cpu(ms: u64) -> u64 {
    let target = trace::thread_cpu_ns() + ms * 1_000_000;
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    while trace::thread_cpu_ns() < target {
        for _ in 0..10_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
        }
    }
    std::hint::black_box(x)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_routes_and_req_id() {
        let r = parse(b"GET /wait?ms=250 HTTP/1.1\r\nHost: x\r\nX-Req: 7\r\n\r\n");
        assert_eq!(r.route, Route::Wait(250));
        assert_eq!(r.req_id, Some(7));
        assert_eq!(
            parse(b"GET /cpu?ms=3 HTTP/1.1\r\n\r\n").route,
            Route::Cpu(3)
        );
        assert_eq!(parse(b"GET / HTTP/1.1\r\n\r\n").route, Route::Hello);
        assert!(header_complete(b"GET / HTTP/1.1\r\n\r\n"));
        assert!(!header_complete(b"GET / HTTP/1.1\r\n"));
    }
}
