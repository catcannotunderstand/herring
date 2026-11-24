//! 計画どおりの時刻にリクエストを投げるクライアント。
//!
//! リクエストごとに1スレッドを使い、全員がそろってから開始時刻を決める。
//! 記録するイベント（同じ CLOCK_MONOTONIC）:
//!   connect   connect() を呼んだ
//!   connected connect() が返った（カーネル同士の3ウェイハンドシェイクが終わった）
//!   sent      リクエストを送り終えた
//!   first     応答の最初のバイトを受け取った
//!   done      応答を最後まで受け取った（arg = 受信バイト数）
//!
//! 各リクエストには `X-Req: <番号>` ヘッダを付け、サーバ側の記録と突き合わせられるようにする。

use crate::trace::now_ns;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Plan {
    /// 開始時刻からの遅れ（ミリ秒）
    pub offset_ms: u64,
    pub path: String,
}

#[derive(Clone, Debug)]
pub struct ClientEvent {
    pub ts: u64,
    pub req: usize,
    pub kind: &'static str,
    pub arg: String,
}

/// 計画を実行し、(開始時刻, イベント列) を返す
pub fn run(port: u16, plans: &[Plan]) -> (u64, Vec<ClientEvent>) {
    // 全スレッドがそろったあとで、少し先の時刻を「開始」にする
    let barrier = Arc::new(Barrier::new(plans.len() + 1));
    let start = Arc::new(std::sync::OnceLock::<u64>::new());

    let handles: Vec<_> = plans
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, plan)| {
            let barrier = barrier.clone();
            let start = start.clone();
            thread::Builder::new()
                .stack_size(256 * 1024)
                .spawn(move || {
                    barrier.wait();
                    let at = *start.get().unwrap() + plan.offset_ms * 1_000_000;
                    let now = now_ns();
                    if at > now {
                        thread::sleep(Duration::from_nanos(at - now));
                    }
                    one(port, i, &plan.path)
                })
                .expect("spawn client thread")
        })
        .collect();

    start.set(now_ns() + 20_000_000).unwrap(); // 20ms 後に一斉開始
    barrier.wait();

    let mut events = Vec::new();
    for h in handles {
        events.extend(h.join().unwrap_or_default());
    }
    events.sort_by_key(|e| e.ts);
    (*start.get().unwrap(), events)
}

fn one(port: u16, req: usize, path: &str) -> Vec<ClientEvent> {
    let mut ev = Vec::with_capacity(5);
    let mut mark = |kind, arg: String| {
        ev.push(ClientEvent {
            ts: now_ns(),
            req,
            kind,
            arg,
        })
    };

    mark("connect", String::new());
    let mut s = match TcpStream::connect(("127.0.0.1", port)) {
        Ok(s) => s,
        Err(e) => {
            mark("error", e.to_string());
            return ev;
        }
    };
    mark(
        "connected",
        s.local_addr()
            .map(|a| a.port().to_string())
            .unwrap_or_default(),
    );
    s.set_nodelay(true).ok();
    let req_text = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nX-Req: {req}\r\n\r\n");
    if let Err(e) = s.write_all(req_text.as_bytes()) {
        mark("error", e.to_string());
        return ev;
    }
    mark("sent", path.to_string());

    let mut buf = [0u8; 4096];
    let mut total = 0;
    loop {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if total == 0 {
                    mark("first", String::new());
                }
                total += n;
            }
            Err(e) => {
                mark("error", e.to_string());
                return ev;
            }
        }
    }
    mark("done", total.to_string());
    ev
}
