//! ② イベントループ + ノンブロッキング I/O のサーバ（nginx / Node.js と同じ考え方）
//!
//!   cargo run --release --bin event_loop -- [--trace FILE] [--exit-after N]
//!
//! スレッドは main の1本だけ。mio は Linux では epoll、macOS では kqueue の薄いラッパー。
//! 止まってよい場所は poll（= epoll_wait）の1か所だけで、それ以外は決して待たない。
//!
//! - 読むデータがまだ無ければ WouldBlock が返るので、その接続は諦めて次へ進む
//! - /wait の「待ち」は sleep ではなくタイマー。epoll_wait のタイムアウトに使う（Node.js の setTimeout と同じ）
//! - /cpu の計算はこのスレッドで行うので、その間はほかの全接続が止まる（Node.js で重い同期処理を書くとまずいのと同じ理由）

use herring::args::ServerArgs;
use herring::http::{self, Route};
use herring::{net, trace::emit};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::time::{Duration, Instant};

const LISTENER: Token = Token(0);

/// 1接続がいまどの段階にいるか。
/// ① ならスレッドのスタック（どこまで実行したか）が覚えている情報を、ここで自分で持つ。
enum State {
    Reading,
    Waiting(Instant),
    Writing(Vec<u8>, usize),
}

struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
    state: State,
}

fn main() -> std::io::Result<()> {
    let args = ServerArgs::parse();
    let mut poll = Poll::new()?; // epoll_create
    let mut events = Events::with_capacity(1024);
    let std_listener = net::listen(args.port)?;
    std_listener.set_nonblocking(true)?;
    let mut listener = TcpListener::from_std(std_listener);
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)?; // epoll_ctl(ADD)
    println!(
        "[event_loop] listening on http://127.0.0.1:{} (1 thread)",
        args.port
    );

    let mut conns: HashMap<Token, Conn> = HashMap::new();
    let mut next = 1usize;

    loop {
        // 一番近いタイマーまでの時間を、待ちの上限にする
        let now = Instant::now();
        let timeout = conns
            .values()
            .filter_map(|c| match c.state {
                State::Waiting(t) => Some(t.saturating_duration_since(now)),
                _ => None,
            })
            .min();

        emit(0, "block", "epoll_wait");
        poll.poll(&mut events, timeout)?; // ← このサーバが止まるのはここだけ
        emit(
            0,
            "unblock",
            format!("epoll_wait:{}", events.iter().count()),
        );

        // --- 1. 準備ができたソケットを処理 ---
        for event in events.iter() {
            if event.token() == LISTENER {
                loop {
                    match listener.accept() {
                        Ok((mut stream, peer)) => {
                            let token = Token(next);
                            next += 1;
                            emit(token.0 as u64, "accept", peer.port().to_string());
                            poll.registry()
                                .register(&mut stream, token, Interest::READABLE)?;
                            conns.insert(
                                token,
                                Conn {
                                    stream,
                                    buf: Vec::new(),
                                    state: State::Reading,
                                },
                            );
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(e) => return Err(e),
                    }
                }
                continue;
            }
            let token = event.token();
            let Some(conn) = conns.get_mut(&token) else {
                continue;
            };
            let mut done = false;
            if event.is_readable() {
                done = on_readable(&poll, token, conn, &args)?;
            }
            if !done && event.is_writable() {
                done = on_writable(token, conn);
            }
            if done {
                close(&poll, &mut conns, token, &args)?;
            }
        }

        // --- 2. 時刻が来たタイマーを処理（「DB の応答が返ってきた」）---
        let now = Instant::now();
        let ready: Vec<Token> = conns
            .iter()
            .filter(|(_, c)| matches!(c.state, State::Waiting(t) if t <= now))
            .map(|(t, _)| *t)
            .collect();
        for token in ready {
            emit(token.0 as u64, "wait_end", "");
            let conn = conns.get_mut(&token).unwrap();
            start_writing(&poll, token, conn)?;
        }
    }
}

/// 読めるだけ読む。ヘッダが揃ったらルートに応じて処理を始める。戻り値 true = 閉じてよい
fn on_readable(
    poll: &Poll,
    token: Token,
    conn: &mut Conn,
    args: &ServerArgs,
) -> std::io::Result<bool> {
    let id = token.0 as u64;
    let mut chunk = [0u8; 2048];
    loop {
        match conn.stream.read(&mut chunk) {
            Ok(0) => return Ok(true),
            Ok(n) => conn.buf.extend_from_slice(&chunk[..n]),
            // 今は読むものがない → 待たずにイベントループへ戻る（① との一番の違い）
            Err(e) if e.kind() == ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(_) => return Ok(true),
        }
    }
    if !matches!(conn.state, State::Reading) || !http::header_complete(&conn.buf) {
        return Ok(false);
    }
    let req = http::parse(&conn.buf);
    emit(id, "request", http::describe(&req));
    if args.verbose {
        println!("[event_loop] {:?} {}", token, req.path);
    }
    match req.route {
        Route::Wait(ms) if ms > 0 => {
            // sleep せず「ms 後に再開」と予定だけ書いて、すぐ次の接続へ
            emit(id, "wait_begin", ms.to_string());
            conn.state = State::Waiting(Instant::now() + Duration::from_millis(ms));
        }
        Route::Cpu(ms) => {
            emit(id, "cpu_begin", ms.to_string());
            http::burn_cpu(ms); // ループ全体が止まる
            emit(id, "cpu_end", "");
            start_writing(poll, token, conn)?;
        }
        _ => start_writing(poll, token, conn)?,
    }
    Ok(false)
}

fn start_writing(poll: &Poll, token: Token, conn: &mut Conn) -> std::io::Result<()> {
    let res = http::response(&format!("event_loop conn {}\n", token.0));
    conn.state = State::Writing(res, 0);
    // 「書けるようになったら教えて」に切り替える（epoll_ctl(MOD)）
    poll.registry()
        .reregister(&mut conn.stream, token, Interest::WRITABLE)
}

/// 書けるだけ書く。送信バッファが一杯なら続きは次の WRITABLE イベントで。戻り値 true = 閉じてよい
fn on_writable(token: Token, conn: &mut Conn) -> bool {
    let State::Writing(buf, written) = &mut conn.state else {
        return false;
    };
    while *written < buf.len() {
        match conn.stream.write(&buf[*written..]) {
            Ok(n) => *written += n,
            Err(e) if e.kind() == ErrorKind::WouldBlock => return false,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(_) => return true,
        }
    }
    emit(token.0 as u64, "respond", buf.len().to_string());
    true
}

fn close(
    poll: &Poll,
    conns: &mut HashMap<Token, Conn>,
    token: Token,
    args: &ServerArgs,
) -> std::io::Result<()> {
    if let Some(mut conn) = conns.remove(&token) {
        poll.registry().deregister(&mut conn.stream)?;
        drop(conn);
        emit(token.0 as u64, "close", "");
        args.connection_done();
    }
    Ok(())
}
