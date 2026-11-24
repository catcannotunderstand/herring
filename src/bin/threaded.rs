//! ① 1接続 = 1スレッド のサーバ（Apache の prefork / worker と同じ考え方）
//!
//!   cargo run --release --bin threaded -- [--trace FILE] [--exit-after N]
//!
//! メインスレッドは accept だけを繰り返し、接続ごとに新しいスレッドへ渡す。
//! 各スレッドの中身は blocking とまったく同じ「普通のブロッキング I/O」。
//! read や sleep で止まるのはそのスレッドだけで、OS のスケジューラが別のスレッドに CPU を回す。

use herring::args::ServerArgs;
use herring::http::{self, Route};
use herring::{net, trace::emit};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let args = ServerArgs::parse();
    let listener = net::listen(args.port)?;
    println!("[threaded] listening on http://127.0.0.1:{}", args.port);

    let mut next_conn = 1u64;
    loop {
        emit(0, "block", "accept");
        let (stream, peer) = listener.accept()?;
        emit(0, "unblock", "accept");
        let conn = next_conn;
        next_conn += 1;
        emit(conn, "accept", peer.port().to_string());

        let args = args.clone();
        // Rust の既定スタックは 2MiB。仮想メモリとしては確保されるが、実際に使ったページしか RSS にならない
        thread::Builder::new()
            .name(format!("conn-{conn}"))
            .spawn(move || {
                emit(conn, "thread_start", "");
                if let Err(e) = handle(conn, stream, &args) {
                    eprintln!("[threaded] conn {conn}: {e}");
                }
                emit(conn, "close", "");
                emit(conn, "thread_end", "");
                args.connection_done();
            })?;
    }
}

fn handle(conn: u64, mut stream: TcpStream, args: &ServerArgs) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 2048];
    while !http::header_complete(&buf) {
        emit(conn, "block", "read");
        let n = stream.read(&mut chunk)?; // 止まるのはこのスレッドだけ
        emit(conn, "unblock", "read");
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let req = http::parse(&buf);
    emit(conn, "request", http::describe(&req));
    if args.verbose {
        println!("[threaded] conn {conn} {}", req.path);
    }

    match req.route {
        Route::Wait(ms) => {
            emit(conn, "wait_begin", ms.to_string());
            emit(conn, "block", "sleep");
            thread::sleep(Duration::from_millis(ms));
            emit(conn, "unblock", "sleep");
            emit(conn, "wait_end", "");
        }
        Route::Cpu(ms) => {
            emit(conn, "cpu_begin", ms.to_string());
            http::burn_cpu(ms); // CPU を使う間も、他のスレッドは別のコアで動ける
            emit(conn, "cpu_end", "");
        }
        Route::Hello => {}
    }

    let res = http::response(&format!("threaded conn {conn}\n"));
    stream.write_all(&res)?;
    emit(conn, "respond", res.len().to_string());
    Ok(())
}
