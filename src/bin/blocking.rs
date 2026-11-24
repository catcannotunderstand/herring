//! ⓪ 1接続ずつ順番に処理するサーバ（比較用）
//!
//!   cargo run --release --bin blocking -- [--trace FILE] [--exit-after N]
//!
//! accept → read → 処理 → write → close を1本ずつ。処理中に来た接続は、
//! カーネルが accept 待ちのキューに溜めておくだけで、アプリは誰も相手にしない。

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
    println!("[blocking] listening on http://127.0.0.1:{}", args.port);

    let mut next_conn = 1u64;
    loop {
        emit(0, "block", "accept");
        let (stream, peer) = listener.accept()?;
        emit(0, "unblock", "accept");
        let conn = next_conn;
        next_conn += 1;
        emit(conn, "accept", peer.port().to_string());
        // handle が終わるまで、次の accept には進めない
        if let Err(e) = handle(conn, stream, &args) {
            eprintln!("[blocking] conn {conn}: {e}");
        }
        emit(conn, "close", "");
        args.connection_done();
    }
}

fn handle(conn: u64, mut stream: TcpStream, args: &ServerArgs) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 2048];
    while !http::header_complete(&buf) {
        emit(conn, "block", "read");
        let n = stream.read(&mut chunk)?; // データが来るまでスレッドごと止まる
        emit(conn, "unblock", "read");
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let req = http::parse(&buf);
    emit(conn, "request", http::describe(&req));
    if args.verbose {
        println!("[blocking] conn {conn} {}", req.path);
    }

    match req.route {
        Route::Wait(ms) => {
            emit(conn, "wait_begin", ms.to_string());
            emit(conn, "block", "sleep");
            thread::sleep(Duration::from_millis(ms)); // I/O 待ちのつもり。この間、他は誰も進まない
            emit(conn, "unblock", "sleep");
            emit(conn, "wait_end", "");
        }
        Route::Cpu(ms) => {
            emit(conn, "cpu_begin", ms.to_string());
            http::burn_cpu(ms);
            emit(conn, "cpu_end", "");
        }
        Route::Hello => {}
    }

    let res = http::response(&format!("blocking conn {conn}\n"));
    stream.write_all(&res)?;
    emit(conn, "respond", res.len().to_string());
    Ok(())
}
