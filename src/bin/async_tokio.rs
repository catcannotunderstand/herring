//! ③ async/await（tokio）のサーバ。② のイベントループを、言語とライブラリが書きやすくしたもの
//!
//!   cargo run --release --bin async_tokio -- [--workers N] [--trace FILE] [--exit-after N]
//!
//! コードは ① のような「上から下へ読める」形のまま、中身は ② と同じ epoll ベース。
//! `.await` のたびに「まだ準備できていなければ、いったん手放して他のタスクへ」が起きる。
//! ② の enum State（どこまで進んだか）は、コンパイラが async fn から自動で作ってくれる。
//!
//! ② との違い: ワーカースレッドが複数（既定は CPU コア数）あり、タスクがスレッド間で分担・融通される。
//! nginx が「コア数ぶんのワーカー × 各ワーカーがイベントループ」なのと近い。
//!
//! /cpu の計算は .await を挟まないので、そのワーカースレッドを占有する。
//! 本番なら tokio::task::spawn_blocking に逃がすところだが、ここでは観察のためにあえてそのままにしている。

use herring::args::ServerArgs;
use herring::http::{self, Route};
use herring::{net, trace::emit};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn main() -> std::io::Result<()> {
    let args = ServerArgs::parse();
    let workers = args.workers.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2)
    });
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;
    rt.block_on(serve(args, workers))
}

async fn serve(args: ServerArgs, workers: usize) -> std::io::Result<()> {
    let std_listener = net::listen(args.port)?;
    std_listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(std_listener)?;
    println!(
        "[async_tokio] listening on http://127.0.0.1:{} ({workers} worker threads)",
        args.port
    );

    static NEXT: AtomicU64 = AtomicU64::new(1);
    loop {
        let (stream, peer) = listener.accept().await?; // 接続が来るまで、このタスクだけが中断される
        let conn = NEXT.fetch_add(1, Ordering::Relaxed);
        emit(conn, "accept", peer.port().to_string());
        let args = args.clone();
        // スレッドではなく「タスク」を作る。1つあたり数百バイト程度
        tokio::spawn(async move {
            if let Err(e) = handle(conn, stream, &args).await {
                eprintln!("[async_tokio] conn {conn}: {e}");
            }
            emit(conn, "close", "");
            args.connection_done();
        });
    }
}

// ① の handle とほぼ同じ見た目。違いは .await だけ
async fn handle(conn: u64, mut stream: TcpStream, args: &ServerArgs) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 2048];
    while !http::header_complete(&buf) {
        let n = stream.read(&mut chunk).await?; // 読めなければ中断して、ワーカーは他のタスクへ
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let req = http::parse(&buf);
    emit(conn, "request", http::describe(&req));
    if args.verbose {
        println!("[async_tokio] conn {conn} {}", req.path);
    }

    match req.route {
        Route::Wait(ms) => {
            emit(conn, "wait_begin", ms.to_string());
            tokio::time::sleep(Duration::from_millis(ms)).await; // タイマー登録だけして中断
            emit(conn, "wait_end", "");
        }
        Route::Cpu(ms) => {
            emit(conn, "cpu_begin", ms.to_string());
            http::burn_cpu(ms); // .await が無いので、このワーカーを占有する
            emit(conn, "cpu_end", "");
        }
        Route::Hello => {}
    }

    let res = http::response(&format!("async_tokio conn {conn}\n"));
    stream.write_all(&res).await?;
    emit(conn, "respond", res.len().to_string());
    Ok(())
}
