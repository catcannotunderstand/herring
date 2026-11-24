//! サーバ共通の引数
//!
//!   --port 8080        待ち受けるポート
//!   --trace FILE.csv   内部イベントを記録して、終了時に FILE.csv に書く
//!   --exit-after N     N 本の接続を処理し終えたら、記録を書き出して終了する
//!   --workers N        （tokio のみ）ワーカースレッド数
//!   --verbose          1リクエストごとに標準出力へログを出す

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Debug)]
pub struct ServerArgs {
    pub port: u16,
    pub trace: Option<String>,
    pub exit_after: Option<u64>,
    pub workers: Option<usize>,
    pub verbose: bool,
}

impl ServerArgs {
    pub fn parse() -> Self {
        let mut a = ServerArgs {
            port: 8080,
            trace: None,
            exit_after: None,
            workers: None,
            verbose: false,
        };
        let mut it = std::env::args().skip(1);
        while let Some(k) = it.next() {
            let mut val = || it.next().unwrap_or_else(|| panic!("{k} needs a value"));
            match k.as_str() {
                "--port" => a.port = val().parse().expect("--port"),
                "--trace" => a.trace = Some(val()),
                "--exit-after" => a.exit_after = Some(val().parse().expect("--exit-after")),
                "--workers" => a.workers = Some(val().parse().expect("--workers")),
                "--verbose" => a.verbose = true,
                other => panic!("unknown option: {other}"),
            }
        }
        if a.trace.is_some() {
            crate::trace::enable();
        }
        a
    }

    /// 接続を1本処理し終えたときに呼ぶ。--exit-after に達したら記録を書き出して終了する。
    pub fn connection_done(&self) {
        static DONE: AtomicU64 = AtomicU64::new(0);
        let n = DONE.fetch_add(1, Ordering::SeqCst) + 1;
        if self.exit_after.is_some_and(|limit| n >= limit) {
            self.finish();
        }
    }

    pub fn finish(&self) -> ! {
        if let Some(path) = &self.trace {
            crate::trace::dump(path).expect("write trace");
        }
        std::process::exit(0)
    }
}
