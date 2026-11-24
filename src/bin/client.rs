//! 手動で負荷をかけるクライアント
//!
//!   cargo run --release --bin client -- [件数=5] [パス=/wait?ms=1000] [--port 8080]
//!
//! 指定した件数のリクエストを同時に送り、それぞれの応答時間と合計時間を表示する。

use herring::client::{self, Plan};

fn main() {
    let mut pos = Vec::new();
    let mut port = 8080u16;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        if a == "--port" {
            port = it.next().and_then(|v| v.parse().ok()).expect("--port");
        } else {
            pos.push(a);
        }
    }
    let n: usize = pos.first().and_then(|s| s.parse().ok()).unwrap_or(5);
    let path = pos
        .get(1)
        .cloned()
        .unwrap_or_else(|| "/wait?ms=1000".into());

    println!("{n} 件の {path} を同時に送ります");
    let plans: Vec<Plan> = (0..n)
        .map(|_| Plan {
            offset_ms: 0,
            path: path.clone(),
        })
        .collect();
    let (start, events) = client::run(port, &plans);

    let mut lat: Vec<(usize, f64)> = Vec::new();
    for i in 0..n {
        let t = |k: &str| {
            events
                .iter()
                .find(|e| e.req == i && e.kind == k)
                .map(|e| e.ts)
        };
        match (t("connect"), t("done")) {
            (Some(a), Some(b)) => lat.push((i, (b - a) as f64 / 1e6)),
            _ => println!("  #{i} 失敗"),
        }
    }
    lat.sort_by(|a, b| a.1.total_cmp(&b.1));
    for (i, ms) in &lat {
        println!("  #{i:<4} {ms:9.2} ms");
    }
    let last = events
        .iter()
        .filter(|e| e.kind == "done")
        .map(|e| e.ts)
        .max()
        .unwrap_or(start);
    println!("合計 {:.2} ms", (last - start) as f64 / 1e6);
}
