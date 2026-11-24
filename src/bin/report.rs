//! runs/ の記録を、可視化ページが読むデータファイル docs/data.js にまとめる
//!
//!   cargo run --release --bin report            # runs/ → docs/data.js
//!   cargo run --release --bin report -- DIR OUT
//!
//! 時刻はすべて「クライアントの開始時刻からのマイクロ秒」（小数1桁 = 100ns 単位）に直す。
//! docs/index.html は data.js を <script src> で読むので、ファイルを直接ブラウザで開いても動く。

use std::fs;
use std::path::Path;

const SCENARIO_ORDER: [&str; 3] = ["io", "cpu", "scale"];
const SERVER_ORDER: [&str; 4] = ["blocking", "threaded", "event_loop", "async_tokio"];

fn main() -> std::io::Result<()> {
    let mut a = std::env::args().skip(1);
    let runs = a.next().unwrap_or_else(|| "runs".into());
    let out = a.next().unwrap_or_else(|| "docs/data.js".into());

    let mut items = Vec::new();
    let mut scenarios: Vec<String> = fs::read_dir(&runs)?
        .filter_map(|e| Some(e.ok()?.file_name().to_str()?.to_string()))
        .collect();
    scenarios.sort_by_key(|s| {
        (
            SCENARIO_ORDER.iter().position(|x| x == s).unwrap_or(99),
            s.clone(),
        )
    });
    for sc in &scenarios {
        let mut servers: Vec<String> = fs::read_dir(Path::new(&runs).join(sc))?
            .filter_map(|e| Some(e.ok()?.file_name().to_str()?.to_string()))
            .collect();
        servers.sort_by_key(|s| {
            (
                SERVER_ORDER.iter().position(|x| x == s).unwrap_or(99),
                s.clone(),
            )
        });
        for sv in &servers {
            let dir = Path::new(&runs).join(sc).join(sv);
            let Ok(meta) = fs::read_to_string(dir.join("meta.json")) else {
                continue;
            };
            let t0: u64 = json_number(&meta, "client_start_ns").expect("client_start_ns");
            let table = |name: &str| csv_json(&dir.join(name), t0).unwrap_or_else(|| "null".into());
            items.push(format!(
                "{{\"meta\":{},\n\"server_events\":{},\n\"client_events\":{},\n\"samples\":{},\n\"thread_samples\":{}}}",
                meta.trim(),
                table("server_events.csv"),
                table("client_events.csv"),
                table("samples.csv"),
                table("thread_samples.csv"),
            ));
            eprintln!("  {sc}/{sv}");
        }
    }
    if let Some(dir) = Path::new(&out).parent() {
        fs::create_dir_all(dir)?;
    }
    let body = format!(
        "// cargo run --release --bin report が生成。手で編集しないでください\nwindow.HERRING_RUNS = [\n{}\n];\n",
        items.join(",\n")
    );
    fs::write(&out, &body)?;
    eprintln!(
        "wrote {out} ({} runs, {} KB)",
        items.len(),
        body.len() / 1024
    );
    Ok(())
}

/// CSV を {"cols":[...],"rows":[[...],...]} にする。ts_ns 列は t0 からの µs に変換する
fn csv_json(path: &Path, t0: u64) -> Option<String> {
    let text = fs::read_to_string(path).ok()?;
    let mut lines = text.lines();
    let header: Vec<&str> = lines.next()?.split(',').collect();
    let ts_col = header.iter().position(|h| *h == "ts_ns");
    let cols: Vec<String> = header
        .iter()
        .map(|h| {
            if *h == "ts_ns" {
                "\"t_us\"".to_string()
            } else {
                format!("\"{h}\"")
            }
        })
        .collect();
    let mut rows = Vec::new();
    for line in lines {
        let f: Vec<&str> = line.split(',').collect();
        let cells: Vec<String> = f
            .iter()
            .enumerate()
            .map(|(i, v)| {
                if Some(i) == ts_col {
                    let ns: i128 = v.parse().unwrap_or(0);
                    let us = (ns - t0 as i128) as f64 / 1000.0;
                    format!("{:.1}", us)
                } else if v.is_empty() {
                    "null".to_string()
                } else if v.parse::<f64>().is_ok() && !v.starts_with('+') {
                    v.to_string()
                } else {
                    format!("\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\""))
                }
            })
            .collect();
        rows.push(format!("[{}]", cells.join(",")));
    }
    Some(format!(
        "{{\"cols\":[{}],\"rows\":[{}]}}",
        cols.join(","),
        rows.join(",")
    ))
}

fn json_number(json: &str, key: &str) -> Option<u64> {
    let i = json.find(&format!("\"{key}\""))?;
    let rest = &json[i + key.len() + 2..];
    let rest = rest.trim_start().strip_prefix(':')?.trim_start();
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}
