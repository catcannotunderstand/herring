//! 実験ランナー（Linux 専用）
//!
//!   cargo run --release --bin observe -- all                 # 全シナリオ × 全サーバ
//!   cargo run --release --bin observe -- io --servers threaded,event_loop
//!   cargo run --release --bin observe -- list                # シナリオ一覧
//!
//! 1回の実験 = 1シナリオ × 1サーバ。次の順に進む。
//!   1. サーバを --trace 付きで起動（内部イベントをサーバ自身が記録）
//!   2. サンプラーのスレッドが、サーバを外から一定間隔で覗き始める（/proc を読むだけ）
//!   3. クライアントが計画どおりの時刻にリクエストを投げる（クライアント側のイベントを記録）
//!   4. サーバは全接続を処理し終えると、記録を書き出して自分で終了する
//!   5. runs/<シナリオ>/<サーバ>/ に CSV 4つと meta.json を保存
//!
//! 3つの記録はすべて CLOCK_MONOTONIC（ナノ秒）で、1本の時間軸に重ねられる。

use herring::client::{self, ClientEvent, Plan};
use herring::trace::now_ns;
use std::collections::HashMap;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const PORT: u16 = 8080;
/// スレッドがこれ以下なら、スレッド1本ずつの状態（いま入っているシステムコール）も記録する
const DETAIL_THREADS: usize = 64;
const ALL_SERVERS: [&str; 4] = ["blocking", "threaded", "event_loop", "async_tokio"];

struct Scenario {
    name: &'static str,
    about: &'static str,
    servers: &'static [&'static str],
    plans: fn() -> Vec<Plan>,
}

fn plan(offset_ms: u64, path: &str) -> Plan {
    Plan {
        offset_ms,
        path: path.to_string(),
    }
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "io",
            about: "1秒の I/O 待ちがあるリクエストを5件同時に送る",
            servers: &ALL_SERVERS,
            plans: || (0..5).map(|_| plan(0, "/wait?ms=1000")).collect(),
        },
        Scenario {
            name: "cpu",
            about: "300ms の CPU 計算を1件送り、その最中に軽いリクエストを4件送る",
            servers: &ALL_SERVERS,
            plans: || {
                let mut v = vec![plan(0, "/cpu?ms=300")];
                v.extend((1..=4).map(|i| plan(i * 50, "/")));
                v
            },
        },
        Scenario {
            name: "scale",
            about: "1秒の I/O 待ちがあるリクエストを500件同時に送る（blocking は500秒かかるので除外）",
            servers: &["threaded", "event_loop", "async_tokio"],
            plans: || (0..500).map(|_| plan(0, "/wait?ms=1000")).collect(),
        },
    ]
}

struct Opts {
    which: String,
    servers: Option<Vec<String>>,
    interval: Duration,
    out: PathBuf,
}

fn parse_opts() -> Opts {
    let mut o = Opts {
        which: "all".into(),
        servers: None,
        interval: Duration::from_millis(10),
        out: "runs".into(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--servers" => {
                o.servers = Some(
                    it.next()
                        .expect("--servers")
                        .split(',')
                        .map(String::from)
                        .collect(),
                )
            }
            "--interval-ms" => {
                o.interval =
                    Duration::from_millis(it.next().expect("--interval-ms").parse().expect("ms"))
            }
            "--out" => o.out = it.next().expect("--out").into(),
            s if !s.starts_with("--") => o.which = s.to_string(),
            other => panic!("unknown option: {other}"),
        }
    }
    o
}

fn main() -> std::io::Result<()> {
    let opts = parse_opts();
    let all = scenarios();
    if opts.which == "list" {
        for s in &all {
            println!("{:6} {}  [{}]", s.name, s.about, s.servers.join(", "));
        }
        return Ok(());
    }
    let bin_dir = std::env::current_exe()?.parent().unwrap().to_path_buf();
    let selected: Vec<&Scenario> = all
        .iter()
        .filter(|s| opts.which == "all" || s.name == opts.which)
        .collect();
    if selected.is_empty() {
        eprintln!("unknown scenario: {} (try `observe list`)", opts.which);
        std::process::exit(2);
    }
    for sc in selected {
        for &server in sc.servers {
            if opts
                .servers
                .as_ref()
                .is_some_and(|l| !l.iter().any(|s| s == server))
            {
                continue;
            }
            let dir = opts.out.join(sc.name).join(server);
            fs::create_dir_all(&dir)?;
            print!("{:6} {:12} ... ", sc.name, server);
            std::io::stdout().flush()?;
            let summary = run_one(&bin_dir, sc, server, &dir, opts.interval)?;
            println!("{summary}");
            wait_port_free();
        }
    }
    println!("\n次に `cargo run --release --bin report` で docs/ の可視化ページを作り直せます");
    Ok(())
}

fn run_one(
    bin_dir: &Path,
    sc: &Scenario,
    server: &str,
    dir: &Path,
    interval: Duration,
) -> std::io::Result<String> {
    let plans = (sc.plans)();
    let trace_path = dir.join("server_events.csv");
    let _ = fs::remove_file(&trace_path);

    let mut child = Command::new(bin_dir.join(server))
        .args([
            "--port",
            &PORT.to_string(),
            "--trace",
            trace_path.to_str().unwrap(),
        ])
        .args(["--exit-after", &plans.len().to_string()])
        .stdout(Stdio::null())
        .spawn()?;
    let pid = child.id();
    wait_listening(pid);

    // サンプラー開始
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let stop = stop.clone();
        thread::spawn(move || sample_loop(pid, interval, &stop))
    };
    thread::sleep(Duration::from_millis(100)); // 何も起きていない状態も少し記録しておく

    let (client_start, client_events) = client::run(PORT, &plans);

    // サーバが自分で終わるのを待つ（終わらなければ止める）
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(st) = child.try_wait()? {
            break Some(st);
        }
        if Instant::now() > deadline {
            child.kill().ok();
            child.wait().ok();
            break None;
        }
        thread::sleep(Duration::from_millis(5));
    };
    stop.store(true, Ordering::Relaxed);
    let (samples, threads) = sampler.join().unwrap();

    write_client(&dir.join("client_events.csv"), &client_events)?;
    write_samples(&dir.join("samples.csv"), &samples)?;
    write_thread_samples(&dir.join("thread_samples.csv"), &threads)?;
    let done = client_events.iter().filter(|e| e.kind == "done").count();
    let errors = client_events.iter().filter(|e| e.kind == "error").count();
    let last_done = client_events
        .iter()
        .filter(|e| e.kind == "done")
        .map(|e| e.ts)
        .max()
        .unwrap_or(client_start);
    let total_ms = last_done.saturating_sub(client_start) as f64 / 1e6;
    write_meta(
        &dir.join("meta.json"),
        sc,
        server,
        &plans,
        client_start,
        interval,
        status.is_some(),
        samples.len(),
    )?;

    let max_thr = samples.iter().map(|s| s.threads).max().unwrap_or(0);
    let mean_cost =
        samples.iter().map(|s| s.cost_us).sum::<u64>() as f64 / samples.len().max(1) as f64;
    let mut s = format!(
        "{done}/{} 件完了 {total_ms:7.1} ms, スレッド最大 {max_thr}, サンプル {} 回 (1回 {mean_cost:.0} µs)",
        plans.len(),
        samples.len()
    );
    if errors > 0 {
        s += &format!(", エラー {errors}");
    }
    if status.is_none() {
        s += " ※サーバが時間内に終わらなかったため停止。server_events.csv はありません";
    }
    Ok(s)
}

// ---------------------------------------------------------------------------
// サンプラー：サーバを外から覗く
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct Sample {
    ts: u64,
    cost_us: u64,
    threads: usize,
    rss_kb: u64,
    vsz_kb: u64,
    fds: usize,
    /// これまでに見たすべてのスレッドの CPU 時間の合計（終了したスレッドは最後に見た値）
    cpu_ns: u64,
    /// これまでに見たすべてのスレッドが CPU に載った回数の合計（≒ コンテキストスイッチの回数）
    slices: u64,
    listen_q: u64,
    conns_queued: u64,
    conns_app: u64,
    /// スレッドの内訳（スレッドが DETAIL_THREADS 以下のときだけ）
    by_class: Option<[usize; 7]>,
}

const CLASSES: [&str; 7] = [
    "running", "epoll", "futex", "sleep", "accept", "read", "other",
];

struct ThreadSample {
    ts: u64,
    tid: u32,
    state: char,
    syscall: String,
    run_ns: u64,
}

fn sample_loop(
    pid: u32,
    interval: Duration,
    stop: &AtomicBool,
) -> (Vec<Sample>, Vec<ThreadSample>) {
    let mut out = Vec::new();
    let mut detail = Vec::new();
    let mut seen: HashMap<u32, (u64, u64)> = HashMap::new(); // tid -> (run_ns, slices)
    let start = Instant::now();
    let mut k = 0u32;
    while !stop.load(Ordering::Relaxed) {
        let t0 = Instant::now();
        if let Some(s) = sample(pid, &mut seen, &mut detail) {
            let mut s = s;
            s.cost_us = t0.elapsed().as_micros() as u64;
            out.push(s);
        } else {
            break; // サーバが終了した
        }
        k += 1;
        // 開始時刻基準で次の時刻まで寝る（ずれが積み重ならないように）
        if let Some(d) = (interval * k).checked_sub(start.elapsed()) {
            thread::sleep(d);
        }
    }
    (out, detail)
}

fn sample(
    pid: u32,
    seen: &mut HashMap<u32, (u64, u64)>,
    detail: &mut Vec<ThreadSample>,
) -> Option<Sample> {
    let ts = now_ns();
    let mut s = Sample {
        ts,
        ..Default::default()
    };

    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        let num = || {
            line.split_whitespace()
                .nth(1)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0)
        };
        if line.starts_with("VmRSS:") {
            s.rss_kb = num();
        } else if line.starts_with("VmSize:") {
            s.vsz_kb = num();
        }
    }

    // --- スレッド ---
    let tids: Vec<u32> = fs::read_dir(format!("/proc/{pid}/task"))
        .ok()?
        .filter_map(|e| e.ok()?.file_name().to_str()?.parse().ok())
        .collect();
    s.threads = tids.len();
    let detailed = tids.len() <= DETAIL_THREADS;
    let mut classes = [0usize; 7];
    for &tid in &tids {
        let base = format!("/proc/{pid}/task/{tid}");
        // schedstat: 「CPU に載っていた時間(ns) 実行待ちの時間(ns) CPU に載った回数」
        if let Ok(ss) = fs::read_to_string(format!("{base}/schedstat")) {
            let v: Vec<u64> = ss
                .split_whitespace()
                .filter_map(|x| x.parse().ok())
                .collect();
            if v.len() >= 3 {
                seen.insert(tid, (v[0], v[2]));
            }
        }
        if detailed {
            let state = fs::read_to_string(format!("{base}/stat"))
                .ok()
                .and_then(|st| {
                    st.rsplit_once(')')
                        .and_then(|(_, r)| r.trim_start().chars().next())
                })
                .unwrap_or('?');
            let sc = fs::read_to_string(format!("{base}/syscall")).unwrap_or_default();
            let name = syscall_name(&sc);
            let class = if state == 'R' || name == "running" {
                0
            } else {
                match name.as_str() {
                    "epoll_wait" | "epoll_pwait" | "epoll_pwait2" | "poll" => 1,
                    "futex" => 2,
                    "nanosleep" | "clock_nanosleep" => 3,
                    "accept" | "accept4" => 4,
                    "read" | "recvfrom" => 5,
                    _ => 6,
                }
            };
            classes[class] += 1;
            let run_ns = seen.get(&tid).map(|v| v.0).unwrap_or(0);
            detail.push(ThreadSample {
                ts,
                tid,
                state,
                syscall: name,
                run_ns,
            });
        }
    }
    s.cpu_ns = seen.values().map(|v| v.0).sum();
    s.slices = seen.values().map(|v| v.1).sum();
    s.by_class = detailed.then_some(classes);

    s.fds = fs::read_dir(format!("/proc/{pid}/fd"))
        .map(|d| d.count())
        .unwrap_or(0);

    // --- TCP（サーバ側のポートの行だけ）---
    // 自分のポートで ESTABLISHED: inode が 0 ならまだ accept されていない（カーネルだけが持っている）
    for path in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(table) = fs::read_to_string(path) else {
            continue;
        };
        for line in table.lines().skip(1) {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() < 10
                || f[1]
                    .rsplit(':')
                    .next()
                    .and_then(|p| u16::from_str_radix(p, 16).ok())
                    != Some(PORT)
            {
                continue;
            }
            match f[3] {
                "0A" => {
                    // LISTEN の rx_queue = accept 待ちの接続数
                    s.listen_q += f[4]
                        .split(':')
                        .nth(1)
                        .and_then(|v| u64::from_str_radix(v, 16).ok())
                        .unwrap_or(0);
                }
                "01" | "08" => {
                    if f[9] == "0" {
                        s.conns_queued += 1;
                    } else {
                        s.conns_app += 1;
                    }
                }
                _ => {}
            }
        }
    }
    Some(s)
}

fn syscall_name(sc: &str) -> String {
    let first = sc.split_whitespace().next().unwrap_or("");
    if first == "running" {
        return "running".into();
    }
    let nr: i64 = first.parse().unwrap_or(-2);
    #[cfg(target_arch = "x86_64")]
    let name = match nr {
        0 => "read",
        1 => "write",
        7 => "poll",
        35 => "nanosleep",
        43 => "accept",
        45 => "recvfrom",
        202 => "futex",
        230 => "clock_nanosleep",
        232 => "epoll_wait",
        281 => "epoll_pwait",
        288 => "accept4",
        441 => "epoll_pwait2",
        -1 => "none",
        _ => "",
    };
    #[cfg(target_arch = "aarch64")]
    let name = match nr {
        22 => "epoll_pwait",
        63 => "read",
        64 => "write",
        98 => "futex",
        101 => "nanosleep",
        115 => "clock_nanosleep",
        202 => "accept",
        207 => "recvfrom",
        242 => "accept4",
        441 => "epoll_pwait2",
        -1 => "none",
        _ => "",
    };
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let name = "";
    if name.is_empty() {
        format!("syscall_{nr}")
    } else {
        name.to_string()
    }
}

// ---------------------------------------------------------------------------
// 準備と書き出し
// ---------------------------------------------------------------------------

/// サーバのポートが LISTEN になるまで待つ
fn wait_listening(pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if tcp_lines()
            .iter()
            .any(|(port, st, _)| *port == PORT && st == "0A")
        {
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!("server (pid {pid}) did not start listening on {PORT}");
}

/// 前のサーバの LISTEN と接続が消えるまで待つ
fn wait_port_free() {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if !tcp_lines()
            .iter()
            .any(|(port, st, _)| *port == PORT && matches!(st.as_str(), "0A" | "01" | "08"))
        {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    thread::sleep(Duration::from_millis(200));
}

fn tcp_lines() -> Vec<(u16, String, String)> {
    let mut v = Vec::new();
    for path in ["/proc/net/tcp", "/proc/net/tcp6"] {
        if let Ok(t) = fs::read_to_string(path) {
            for line in t.lines().skip(1) {
                let f: Vec<&str> = line.split_whitespace().collect();
                if f.len() >= 10
                    && let Some(p) = f[1]
                        .rsplit(':')
                        .next()
                        .and_then(|p| u16::from_str_radix(p, 16).ok())
                {
                    v.push((p, f[3].to_string(), f[9].to_string()));
                }
            }
        }
    }
    v
}

fn write_client(path: &Path, ev: &[ClientEvent]) -> std::io::Result<()> {
    let mut w = BufWriter::new(fs::File::create(path)?);
    writeln!(w, "ts_ns,req,kind,arg")?;
    for e in ev {
        writeln!(
            w,
            "{},{},{},{}",
            e.ts,
            e.req,
            e.kind,
            e.arg.replace(',', ";")
        )?;
    }
    w.flush()
}

fn write_samples(path: &Path, s: &[Sample]) -> std::io::Result<()> {
    let mut w = BufWriter::new(fs::File::create(path)?);
    write!(
        w,
        "ts_ns,cost_us,threads,rss_kb,vsz_kb,fds,cpu_ns,slices,listen_q,conns_queued,conns_app"
    )?;
    for c in CLASSES {
        write!(w, ",thr_{c}")?;
    }
    writeln!(w)?;
    for x in s {
        write!(
            w,
            "{},{},{},{},{},{},{},{},{},{},{}",
            x.ts,
            x.cost_us,
            x.threads,
            x.rss_kb,
            x.vsz_kb,
            x.fds,
            x.cpu_ns,
            x.slices,
            x.listen_q,
            x.conns_queued,
            x.conns_app
        )?;
        match x.by_class {
            Some(c) => c.iter().try_for_each(|v| write!(w, ",{v}"))?,
            None => write!(w, "{}", ",".repeat(CLASSES.len()))?,
        }
        writeln!(w)?;
    }
    w.flush()
}

fn write_thread_samples(path: &Path, s: &[ThreadSample]) -> std::io::Result<()> {
    let mut w = BufWriter::new(fs::File::create(path)?);
    writeln!(w, "ts_ns,tid,state,syscall,run_ns")?;
    for x in s {
        writeln!(
            w,
            "{},{},{},{},{}",
            x.ts, x.tid, x.state, x.syscall, x.run_ns
        )?;
    }
    w.flush()
}

#[allow(clippy::too_many_arguments)]
fn write_meta(
    path: &Path,
    sc: &Scenario,
    server: &str,
    plans: &[Plan],
    client_start: u64,
    interval: Duration,
    server_exited: bool,
    n_samples: usize,
) -> std::io::Result<()> {
    let cores = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    let kernel = fs::read_to_string("/proc/sys/kernel/osrelease")
        .unwrap_or_default()
        .trim()
        .to_string();
    let plans_json: Vec<String> = plans
        .iter()
        .map(|p| format!("{{\"offset_ms\":{},\"path\":\"{}\"}}", p.offset_ms, p.path))
        .collect();
    let json = format!(
        "{{\n  \"scenario\": \"{}\",\n  \"about\": \"{}\",\n  \"server\": \"{}\",\n  \"client_start_ns\": {},\n  \"interval_ms\": {},\n  \"samples\": {},\n  \"cores\": {},\n  \"kernel\": \"{}\",\n  \"arch\": \"{}\",\n  \"server_exited\": {},\n  \"plans\": [{}]\n}}\n",
        sc.name,
        sc.about,
        server,
        client_start,
        interval.as_millis(),
        n_samples,
        cores,
        kernel,
        std::env::consts::ARCH,
        server_exited,
        plans_json.join(",")
    );
    fs::write(path, json)
}
