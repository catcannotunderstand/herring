//! サーバ内部イベントの記録。
//!
//! 「一瞬で終わること」（accept、read、epoll_wait から戻った瞬間など）は、外から一定間隔で
//! 覗く方法では原理的に写らない。そこでサーバ自身に、起きた瞬間の時刻を記録させる。
//!
//! - 時刻は CLOCK_MONOTONIC（ナノ秒）。同じマシン上の別プロセス（クライアントやサンプラー）とも
//!   同じ時計なので、あとで1本の時間軸に並べられる。
//! - スレッド ID は OS の tid（gettid）。/proc/<pid>/task/<tid> と突き合わせられる。
//! - 記録はメモリ上の Vec に積むだけで、ファイルへの書き出しは終了時に1回。
//!   記録そのものがサーバの動きを乱さないようにするため（1件あたり数十ナノ秒）。

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Event {
    pub ts: u64,
    pub tid: u32,
    pub conn: u64,
    pub kind: &'static str,
    pub arg: String,
}

static ENABLED: AtomicBool = AtomicBool::new(false);
static EVENTS: Mutex<Vec<Event>> = Mutex::new(Vec::new());

/// 記録を有効にする。無効のままなら emit は何もしない。
pub fn enable() {
    EVENTS.lock().unwrap().reserve(64 * 1024);
    ENABLED.store(true, Ordering::Relaxed);
}

pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// CLOCK_MONOTONIC をナノ秒で返す
pub fn now_ns() -> u64 {
    clock_ns(libc::CLOCK_MONOTONIC)
}

/// このスレッドが実際に CPU を使った時間（ナノ秒）
pub fn thread_cpu_ns() -> u64 {
    clock_ns(libc::CLOCK_THREAD_CPUTIME_ID)
}

fn clock_ns(clock: libc::clockid_t) -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts は有効な書き込み先
    unsafe { libc::clock_gettime(clock, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// OS が付けたスレッド ID
#[cfg(target_os = "linux")]
pub fn tid() -> u32 {
    // gettid は本物のシステムコールなので、スレッドごとに1回だけ呼んで覚えておく
    // SAFETY: 引数なしのシステムコール
    thread_local!(static ID: u32 = unsafe { libc::gettid() as u32 });
    ID.with(|id| *id)
}

#[cfg(not(target_os = "linux"))]
pub fn tid() -> u32 {
    use std::sync::atomic::AtomicU32;
    static NEXT: AtomicU32 = AtomicU32::new(1);
    thread_local!(static ID: u32 = NEXT.fetch_add(1, Ordering::Relaxed));
    ID.with(|id| *id)
}

/// イベントを1件記録する。conn = 0 はスレッドそのものの出来事。
pub fn emit(conn: u64, kind: &'static str, arg: impl Into<String>) {
    if !enabled() {
        return;
    }
    let ev = Event {
        ts: now_ns(),
        tid: tid(),
        conn,
        kind,
        arg: arg.into(),
    };
    EVENTS.lock().unwrap().push(ev);
}

/// 記録したイベントを CSV に書き出す。列: ts_ns,tid,conn,kind,arg
pub fn dump(path: &str) -> std::io::Result<()> {
    let events = std::mem::take(&mut *EVENTS.lock().unwrap());
    let mut w = BufWriter::new(File::create(path)?);
    writeln!(w, "ts_ns,tid,conn,kind,arg")?;
    for e in &events {
        writeln!(
            w,
            "{},{},{},{},{}",
            e.ts,
            e.tid,
            e.conn,
            e.kind,
            e.arg.replace(',', ";")
        )?;
    }
    w.flush()
}
