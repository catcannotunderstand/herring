//! trace::emit 1回にかかる時間を測る
//!   cargo run --release --example trace_overhead
use herring::trace;

fn main() {
    trace::enable();
    let n = 1_000_000u64;
    for (label, threads) in [("1スレッド", 1usize), ("4スレッド同時", 4)] {
        let t0 = trace::now_ns();
        std::thread::scope(|s| {
            for _ in 0..threads {
                s.spawn(|| {
                    for i in 0..n / threads as u64 {
                        trace::emit(i, "bench", "");
                    }
                });
            }
        });
        let ns = (trace::now_ns() - t0) as f64 / n as f64;
        println!("{label}: emit 1回あたり 平均 {ns:.0} ns（{n} 回）");
    }
}
