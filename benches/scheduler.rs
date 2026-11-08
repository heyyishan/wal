use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use forge_steal::ThreadPool;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread;

const TASKS: usize = 20_000;

fn work(iterations: u64) -> u64 {
    let mut x = 0x1234_5678_u64;
    for _ in 0..iterations { x = x.wrapping_mul(6364136223846793005).wrapping_add(1); }
    black_box(x)
}

fn shared_queue(tasks: &[u64], workers: usize) {
    let queue = Arc::new(Mutex::new(VecDeque::from(tasks.to_vec())));
    let mut joins = Vec::new();
    for _ in 0..workers {
        let q = Arc::clone(&queue);
        joins.push(thread::spawn(move || loop {
            let item = q.lock().unwrap().pop_front();
            match item { Some(n) => { black_box(work(n)); }, None => break }
        }));
    }
    for j in joins { j.join().unwrap(); }
}

fn work_stealing(tasks: &[u64], workers: usize) {
    let pool = ThreadPool::new(workers);
    for &n in tasks { pool.spawn(move || { black_box(work(n)); }).unwrap(); }
    pool.wait();
}

fn scheduler_benches(c: &mut Criterion) {
    let workers = thread::available_parallelism().map_or(4, usize::from).min(8);
    let mut group = c.benchmark_group("scheduler_comparison");
    for (name, tasks) in [
        ("balanced", vec![80_u64; TASKS]),
        ("irregular", (0..TASKS).map(|i| if i % 100 == 0 { 20_000 } else { 10 }).collect()),
    ] {
        group.bench_with_input(BenchmarkId::new("work_stealing", name), &tasks, |b, t| {
            b.iter(|| work_stealing(black_box(t), workers));
        });
        group.bench_with_input(BenchmarkId::new("mutex_shared_queue", name), &tasks, |b, t| {
            b.iter(|| shared_queue(black_box(t), workers));
        });
    }
    group.finish();
}
criterion_group!(benches, scheduler_benches);
criterion_main!(benches);
