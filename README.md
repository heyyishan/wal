# forge-steal

**A compact Rust work-stealing thread pool for irregular CPU workloads.**

`forge-steal` uses one private deque per worker, a shared submission injector, and randomized victim selection. It is designed to reduce the contention that appears when every worker repeatedly locks the same queue, especially when task durations vary substantially.

> **Benchmark honesty:** this repository includes reproducible Criterion benchmarks, but no benchmark numbers are published here. The build environment used to assemble this archive did not have `cargo`/`rustc` installed, so reporting speedups would be fiction. Run the commands below on the target machine and publish the resulting Criterion output with the machine details.

## Features

- Per-worker LIFO deques with batch stealing.
- Shared injector for submissions from arbitrary threads.
- Randomized victim starting points to reduce synchronized stealing collisions.
- Atomic outstanding-task accounting and blocking `wait()`.
- Explicit shutdown that rejects new submissions, drains accepted work, and joins workers.
- Panicking jobs do not kill worker threads or strand the completion counter.
- Small safe API. No `unsafe` code in this crate.

## Quick start

```rust
use forge_steal::ThreadPool;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

fn main() {
    let pool = ThreadPool::new(4);
    let total = Arc::new(AtomicUsize::new(0));

    for _ in 0..10_000 {
        let total = Arc::clone(&total);
        pool.spawn(move || {
            total.fetch_add(1, Ordering::Relaxed);
        }).expect("pool is running");
    }

    pool.wait();
    assert_eq!(total.load(Ordering::Relaxed), 10_000);
    // Drop also shuts down and joins the workers.
}
```

A `Spawner` can be cloned and moved into producer threads:

```rust
let pool = forge_steal::ThreadPool::new(4);
let submit = pool.spawner();

std::thread::spawn(move || {
    for item in 0..1_000 {
        submit.spawn(move || process(item)).unwrap();
    }
});

pool.wait();
```

`wait()` waits for the outstanding count to reach zero. If producers are still submitting concurrently, it may observe a temporary zero and return before a later submission. Join producer threads before calling `wait()` when you need a closed submission phase.

## Architecture

```text
                    external producers
                            |
                            v
                    +----------------+
                    | global Injector|
                    +----------------+
                       ^     ^     ^
                  batch|     |     |batch
                       |     |     |
              +--------+     |     +--------+
              |              |              |
        +-----------+  +-----------+  +-----------+
        | Worker 0  |  | Worker 1  |  | Worker N  |
        | local deque| | local deque| | local deque|
        +-----------+  +-----------+  +-----------+
              ^              ^              ^
              +------ steal--+---steal-------+
```

### Scheduling path

1. External submissions are pushed into `crossbeam_deque::Injector`.
2. A worker first pops from its own deque. Local work is the cheapest path and is preferred.
3. If its deque is empty, it attempts to move a batch from the injector into its local deque.
4. If the injector is empty, it probes peer stealers, beginning at a per-worker pseudo-random victim index. A successful batch steal moves work into the thief's local deque.
5. If no work is available, the worker parks briefly. Submission unparks one worker; shutdown unparks all workers.

The local queues are LIFO. This tends to keep recently spawned/rediscovered work close to the worker and can improve cache locality. Stealing batches from the opposite end spreads older work to idle workers and amortizes coordination overhead.

### Why a shared injector still exists

A work-stealing pool needs a safe entry point for work submitted by arbitrary application threads. The injector serves that purpose. It is not the steady-state execution queue: workers move batches out of it and then execute from private deques. This avoids putting a mutex around every task acquisition.

### Deque implementation and memory safety

The queue layer is supplied by [`crossbeam-deque`](https://docs.rs/crossbeam-deque/). Its worker/stealer model provides the work-stealing deque operations and the associated atomic synchronization. This crate does not claim to implement the Chase-Lev algorithm internally. It composes a proven deque implementation into a scheduler, where victim selection, injection, wakeups, task accounting, panic containment, and shutdown policy live.

That is a deliberate tradeoff. A home-grown Chase-Lev deque is an excellent research exercise, but correct resizing and reclamation are subtle: the last-item pop races with thieves, and old buffers cannot be reclaimed while a thief may still hold a pointer. Avoiding `unsafe` here makes the scheduler easier to audit without pretending those problems are trivial.

## Synchronization and memory ordering

| State | Primitive / ordering | Purpose |
|---|---|---|
| Job queues | `crossbeam-deque` atomics | Publish, pop, and steal jobs safely |
| Outstanding count | `fetch_add` / `fetch_sub` with `AcqRel`; reads with `Acquire` | Track accepted but unfinished jobs |
| Shutdown flag | `Release` store, `Acquire` loads | Publish shutdown after submission gate closes |
| Submission gate | `Mutex<()>` | Prevent shutdown from racing with acceptance/accounting/publication |
| Completion wait | `Mutex<()>` + `Condvar` | Sleep until outstanding work reaches zero without a lost wakeup |
| Wake cursor | `Relaxed` atomic increment | Select a worker to unpark; it does not publish task data |

The submission gate is intentionally narrow. It serializes the acceptance boundary, not task execution. The outstanding count is incremented before a job is made visible in the injector, so a worker cannot complete a job before it has been counted.

On completion, a worker decrements the count. The worker that observes the transition from one to zero takes the completion mutex and notifies waiters. `wait()` checks the predicate while holding that same mutex, avoiding the classic condition-variable lost-wakeup race.

## API and semantics

- `ThreadPool::new(worker_count)`: create a fixed-size pool; zero workers is rejected.
- `ThreadPool::spawn(job)`: submit a `Send + 'static` closure.
- `ThreadPool::spawner()`: create a cloneable `Spawner` for producer threads.
- `ThreadPool::wait()`: block until the outstanding count is zero.
- `ThreadPool::outstanding_tasks()`: return the current count.
- `ThreadPool::shutdown()`: stop accepting work, drain accepted work, and join workers. Idempotent.

A job panic is caught at the worker boundary. The panic hook may still print a message, but the worker survives and the completion count is decremented. Panics are not propagated to the thread that submitted the job. Applications that need per-task error reporting should return results through channels or shared result slots.

A `Spawner` may outlive the pool handle, but submissions fail once shutdown starts. Shutdown does not cancel accepted jobs.

## Benchmarks

The Criterion suite compares this pool against a deliberately simple baseline: `Arc<Mutex<VecDeque<Job>>>`, where every worker locks the same queue to take its next task.

Two workloads are included:

| Workload | Task distribution | What it stresses |
|---|---|---|
| `balanced` | 20,000 tasks, each doing 80 iterations | Queue overhead when task costs are similar |
| `irregular` | 20,000 tasks; every 100th task does 20,000 iterations, the rest do 10 | Load imbalance and the ability to redistribute work |

Run on a machine with Rust installed:

```sh
cargo bench
```

Criterion writes detailed estimates and samples under `target/criterion/`. For a useful comparison, record:

- CPU model and physical/logical core count
- OS and kernel version
- `rustc --version`
- power/performance mode and whether the machine was otherwise idle
- Criterion median/mean estimates and confidence intervals for all four cases

Do not compare numbers from different machines as if they were controlled experiments. Also note that these benchmarks include pool/thread creation in each measured iteration. That measures end-to-end short-lived pool usage, not only steady-state scheduling. For a production evaluation, add a second benchmark that reuses a persistent pool and synchronizes each iteration with a barrier or completion counter.

### Expected behavior, not claimed measurements

- On balanced tiny tasks, a mutex queue can be competitive because it is simple and the scheduler overhead may dominate useful work.
- On irregular workloads, local execution plus batch stealing should reduce the central queue's lock traffic and let idle workers take work from busy workers.
- If tasks are extremely coarse, both schedulers may spend little time in queue coordination; the difference can shrink.
- If tasks are extremely fine, task granularity and wakeup policy become decisive. Batching helps, but this implementation is not a substitute for benchmarking the actual workload.

## Tradeoffs and next steps

This is a compact baseline, not a production runtime. In particular:

- External tasks enter through the injector. There is no task-local spawn API that directly pushes onto the currently executing worker's deque.
- Idle workers use a short timed park rather than a full event-count protocol. This is simple and robust, but periodic wakeups consume a little power and can add latency.
- Victim selection uses a lightweight per-worker xorshift generator, not a cryptographic RNG.
- There is no task prioritization, cancellation, scoped borrowing, work-stealing-aware async executor, or NUMA topology awareness.
- A fixed worker count is used. The caller chooses it, typically from `available_parallelism()`.

Promising next steps are a worker-local task context, a sequence-based parking protocol, persistent-pool benchmarks, and workload-specific batch-size tuning. Each should be measured independently; concurrency code has enough folklore already.

## Build and test

```sh
cargo test
cargo test --release
cargo bench
```

## License

Dual-licensed under MIT or Apache-2.0.
