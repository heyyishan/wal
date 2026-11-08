//! A small work-stealing thread pool.
//!
//! External submissions enter a shared injector. Each worker owns a local
//! deque and periodically pulls batches from the injector or steals from peers.
//! Queue mechanics and memory reclamation are provided by `crossbeam-deque`.

use crossbeam_deque::{Injector, Stealer, Steal, Worker};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle, Thread};
use std::time::Duration;

 type Job = Box<dyn FnOnce() + Send + 'static>;

struct Shared {
    injector: Injector<Job>,
    stealers: Vec<Stealer<Job>>,
    outstanding: AtomicUsize,
    shutting_down: AtomicBool,
    submit_gate: Mutex<()>,
    completion_lock: Mutex<()>,
    completed: Condvar,
    threads: Mutex<Vec<Thread>>,
    wake_cursor: AtomicUsize,
}

/// A cloneable handle for submitting work without owning the pool.
#[derive(Clone)]
pub struct Spawner {
    shared: Arc<Shared>,
}

impl Spawner {
    /// Submit a `Send + 'static` job. Returns an error after shutdown begins.
    pub fn spawn<F>(&self, job: F) -> Result<(), SpawnError>
    where F: FnOnce() + Send + 'static {
        submit(&self.shared, Box::new(job))
    }
}

/// A fixed-size work-stealing pool.
pub struct ThreadPool {
    shared: Arc<Shared>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

/// Returned when a task is submitted after shutdown has started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpawnError;

impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("thread pool is shutting down")
    }
}
impl std::error::Error for SpawnError {}

impl ThreadPool {
    /// Create a pool with `n` workers. Zero is rejected.
    pub fn new(n: usize) -> Self {
        assert!(n > 0, "a thread pool needs at least one worker");
        let locals: Vec<Worker<Job>> = (0..n).map(|_| Worker::new_lifo()).collect();
        let stealers = locals.iter().map(Worker::stealer).collect::<Vec<_>>();
        let shared = Arc::new(Shared {
            injector: Injector::new(), stealers,
            outstanding: AtomicUsize::new(0),
            shutting_down: AtomicBool::new(false),
            submit_gate: Mutex::new(()), completion_lock: Mutex::new(()),
            completed: Condvar::new(), threads: Mutex::new(Vec::with_capacity(n)),
            wake_cursor: AtomicUsize::new(0),
        });
        let mut handles = Vec::with_capacity(n);
        for (index, local) in locals.into_iter().enumerate() {
            let state = Arc::clone(&shared);
            handles.push(thread::Builder::new()
                .name(format!("forge-steal-{index}"))
                .spawn(move || worker_loop(index, local, state))
                .expect("failed to spawn worker"));
        }
        Self { shared, workers: Mutex::new(handles) }
    }

    /// Submit a job to the global injector.
    pub fn spawn<F>(&self, job: F) -> Result<(), SpawnError>
    where F: FnOnce() + Send + 'static {
        submit(&self.shared, Box::new(job))
    }

    /// Obtain a lightweight cloneable submission handle.
    pub fn spawner(&self) -> Spawner { Spawner { shared: Arc::clone(&self.shared) } }

    /// Number of submitted jobs that have not finished yet.
    pub fn outstanding_tasks(&self) -> usize {
        self.shared.outstanding.load(Ordering::Acquire)
    }

    /// Block until all jobs submitted so far (including jobs submitted by
    /// other `Spawner`s before shutdown) have completed.
    pub fn wait(&self) {
        let mut guard = self.shared.completion_lock.lock().unwrap();
        while self.shared.outstanding.load(Ordering::Acquire) != 0 {
            guard = self.shared.completed.wait(guard).unwrap();
        }
    }

    /// Stop accepting work, drain accepted work, and join all workers.
    /// Safe to call more than once.
    pub fn shutdown(&self) {
        {
            let _gate = self.shared.submit_gate.lock().unwrap();
            self.shared.shutting_down.store(true, Ordering::Release);
        }
        self.unpark_all();
        let mut handles = self.workers.lock().unwrap();
        for handle in handles.drain(..) { let _ = handle.join(); }
    }

    fn unpark_all(&self) {
        for thread in self.shared.threads.lock().unwrap().iter() { thread.unpark(); }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) { self.shutdown(); }
}

fn submit(shared: &Arc<Shared>, job: Job) -> Result<(), SpawnError> {
    let _gate = shared.submit_gate.lock().unwrap();
    if shared.shutting_down.load(Ordering::Acquire) { return Err(SpawnError); }
    // Increment before publishing the job, so a worker cannot finish it before
    // it is represented in the completion count.
    shared.outstanding.fetch_add(1, Ordering::AcqRel);
    shared.injector.push(job);
    let threads = shared.threads.lock().unwrap();
    if !threads.is_empty() {
        let i = shared.wake_cursor.fetch_add(1, Ordering::Relaxed) % threads.len();
        threads[i].unpark();
    }
    Ok(())
}

fn worker_loop(index: usize, local: Worker<Job>, shared: Arc<Shared>) {
    shared.threads.lock().unwrap().push(thread::current());
    let mut rng = (index as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    loop {
        if let Some(job) = find_job(&local, &shared, &mut rng) {
            // A panicking task must not kill a worker or leak the completion count.
            let _ = catch_unwind(AssertUnwindSafe(job));
            if shared.outstanding.fetch_sub(1, Ordering::AcqRel) == 1 {
                let _guard = shared.completion_lock.lock().unwrap();
                shared.completed.notify_all();
            }
            continue;
        }
        if shared.shutting_down.load(Ordering::Acquire)
            && shared.outstanding.load(Ordering::Acquire) == 0 { break; }
        thread::park_timeout(Duration::from_micros(200));
    }
}

fn find_job(local: &Worker<Job>, shared: &Shared, rng: &mut u64) -> Option<Job> {
    if let Some(job) = local.pop() { return Some(job); }

    // Move a batch from the injector into this worker's private deque. This
    // amortizes global queue traffic across multiple tasks.
    loop {
        match shared.injector.steal_batch_and_pop(local) {
            Steal::Success(job) => return Some(job),
            Steal::Retry => continue,
            Steal::Empty => break,
        }
    }

    let n = shared.stealers.len();
    if n <= 1 { return None; }
    // Xorshift64*: cheap per-worker victim randomization avoids synchronized
    // workers repeatedly colliding on the same victim.
    *rng ^= *rng >> 12;
    *rng ^= *rng << 25;
    *rng ^= *rng >> 27;
    let start = ((*rng).wrapping_mul(0x2545_F491_4F6C_DD1D) as usize) % n;
    for offset in 0..n {
        let victim = (start + offset) % n;
        if let Some(job) = match shared.stealers[victim].steal_batch_and_pop(local) {
            Steal::Success(job) => Some(job),
            Steal::Retry | Steal::Empty => None,
        } { return Some(job); }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn executes_all_jobs() {
        let pool = ThreadPool::new(4);
        let count = Arc::new(AtomicUsize::new(0));
        for _ in 0..10_000 {
            let count = Arc::clone(&count);
            pool.spawn(move || { count.fetch_add(1, Ordering::Relaxed); }).unwrap();
        }
        pool.wait();
        assert_eq!(count.load(Ordering::Relaxed), 10_000);
    }

    #[test]
    fn spawner_can_submit_and_wait() {
        let pool = ThreadPool::new(2);
        let spawner = pool.spawner();
        let count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&count);
        spawner.spawn(move || { c.fetch_add(1, Ordering::Relaxed); }).unwrap();
        pool.wait();
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn task_panic_does_not_kill_pool() {
        let pool = ThreadPool::new(2);
        pool.spawn(|| panic!("intentional task panic")).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&count);
        pool.spawn(move || { c.fetch_add(1, Ordering::Relaxed); }).unwrap();
        pool.wait();
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn rejects_work_after_shutdown() {
        let pool = ThreadPool::new(1);
        pool.shutdown();
        assert!(pool.spawn(|| {}).is_err());
    }
}
