use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use chrono::Utc;
use rscron::{Job, JobBuilder, Scheduler, SchedulerConfig};

#[allow(unused)]
pub const TEST_TIMEOUT: Duration = Duration::from_secs(5000);

pub fn create_scheduler() -> Scheduler {
    Scheduler::new(SchedulerConfig::default())
}

#[allow(unused)]
pub fn create_counter_job(counter: Arc<Mutex<usize>>, interval: chrono::Duration) -> Job {
    let counter_clone = Arc::clone(&counter);
    JobBuilder::default().repeating(interval).build(move || {
        let mut count = counter_clone.lock().unwrap();
        *count += 1;
    })
}

#[allow(unused)]
pub fn wait_and_assert(duration: Duration, message: &str, condition: impl Fn() -> bool) {
    let start = Utc::now();
    while Utc::now() - start < chrono::Duration::from_std(duration).unwrap() {
        if condition() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("{}", message);
}

#[allow(unused)]
pub fn run_with_timeout<T, F>(timeout: Duration, test: F) -> T
where
    T: Send + 'static,
    F: FnOnce() -> T,
    F: Send + 'static,
{
    let (sender, receiver) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let result = test();
        let _ = sender.send(result);
    });

    match receiver.recv_timeout(timeout) {
        Ok(result) => {
            handle.join().unwrap();
            result
        }
        Err(_) => {
            panic!("Test timed out after {:?}", timeout);
        }
    }
}
