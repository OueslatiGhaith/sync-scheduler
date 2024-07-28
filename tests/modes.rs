use std::sync::Arc;

use chrono::Duration;
use common::{create_counter_job, create_scheduler};
use rscron::{arc_mutex, JobBuilder};

mod common;

#[test]
fn test_once_mode() {
    let scheduler = create_scheduler();
    let counter = arc_mutex!(0);
    let counter_clone = Arc::clone(&counter);

    let job = JobBuilder::default().once().build(move |_, _| {
        let mut count = counter_clone.lock().unwrap();
        *count += 1;
    });

    scheduler.add_job(job).unwrap();
    scheduler.start();
    std::thread::sleep(std::time::Duration::from_millis(500));
    scheduler.stop();

    assert_eq!(*counter.lock().unwrap(), 1);
}

#[test]
fn test_repeating_mode() {
    let scheduler = create_scheduler();
    let counter = arc_mutex!(0);

    let job = create_counter_job(counter.clone(), Duration::milliseconds(100));
    scheduler.add_job(job).unwrap();

    scheduler.start();
    std::thread::sleep(std::time::Duration::from_millis(550));
    scheduler.stop();

    assert!(*counter.lock().unwrap() >= 5);
}

#[test]
fn test_limited_mode() {
    let scheduler = create_scheduler();
    let counter = arc_mutex!(0);
    let counter_clone = Arc::clone(&counter);

    let job = JobBuilder::default()
        .limited(3, Duration::milliseconds(100))
        .build(move |_, _| {
            let mut count = counter_clone.lock().unwrap();
            *count += 1;
        });

    scheduler.add_job(job).unwrap();
    scheduler.start();
    std::thread::sleep(std::time::Duration::from_secs(1));
    scheduler.stop();

    assert_eq!(*counter.lock().unwrap(), 3);
}
