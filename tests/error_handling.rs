// tests/test_error_handling.rs

use chrono::Duration;
use rscron::JobBuilder;
use std::sync::{Arc, Mutex};

mod common;
use common::*;

#[test]
fn test_job_panic() {
    let scheduler = create_scheduler();
    let panic_counter = Arc::new(Mutex::new(0));
    let on_fail_counter = Arc::new(Mutex::new(0));

    let panic_counter_clone = Arc::clone(&panic_counter);
    let on_fail_counter_clone = Arc::clone(&on_fail_counter);

    let job = JobBuilder::default()
        .repeating(Duration::milliseconds(100))
        .on_fail(move |_, _| {
            let mut count = on_fail_counter_clone.lock().unwrap();
            *count += 1;
        })
        .build(move || {
            let mut count = panic_counter_clone.lock().unwrap();
            *count += 1;
            if *count % 2 == 0 {
                panic!("Simulated panic");
            }
        });

    scheduler.add_job(job).unwrap();
    scheduler.start();
    std::thread::sleep(std::time::Duration::from_millis(550));
    scheduler.stop();

    assert!(
        *panic_counter.lock().unwrap() >= 5,
        "Job should have run at least 5 times"
    );
    assert!(
        *on_fail_counter.lock().unwrap() >= 2,
        "on_fail should have been called at least 2 times"
    );
}

#[test]
fn test_max_retries() {
    let scheduler = create_scheduler();
    let attempt_counter = Arc::new(Mutex::new(0));
    let max_retries = 3;

    let attempt_counter_clone = Arc::clone(&attempt_counter);

    let job = JobBuilder::default().once().build(move || {
        let mut count = attempt_counter_clone.lock().unwrap();
        *count += 1;
        if *count <= max_retries {
            panic!("Simulated failure");
        }
    });

    scheduler.add_job(job).unwrap();
    scheduler.start();
    std::thread::sleep(std::time::Duration::from_secs(1));
    scheduler.stop();

    assert_eq!(
        *attempt_counter.lock().unwrap(),
        max_retries + 1,
        "Job should have been retried exactly 3 times"
    );
}
