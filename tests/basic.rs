use std::{sync::Arc, thread, time::Instant};

use chrono::Duration;
use common::{create_counter_job, create_scheduler, run_with_timeout, TEST_TIMEOUT};
use rscron::{arc_mutex, JobBuilder};

mod common;

#[test]
fn test_add_and_removeed_job() {
    run_with_timeout(TEST_TIMEOUT, || {
        let scheduler = create_scheduler();
        let counter = arc_mutex!(0);
        let job = create_counter_job(counter.clone(), Duration::milliseconds(100));

        let job_id = scheduler.add_job(job).unwrap();
        assert!(scheduler.jobs().contains_key(&job_id));

        scheduler.remove_job(job_id);
        assert!(!scheduler.jobs().contains_key(&job_id));
    });
}

#[test]
fn test_update_job() {
    run_with_timeout(TEST_TIMEOUT, || {
        let scheduler = create_scheduler();
        let counter1 = arc_mutex!(0);
        let counter2 = arc_mutex!(0);

        let job = create_counter_job(counter1.clone(), Duration::milliseconds(100));
        let job_id = scheduler.add_job(job).unwrap();

        let new_job = create_counter_job(counter2.clone(), Duration::milliseconds(100));
        scheduler.update_job(job_id, new_job).unwrap();

        scheduler.start();
        thread::sleep(std::time::Duration::from_millis(250));
        scheduler.stop();

        assert_eq!(*counter1.lock().unwrap(), 0);
        assert!(*counter2.lock().unwrap() > 0);
    });
}

#[test]
fn test_add_job_while_running() {
    run_with_timeout(TEST_TIMEOUT, || {
        let scheduler = create_scheduler();
        let counter1 = arc_mutex!(0);
        let counter2 = arc_mutex!(0);
        let now = Instant::now();

        // let job1 = create_counter_job(counter1.clone(), Duration::milliseconds(100));
        let counter_clone = Arc::clone(&counter1);
        let job1 = JobBuilder::default()
            .repeating(Duration::milliseconds(100))
            .build(move |_, _| {
                let since = now.elapsed();
                println!("elapsed {:?}", since);
                let mut count = counter_clone.lock().unwrap();
                *count += 1;
            });
        scheduler.add_job(job1).unwrap();

        scheduler.start();
        thread::sleep(std::time::Duration::from_millis(250));

        let job2 = create_counter_job(counter2.clone(), Duration::milliseconds(100));
        scheduler.add_job(job2).unwrap();

        thread::sleep(std::time::Duration::from_millis(250));
        scheduler.stop();

        assert!(*counter1.lock().unwrap() > 1);
        assert!(*counter2.lock().unwrap() > 0);
    });
}

#[test]
fn test_stop_and_restart() {
    run_with_timeout(TEST_TIMEOUT, || {
        let scheduler = create_scheduler();
        let counter = arc_mutex!(0);
        let job = create_counter_job(counter.clone(), Duration::milliseconds(50));

        scheduler.add_job(job).unwrap();
        scheduler.start();
        thread::sleep(std::time::Duration::from_millis(200));
        scheduler.stop();

        let count_after_first_run = *counter.lock().unwrap();
        assert!(
            count_after_first_run > 0,
            "Job should have run at least once"
        );

        thread::sleep(std::time::Duration::from_millis(200));
        let count_after_stop = *counter.lock().unwrap();
        assert_eq!(
            count_after_stop, count_after_first_run,
            "Counter should not increase while scheduler is stopped"
        );

        scheduler.start();
        thread::sleep(std::time::Duration::from_millis(200));
        scheduler.stop();

        let count_after_second_run = *counter.lock().unwrap();
        assert!(
            count_after_second_run > count_after_first_run,
            "Job should run again after restart"
        );
    });
}
