use std::sync::Arc;

use chrono::Utc;
use common::create_scheduler;
use sync_scheduler::{arc_mutex, JobBuilder};

mod common;

#[test]
fn test_simple_dependency() {
    let scheduler = create_scheduler();
    let dep_completed = arc_mutex!(false);
    let dep_completed_clone = Arc::clone(&dep_completed);
    let dep_completed_clone2 = Arc::clone(&dep_completed);
    let main_executed = arc_mutex!(false);
    let main_executed_clone = Arc::clone(&main_executed);

    let dep_job = JobBuilder::default().once().build(move |_, _| {
        std::thread::sleep(std::time::Duration::from_millis(100));
        *dep_completed_clone.lock().unwrap() = true;

        Ok(())
    });

    let dep_job_id = scheduler.add_job(dep_job).unwrap();

    let main_job = JobBuilder::default()
        .once()
        .depends_on(dep_job_id)
        .build(move |_, _| {
            assert!(
                *dep_completed_clone2.lock().unwrap(),
                "Dependency should be completed before main job runs"
            );
            *main_executed_clone.lock().unwrap() = true;

            Ok(())
        });

    scheduler.add_job(main_job).unwrap();
    scheduler.start();
    std::thread::sleep(std::time::Duration::from_millis(500));
    scheduler.stop();

    assert!(
        *dep_completed.lock().unwrap(),
        "Dependency job should have completed"
    );
    assert!(
        *main_executed.lock().unwrap(),
        "Main job should have executed"
    );
}

#[test]
fn test_multiple_dependencies() {
    let scheduler = create_scheduler();
    let dep_counters = vec![arc_mutex!(0), arc_mutex!(0), arc_mutex!(0)];
    let main_executed = arc_mutex!(false);
    let main_executed_clone = Arc::clone(&main_executed);

    let mut dep_job_ids = Vec::new();

    let now = Utc::now();
    for counter in &dep_counters {
        let counter_clone = Arc::clone(counter);
        let job = JobBuilder::default()
            .with_tag("counter")
            .start_time(now)
            .once()
            .build(move |_, _| {
                let mut count = counter_clone.lock().unwrap();
                *count += 1;

                Ok(())
            });
        let job_id = scheduler.add_job(job).unwrap();
        dep_job_ids.push(job_id);
    }

    let main_job = JobBuilder::default()
        .once()
        .with_tag("main")
        .depends_on(dep_job_ids[0])
        .depends_on(dep_job_ids[1])
        .depends_on(dep_job_ids[2])
        .build(move |_, _| {
            for counter in &dep_counters {
                assert!(
                    *counter.lock().unwrap() > 0,
                    "All dependencies should have run at least once"
                );
            }
            *main_executed_clone.lock().unwrap() = true;

            Ok(())
        });

    scheduler.add_job(main_job).unwrap();
    scheduler.start();
    std::thread::sleep(std::time::Duration::from_millis(500));
    scheduler.stop();

    assert!(
        *main_executed.lock().unwrap(),
        "Main job should have executed"
    );
}

#[test]
fn test_dependency_chain() {
    let scheduler = create_scheduler();
    let counters = vec![arc_mutex!(0), arc_mutex!(0), arc_mutex!(0)];

    let mut prev_job_id = None;

    for (i, counter) in counters.clone().into_iter().enumerate() {
        let mut job_builder = JobBuilder::default().once();
        if let Some(id) = prev_job_id {
            job_builder = job_builder.depends_on(id);
        }
        let job = job_builder.build(move |_, _| {
            std::thread::sleep(std::time::Duration::from_millis(100));
            let mut count = counter.lock().unwrap();
            *count = i + 1;

            Ok(())
        });

        let job_id = scheduler.add_job(job).unwrap();
        prev_job_id = Some(job_id);
    }

    scheduler.start();
    std::thread::sleep(std::time::Duration::from_millis(1000));
    scheduler.stop();

    for (i, counter) in counters.iter().enumerate() {
        assert_eq!(
            *counter.lock().unwrap(),
            i + 1,
            "Job {} should have executed",
            i + 1
        );
    }
}
