use chrono::Duration;
use common::create_scheduler;
use sync_scheduler::JobBuilder;

mod common;

#[test]
fn test_job_with_single_condition() {
    let scheduler = create_scheduler();
    let job = JobBuilder::default()
        .repeating(Duration::milliseconds(200))
        .add_condition(|uuid, scheduler| {
            let job = scheduler.get_job(uuid).unwrap();
            let job_guard = job.read().unwrap();
            job_guard.executions() < 1
        })
        .build(|uuid, _| {
            println!("Job {uuid} executed");
            Ok(())
        });

    let job_id = scheduler.add_job(job).unwrap();

    scheduler.start();
    std::thread::sleep(std::time::Duration::from_millis(500));
    scheduler.stop();

    let job = scheduler.get_job(job_id).unwrap();
    let job_guard = job.read().unwrap();
    assert!(job_guard.executions() == 1);
}
