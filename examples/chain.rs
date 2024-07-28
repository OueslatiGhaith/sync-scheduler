use std::sync::{Arc, Mutex};

use chrono::Duration;
use sync_scheduler::{arc_mutex, Job, JobBuilder, Scheduler, SchedulerConfig};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

fn make_repeating_job(update_counter: Arc<Mutex<usize>>) -> Job {
    let rand_id = rand::random::<u8>();

    JobBuilder::default()
        .repeating(Duration::seconds(5))
        .build(move |_, _| {
            let counter = *update_counter.lock().unwrap();
            info!("[{rand_id}] repeating job updated {counter} times");

            Ok(())
        })
}

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let config = SchedulerConfig::default();
    let scheduler = Scheduler::new(config);

    let update_counter = arc_mutex!(0);
    let update_counter_clone = Arc::clone(&update_counter);

    let repeating_job = make_repeating_job(update_counter);
    let repeating_job_id = repeating_job.id();

    let scheduler_job =
        JobBuilder::default()
            .repeating(Duration::seconds(5))
            .build(move |_, scheduler| {
                info!("outer job running");
                let update_counter_clone2 = Arc::clone(&update_counter_clone);
                let update_counter_clone3 = Arc::clone(&update_counter_clone);

                // adding a new job to the scheduler
                let once_job = JobBuilder::default().once().build(move |_, _| {
                    let counter = *update_counter_clone2.lock().unwrap();
                    info!("inner once job ran {counter} times");

                    Ok(())
                });
                scheduler.add_job(once_job).unwrap();

                // updating an existing job
                let mut counter_lock = update_counter_clone.lock().unwrap();
                *counter_lock += 1;
                drop(counter_lock);

                let new_job = make_repeating_job(update_counter_clone3);
                scheduler.update_job(repeating_job_id, new_job).unwrap();

                Ok(())
            });

    scheduler.add_job(repeating_job).unwrap();
    scheduler.add_job(scheduler_job).unwrap();

    scheduler.start();
    std::thread::park();
}
