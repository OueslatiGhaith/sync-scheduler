use std::thread;
use std::time::Duration as StdDuration;

use chrono::Duration;
use sync_scheduler::{JobBuilder, Scheduler, SchedulerConfig};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let config = SchedulerConfig::default();
    let scheduler = Scheduler::new(config);

    let repeating_uuid = Uuid::new_v4();
    let failing_uuid: Uuid = Uuid::new_v4();

    scheduler
        .add_job(
            JobBuilder::default()
                .with_tag("repeating")
                .repeating(Duration::seconds(5))
                .build(|_, _| {
                    info!("Repeating job");
                    Ok(())
                }),
        )
        .unwrap();

    let once_job = scheduler
        .add_job(JobBuilder::default().with_tag("once").once().build(|_, _| {
            info!("Once job");
            Ok(())
        }))
        .unwrap();

    scheduler
        .add_job(
            JobBuilder::default()
                .with_tag("limited")
                .limited(5, Duration::seconds(5))
                .depends_on(once_job)
                .build(|_, _| {
                    info!("Limited job");
                    Ok(())
                }),
        )
        .unwrap();

    scheduler
        .add_job(
            JobBuilder::default()
                .with_tag("failing")
                .once()
                .build(|_, _| panic!("failing job")),
        )
        .unwrap();

    scheduler.start();

    thread::sleep(StdDuration::from_secs(15));
    scheduler.remove_job(repeating_uuid);

    thread::sleep(StdDuration::from_secs(15));
    scheduler.remove_job(failing_uuid);

    scheduler.stop();
}
