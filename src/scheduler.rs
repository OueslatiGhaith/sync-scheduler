use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use crate::{
    arc_mutex,
    job::{Job, JobEvent, JobTask},
    trigger_job_option,
};
use chrono::Utc;
use chrono_tz::Tz;
use crossbeam_channel::{unbounded, Receiver, Sender};
use tracing::trace;
use uuid::Uuid;

#[derive(Clone)]
pub struct SchedulerConfig {
    pub timezone: Tz,
    pub max_concurrent_jobs: usize,
    pub thread_pool_size: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            timezone: Tz::UTC,
            max_concurrent_jobs: num_cpus::get(),
            thread_pool_size: num_cpus::get(),
        }
    }
}

pub enum ScheduleMode {
    Once,
    Repeating,
    Limited(usize),
    Signleton,
}

pub struct Scheduler {
    jobs: Arc<Mutex<HashMap<Uuid, Job>>>,
    running: Arc<Mutex<bool>>,
    job_sender: Sender<JobExecution>,
    job_receiver: Receiver<JobExecution>,
    config: SchedulerConfig,
    running_jobs_count: Arc<Mutex<usize>>,
}

struct JobExecution {
    id: Uuid,
    task: JobTask,
}

impl Scheduler {
    pub fn new(config: SchedulerConfig) -> Self {
        let (job_sender, job_receiver) = unbounded();
        Self {
            jobs: arc_mutex!(HashMap::new()),
            running: arc_mutex!(false),
            job_sender,
            job_receiver,
            config,
            running_jobs_count: arc_mutex!(0),
        }
    }

    pub fn add_job(&self, job: Job) -> Result<(), String> {
        trace!("Adding job with id {}", job.id);

        let job_id = job.id.clone();
        let mut jobs = self.jobs.lock().unwrap();
        if jobs.contains_key(&job.id) {
            return Err(format!("Job with id {} already exists", job.id));
        }
        jobs.insert(job.id.clone(), job);
        drop(jobs);
        Self::trigger_job_hooks(&self.jobs, &job_id, JobEvent::Scheduled(job_id));
        Ok(())
    }

    pub fn remove_job(&self, job_id: Uuid) {
        trace!("Removing job with id {}", job_id);

        if self.jobs.lock().unwrap().remove(&job_id).is_some() {
            Self::trigger_job_hooks(&self.jobs, &job_id, JobEvent::Removed(job_id));
        }
    }

    pub fn update_job(
        &self,
        id: Uuid,
        new_task: impl Fn() -> () + Send + Sync + 'static,
    ) -> Result<(), String> {
        trace!("Updating job with id {}", id);

        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&id) {
            let mut task = job.task.lock().unwrap();
            *task = Box::new(new_task);
            Ok(())
        } else {
            Err(format!("Job with id {} not found", id))
        }
    }

    pub fn start(&self) {
        let mut running = self.running.lock().unwrap();
        if *running {
            println!("Scheduler already running");
            return;
        }
        *running = true;
        drop(running);

        trace!("Starting scheduler");

        let jobs = Arc::clone(&self.jobs);
        let running = Arc::clone(&self.running);
        let job_sender = self.job_sender.clone();
        let config = self.config.clone();
        let running_jobs_count: Arc<Mutex<usize>> = Arc::clone(&self.running_jobs_count);

        thread::spawn(move || {
            trace!("Scheduler thread started");

            while *running.lock().unwrap() {
                let now = Utc::now().with_timezone(&config.timezone);
                let mut job_lock = jobs.lock().unwrap();

                for (id, job) in job_lock.iter_mut() {
                    if now >= job.start_time.with_timezone(&config.timezone)
                        && now.signed_duration_since(job.last_scheduled) >= job.interval
                    {
                        let running_count = *running_jobs_count.lock().unwrap();
                        if running_count <= config.max_concurrent_jobs {
                            match job.mode {
                                ScheduleMode::Once if job.executions == 0 => {
                                    trace!("Scheduling job with id {} for once", id);
                                    Self::schedule_job(
                                        id,
                                        &config.timezone,
                                        job,
                                        &job_sender,
                                        &running_jobs_count,
                                    );
                                }
                                ScheduleMode::Repeating => {
                                    trace!("Scheduling job with id {} for repeating", id);
                                    Self::schedule_job(
                                        id,
                                        &config.timezone,
                                        job,
                                        &job_sender,
                                        &running_jobs_count,
                                    );
                                }
                                ScheduleMode::Limited(limit) if job.executions < limit => {
                                    trace!(
                                        "Scheduling job with id {} for limited with limit {}/{}",
                                        id,
                                        job.executions,
                                        limit
                                    );
                                    Self::schedule_job(
                                        id,
                                        &config.timezone,
                                        job,
                                        &job_sender,
                                        &running_jobs_count,
                                    );
                                }
                                ScheduleMode::Signleton if !job.is_running => {
                                    trace!("Scheduling job with id {} for singleton", id);
                                    Self::schedule_job(
                                        id,
                                        &config.timezone,
                                        job,
                                        &job_sender,
                                        &running_jobs_count,
                                    );
                                }
                                _ => {}
                            }
                        } else {
                            trace!("Max concurrent jobs reached, job {} not scheduled", id);
                        }
                    }
                }

                job_lock.retain(|id, job| {
                    let retain = match job.mode {
                        ScheduleMode::Once => job.executions == 0,
                        ScheduleMode::Limited(limit) => job.executions < limit,
                        _ => true,
                    };
                    if !retain {
                        trace!("Removing job with id {} from scheduler", id);
                    }
                    retain
                });

                drop(job_lock);
                thread::sleep(Duration::from_millis(100));
            }

            trace!("Scheduler thread stopped");
        });

        for _ in 0..self.config.thread_pool_size {
            let job_receiver = self.job_receiver.clone();
            let running = Arc::clone(&self.running);
            let jobs = Arc::clone(&self.jobs);
            let running_jobs_count = Arc::clone(&self.running_jobs_count);

            thread::spawn(move || {
                trace!("Job thread started");

                while *running.lock().unwrap() {
                    if let Ok(job_execution) = job_receiver.recv_timeout(Duration::from_millis(100))
                    {
                        trace!("Executing job with id {}", job_execution.id);

                        Self::trigger_job_hooks(
                            &jobs,
                            &job_execution.id,
                            JobEvent::Started(job_execution.id),
                        );

                        let task = job_execution.task.lock().unwrap();
                        let result = std::panic::catch_unwind(|| task());

                        let mut jobs_lock = jobs.lock().unwrap();
                        if let Some(job) = jobs_lock.get_mut(&job_execution.id) {
                            job.executions += 1;
                            job.is_running = false;
                        }

                        match result {
                            Ok(_) => {
                                trace!("Job with id {} completed", job_execution.id);
                                Self::trigger_job_hooks(
                                    &jobs,
                                    &job_execution.id,
                                    JobEvent::Completed(job_execution.id),
                                );
                            }
                            Err(e) => {
                                let error_msg = if let Some(s) = e.downcast_ref::<String>() {
                                    s.clone()
                                } else if let Some(s) = e.downcast_ref::<&str>() {
                                    s.to_string()
                                } else {
                                    "Unknown error".to_string()
                                };
                                trace!(
                                    "Job with id {} failed with error {}",
                                    job_execution.id,
                                    error_msg
                                );
                                Self::trigger_job_hooks(
                                    &jobs,
                                    &job_execution.id,
                                    JobEvent::Failed(job_execution.id, error_msg),
                                );
                            }
                        }

                        let mut count = running_jobs_count.lock().unwrap();
                        *count -= 1;
                    }
                }

                trace!("Job thread stopped");
            });
        }
    }

    fn schedule_job(
        id: &Uuid,
        timezone: &Tz,
        job: &mut Job,
        sender: &Sender<JobExecution>,
        running_jobs_count: &Arc<Mutex<usize>>,
    ) {
        sender
            .send(JobExecution {
                id: id.clone(),
                task: Arc::clone(&job.task),
            })
            .unwrap();
        job.last_scheduled = Utc::now().with_timezone(timezone);
        job.is_running = true;

        let mut count = running_jobs_count.lock().unwrap();
        *count += 1;
        trace!("Job {} scheduled, Running jobs count: {}", id, *count);
    }

    fn trigger_job_hooks(jobs: &Arc<Mutex<HashMap<Uuid, Job>>>, job_id: &Uuid, event: JobEvent) {
        trace!("Triggering job hooks for job with id {}", job_id);

        if let Some(job) = jobs.lock().unwrap().get(&job_id) {
            match event {
                JobEvent::Started(uuid) => trigger_job_option!(job, on_start, (uuid)),
                JobEvent::Completed(uuid) => trigger_job_option!(job, on_complete, (uuid)),
                JobEvent::Failed(uuid, err) => trigger_job_option!(job, on_fail, (uuid, err)),
                JobEvent::Scheduled(uuid) => trigger_job_option!(job, on_schedule, (uuid)),
                JobEvent::Removed(uuid) => trigger_job_option!(job, on_remove, (uuid)),
            };
        }
    }

    pub fn stop(&self) {
        trace!("Stopping scheduler");

        let mut running = self.running.lock().unwrap();
        *running = false;
    }
}
