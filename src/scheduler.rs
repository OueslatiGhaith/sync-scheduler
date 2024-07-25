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
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use crossbeam_channel::{unbounded, Receiver, Sender};
use tracing::{trace, warn};
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
    pub jobs: Arc<Mutex<HashMap<Uuid, Job>>>,
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

    pub fn add_job(&self, job: Job) -> Result<Uuid, String> {
        trace!("Adding job with id {}", job.id);

        let job_id = job.id.clone();
        let mut jobs = self.jobs.lock().unwrap();
        if jobs.contains_key(&job.id) {
            return Err(format!("Job with id {} already exists", job.id));
        }
        jobs.insert(job.id.clone(), job);
        drop(jobs);
        Self::trigger_job_hooks(&self.jobs, &job_id, JobEvent::Scheduled(job_id));
        Ok(job_id)
    }

    pub fn remove_job(&self, job_id: Uuid) {
        trace!("Removing job with id {}", job_id);

        if self.jobs.lock().unwrap().remove(&job_id).is_some() {
            Self::trigger_job_hooks(&self.jobs, &job_id, JobEvent::Removed(job_id));
        }
    }

    pub fn update_job(&self, id: Uuid, new_job: Job) -> Result<(), String> {
        trace!("Updating job with id {}", id);

        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&id) {
            *job = new_job;
            Ok(())
        } else {
            Err(format!("Job with id {} not found", id))
        }
    }

    pub fn start(&self) {
        if !self.set_running_state(true) {
            warn!("Scheduler already running");
            return;
        }

        trace!("Starting scheduler");

        let scheduler_context = self.create_scheduler_context();

        thread::spawn(move || {
            trace!("Scheduler thread started");
            Self::run_scheduler_loop(scheduler_context);
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

    fn set_running_state(&self, state: bool) -> bool {
        let mut running = self.running.lock().unwrap();
        if *running == state {
            return false;
        }
        *running = state;
        true
    }

    fn create_scheduler_context(&self) -> SchedulerContext {
        SchedulerContext {
            jobs: Arc::clone(&self.jobs),
            running: Arc::clone(&self.running),
            job_sender: self.job_sender.clone(),
            config: self.config.clone(),
            running_jobs_count: Arc::clone(&self.running_jobs_count),
        }
    }

    fn run_scheduler_loop(context: SchedulerContext) {
        while *context.running.lock().unwrap() {
            let now = Utc::now().with_timezone(&context.config.timezone);
            let jobs_to_schedule = Self::identify_jobs_to_schedule(&context, now);
            Self::schedule_identified_jobs(&context, jobs_to_schedule, now);
            Self::remove_completed_jobs(&context);
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn identify_jobs_to_schedule(context: &SchedulerContext, now: DateTime<Tz>) -> Vec<Uuid> {
        let jobs_lock = context.jobs.lock().unwrap();
        jobs_lock
            .iter()
            .filter(|(_, job)| Self::should_schedule_job(job, now, &jobs_lock))
            .map(|(id, _)| *id)
            .collect()
    }

    fn should_schedule_job(job: &Job, now: DateTime<Tz>, jobs: &HashMap<Uuid, Job>) -> bool {
        now >= job.start_time.with_timezone(&now.timezone())
            && now.signed_duration_since(job.last_scheduled) >= job.interval
            && !job.completed
            && !job.is_running
            && Self::are_dependencies_met(jobs, job)
    }

    fn schedule_identified_jobs(
        context: &SchedulerContext,
        jobs_to_schedule: Vec<Uuid>,
        now: DateTime<Tz>,
    ) {
        let mut jobs_lock = context.jobs.lock().unwrap();
        let running_count = *context.running_jobs_count.lock().unwrap();
        let available_slots = context
            .config
            .max_concurrent_jobs
            .saturating_sub(running_count);

        for id in jobs_to_schedule.into_iter().take(available_slots) {
            if let Some(job) = jobs_lock.get_mut(&id) {
                if Self::can_schedule_job(job) {
                    Self::schedule_job(job, now, &context.job_sender, &context.running_jobs_count);
                }
            }
        }
    }

    fn can_schedule_job(job: &Job) -> bool {
        match job.mode {
            ScheduleMode::Once if job.executions == 0 => true,
            ScheduleMode::Repeating => true,
            ScheduleMode::Limited(limit) if job.executions < limit => true,
            ScheduleMode::Signleton => true,
            _ => false,
        }
    }

    fn remove_completed_jobs(context: &SchedulerContext) {
        let mut jobs_lock = context.jobs.lock().unwrap();
        jobs_lock.retain(|_, job| {
            let retain = match job.mode {
                ScheduleMode::Once => !job.completed,
                ScheduleMode::Limited(limit) => job.executions < limit,
                _ => true,
            };
            if !retain {
                trace!("Removing job with id {}", job.id);
            }
            retain
        });
    }

    fn schedule_job(
        job: &mut Job,
        now: DateTime<Tz>,
        sender: &Sender<JobExecution>,
        running_jobs_count: &Arc<Mutex<usize>>,
    ) {
        sender
            .send(JobExecution {
                id: job.id,
                task: Arc::clone(&job.task),
            })
            .unwrap();
        job.last_scheduled = now;
        job.is_running = true;

        let mut count = running_jobs_count.lock().unwrap();
        *count += 1;
        trace!("Job {} scheduled, Running jobs count: {}", job.id, *count);
    }

    fn trigger_job_hooks(jobs: &Arc<Mutex<HashMap<Uuid, Job>>>, job_id: &Uuid, event: JobEvent) {
        trace!("Triggering job hooks for job with id {}", job_id);

        if let Some(job) = jobs.lock().unwrap().get_mut(&job_id) {
            match event {
                JobEvent::Started(uuid) => trigger_job_option!(job, on_start, (uuid)),
                JobEvent::Failed(uuid, err) => trigger_job_option!(job, on_fail, (uuid, err)),
                JobEvent::Scheduled(uuid) => trigger_job_option!(job, on_schedule, (uuid)),
                JobEvent::Removed(uuid) => trigger_job_option!(job, on_remove, (uuid)),
                JobEvent::Completed(uuid) => {
                    job.completed = true;
                    trigger_job_option!(job, on_complete, (uuid))
                }
            };
        }
    }

    pub fn stop(&self) {
        trace!("Stopping scheduler");

        let mut running = self.running.lock().unwrap();
        *running = false;
    }

    fn are_dependencies_met(jobs: &HashMap<Uuid, Job>, job: &Job) -> bool {
        job.dependencies.iter().all(|dep_id| {
            jobs.get(dep_id)
                .map(|dep_job| dep_job.completed)
                .unwrap_or(false)
        })
    }
}

struct SchedulerContext {
    jobs: Arc<Mutex<HashMap<Uuid, Job>>>,
    running: Arc<Mutex<bool>>,
    job_sender: Sender<JobExecution>,
    config: SchedulerConfig,
    running_jobs_count: Arc<Mutex<usize>>,
}
