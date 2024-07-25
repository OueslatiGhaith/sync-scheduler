use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    thread,
    time::Duration,
};

use crate::{
    arc_mutex, arc_rwlock,
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
    pub jobs: Arc<RwLock<HashMap<Uuid, Arc<Mutex<Job>>>>>,
    running: Arc<RwLock<bool>>,
    job_sender: Sender<JobExecution>,
    job_receiver: Receiver<JobExecution>,
    config: SchedulerConfig,
    running_jobs_count: Arc<RwLock<usize>>,
    scheduler_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    worker_threads: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
}

struct JobExecution {
    id: Uuid,
    task: JobTask,
}

impl Scheduler {
    pub fn new(config: SchedulerConfig) -> Self {
        let (job_sender, job_receiver) = unbounded();
        Self {
            jobs: arc_rwlock!(HashMap::new()),
            running: arc_rwlock!(false),
            job_sender,
            job_receiver,
            config,
            running_jobs_count: arc_rwlock!(0),
            scheduler_thread: arc_mutex!(None),
            worker_threads: arc_mutex!(Vec::new()),
        }
    }

    pub fn add_job(&self, job: Job) -> Result<Uuid, String> {
        let job_id = job.id;
        trace!("Adding job with id {}", job_id);

        let mut jobs = self.jobs.write().unwrap();
        if jobs.contains_key(&job.id) {
            return Err(format!("Job with id {} already exists", job.id));
        }
        jobs.insert(job.id, arc_mutex!(job));
        drop(jobs);

        Self::trigger_job_hooks(self.jobs.clone(), &job_id, JobEvent::Scheduled(job_id));
        Ok(job_id)
    }

    pub fn remove_job(&self, job_id: Uuid) {
        trace!("Removing job with id {}", job_id);

        let mut jobs = self.jobs.write().unwrap();
        if jobs.remove(&job_id).is_some() {
            drop(jobs);
            Self::trigger_job_hooks(self.jobs.clone(), &job_id, JobEvent::Removed(job_id));
        }
    }

    pub fn update_job(&self, id: Uuid, new_job: Job) -> Result<(), String> {
        trace!("Updating job with id {}", id);

        let jobs = self.jobs.read().unwrap();
        if let Some(job) = jobs.get(&id) {
            let mut job = job.lock().unwrap();
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

        let mut scheduler_thread_guard = self.scheduler_thread.lock().unwrap();
        *scheduler_thread_guard = Some(thread::spawn(move || {
            trace!("Scheduler thread started");
            Self::run_scheduler_loop(scheduler_context);
            trace!("Scheduler thread stopped");
        }));
        drop(scheduler_thread_guard);

        let mut worker_threads_guard = self.worker_threads.lock().unwrap();
        for _ in 0..self.config.thread_pool_size {
            let job_receiver = self.job_receiver.clone();
            let running = Arc::clone(&self.running);
            let jobs = Arc::clone(&self.jobs);
            let running_jobs_count = Arc::clone(&self.running_jobs_count);

            worker_threads_guard.push(thread::spawn(move || {
                trace!("Job thread started");
                Self::run_worker_loop(job_receiver, running, jobs, running_jobs_count);
                trace!("Job thread stopped");
            }));
        }
    }

    pub fn stop(&self) {
        trace!("Stopping scheduler");

        let mut running = self.running.write().unwrap();
        *running = false;
        drop(running);

        // wait for scheduler thread to finish
        if let Some(thread) = self.scheduler_thread.lock().unwrap().take() {
            thread.join().unwrap();
        }

        // wait for worker threads to finish
        let mut worker_threads = self.worker_threads.lock().unwrap();
        while let Some(thread) = worker_threads.pop() {
            thread.join().unwrap();
        }
    }
}

impl Scheduler {
    fn set_running_state(&self, state: bool) -> bool {
        let mut running = self.running.write().unwrap();
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
        while *context.running.read().unwrap() {
            let now = Utc::now().with_timezone(&context.config.timezone);
            let jobs_to_schedule = Self::identify_jobs_to_schedule(&context, now);
            Self::schedule_identified_jobs(&context, jobs_to_schedule, now);
            Self::remove_completed_jobs(&context);
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn run_worker_loop(
        job_receiver: Receiver<JobExecution>,
        running: Arc<RwLock<bool>>,
        jobs: Arc<RwLock<HashMap<Uuid, Arc<Mutex<Job>>>>>,
        running_jobs_count: Arc<RwLock<usize>>,
    ) {
        while *running.read().unwrap() {
            if let Ok(job_exec) = job_receiver.recv_timeout(Duration::from_millis(100)) {
                trace!("Executing job with id {}", job_exec.id);

                Self::trigger_job_hooks(jobs.clone(), &job_exec.id, JobEvent::Started(job_exec.id));

                let task = job_exec.task.lock().unwrap();
                #[allow(clippy::redundant_closure)]
                let result = std::panic::catch_unwind(|| task());

                let jobs_lock = jobs.read().unwrap();
                if let Some(job) = jobs_lock.get(&job_exec.id) {
                    let mut job = job.lock().unwrap();
                    job.executions += 1;
                    job.is_running = false;
                }
                drop(jobs_lock);

                match result {
                    Ok(_) => {
                        trace!("Job with id {} completed", job_exec.id);
                        Self::trigger_job_hooks(
                            jobs.clone(),
                            &job_exec.id,
                            JobEvent::Completed(job_exec.id),
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
                            job_exec.id,
                            error_msg
                        );
                        Self::trigger_job_hooks(
                            jobs.clone(),
                            &job_exec.id,
                            JobEvent::Failed(job_exec.id, error_msg),
                        );
                    }
                }

                let mut count = running_jobs_count.write().unwrap();
                *count -= 1;
            }
        }
    }

    fn identify_jobs_to_schedule(context: &SchedulerContext, now: DateTime<Tz>) -> Vec<Uuid> {
        let jobs_read = context.jobs.read().unwrap();
        jobs_read
            .iter()
            .filter(|(_, job)| {
                let job = job.lock().unwrap();
                Self::should_schedule_job(&job, now, &jobs_read)
            })
            .map(|(id, _)| *id)
            .collect()
    }

    fn should_schedule_job(
        job: &Job,
        now: DateTime<Tz>,
        jobs: &HashMap<Uuid, Arc<Mutex<Job>>>,
    ) -> bool {
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
        let running_count = *context.running_jobs_count.read().unwrap();
        let available_slots = context
            .config
            .max_concurrent_jobs
            .saturating_sub(running_count);
        let jobs_read = context.jobs.read().unwrap();

        for id in jobs_to_schedule.into_iter().take(available_slots) {
            if let Some(job) = jobs_read.get(&id) {
                let mut job = job.lock().unwrap();
                if Self::can_schedule_job(&job) {
                    Self::schedule_job(
                        &mut job,
                        now,
                        &context.job_sender,
                        &context.running_jobs_count,
                    );
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
        let mut jobs_lock = context.jobs.write().unwrap();
        jobs_lock.retain(|_, job| {
            let job = job.lock().unwrap();
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
        running_jobs_count: &Arc<RwLock<usize>>,
    ) {
        sender
            .send(JobExecution {
                id: job.id,
                task: Arc::clone(&job.task),
            })
            .unwrap();
        job.last_scheduled = now;
        job.is_running = true;

        let mut count = running_jobs_count.write().unwrap();
        *count += 1;
        trace!("Job {} scheduled, Running jobs count: {}", job.id, *count);
    }

    fn trigger_job_hooks(
        jobs: Arc<RwLock<HashMap<Uuid, Arc<Mutex<Job>>>>>,
        job_id: &Uuid,
        event: JobEvent,
    ) {
        trace!("Triggering job hooks for job with id {}", job_id);

        let jobs_read = jobs.read().unwrap();
        if let Some(job) = jobs_read.get(job_id) {
            let mut job = job.lock().unwrap();
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

    fn are_dependencies_met(jobs: &HashMap<Uuid, Arc<Mutex<Job>>>, job: &Job) -> bool {
        job.dependencies.iter().all(|dep_id| {
            jobs.get(dep_id)
                .map(|dep_job| dep_job.lock().unwrap().completed)
                .unwrap_or(false)
        })
    }
}

struct SchedulerContext {
    jobs: Arc<RwLock<HashMap<Uuid, Arc<Mutex<Job>>>>>,
    running: Arc<RwLock<bool>>,
    job_sender: Sender<JobExecution>,
    config: SchedulerConfig,
    running_jobs_count: Arc<RwLock<usize>>,
}
