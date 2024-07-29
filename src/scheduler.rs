use std::{collections::HashMap, sync::Arc, thread, time::Duration};

use crate::{
    arc_mutex, arc_rwlock,
    job::{Job, JobEvent, JobTask},
    trigger_job_option, Error,
};
use chrono::{DateTime, Utc};
use crossbeam_channel::{unbounded, Receiver, Sender};
use parking_lot::{Mutex, RwLock};
use tracing::{trace, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct SchedulerConfig {
    pub max_concurrent_jobs: usize,
    pub thread_pool_size: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_jobs: num_cpus::get(),
            thread_pool_size: num_cpus::get(),
        }
    }
}

#[derive(Clone, Copy)]
pub enum ScheduleMode {
    Once,
    Repeating,
    Limited(usize),
}

impl ScheduleMode {
    fn should_complete(&self, runs: usize) -> bool {
        match self {
            ScheduleMode::Once => runs > 0,
            ScheduleMode::Limited(limit) => runs >= *limit,
            ScheduleMode::Repeating => false,
        }
    }
}

pub struct Scheduler {
    inner: Arc<SchedulerHandle>,
}

pub struct SchedulerHandle {
    pub jobs: Arc<RwLock<HashMap<Uuid, Arc<RwLock<Job>>>>>,
    running: Arc<RwLock<bool>>,
    job_sender: Sender<JobExecution>,
    job_receiver: Receiver<JobExecution>,
    config: SchedulerConfig,
    running_jobs_count: Arc<RwLock<usize>>,
    scheduler_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    worker_threads: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
}

struct SchedulerContext {
    jobs: Arc<RwLock<HashMap<Uuid, Arc<RwLock<Job>>>>>,
    running: Arc<RwLock<bool>>,
    job_sender: Sender<JobExecution>,
    config: SchedulerConfig,
    running_jobs_count: Arc<RwLock<usize>>,
    scheduler: Arc<SchedulerHandle>,
}

struct JobExecution {
    id: Uuid,
    task: JobTask,
}

impl Scheduler {
    pub fn new(config: SchedulerConfig) -> Self {
        let (job_sender, job_receiver) = unbounded();
        Self {
            inner: Arc::new(SchedulerHandle {
                jobs: arc_rwlock!(HashMap::new()),
                running: arc_rwlock!(false),
                job_sender,
                job_receiver,
                config,
                running_jobs_count: arc_rwlock!(0),
                scheduler_thread: arc_mutex!(None),
                worker_threads: arc_mutex!(Vec::new()),
            }),
        }
    }

    pub fn jobs(&self) -> parking_lot::RwLockReadGuard<HashMap<Uuid, Arc<RwLock<Job>>>> {
        self.inner.jobs()
    }

    pub fn get_job(&self, id: Uuid) -> Option<Arc<RwLock<Job>>> {
        self.inner.get_job(id)
    }

    pub fn add_job(&self, job: Job) -> Result<Uuid, Error> {
        self.inner.add_job(job)
    }

    pub fn remove_job(&self, job_id: Uuid) {
        self.inner.remove_job(job_id)
    }

    pub fn update_job(&self, id: Uuid, new_job: Job) -> Result<(), Error> {
        self.inner.update_job(id, new_job)
    }

    pub fn stop(&self) {
        self.inner.stop()
    }

    pub fn start(&self) {
        if !self.inner.set_running_state(true) {
            warn!("Scheduler already running");
            return;
        }

        trace!("Starting scheduler");

        let scheduler_context = self.create_scheduler_context();

        let mut scheduler_thread_guard = self.inner.scheduler_thread.lock();
        *scheduler_thread_guard = Some(thread::spawn(move || {
            trace!("Scheduler thread started");
            SchedulerHandle::run_scheduler_loop(scheduler_context);
            trace!("Scheduler thread stopped");
        }));
        drop(scheduler_thread_guard);

        let mut worker_threads_guard = self.inner.worker_threads.lock();
        let scheduler = Arc::clone(&self.inner);
        for _ in 0..self.inner.config.thread_pool_size {
            let job_receiver = self.inner.job_receiver.clone();
            let running = Arc::clone(&self.inner.running);
            let jobs = Arc::clone(&self.inner.jobs);
            let running_jobs_count = Arc::clone(&self.inner.running_jobs_count);
            let scheduler_clone = Arc::clone(&scheduler);

            worker_threads_guard.push(thread::spawn(move || {
                trace!("Job thread started");
                SchedulerHandle::run_worker_loop(
                    job_receiver,
                    running,
                    jobs,
                    running_jobs_count,
                    scheduler_clone,
                );
                trace!("Job thread stopped");
            }));
        }
    }

    pub fn next_execution_time(&self, id: Uuid) -> Option<DateTime<Utc>> {
        self.inner.next_execution_time(id)
    }

    fn create_scheduler_context(&self) -> SchedulerContext {
        SchedulerContext {
            jobs: Arc::clone(&self.inner.jobs),
            running: Arc::clone(&self.inner.running),
            job_sender: self.inner.job_sender.clone(),
            config: self.inner.config.clone(),
            running_jobs_count: Arc::clone(&self.inner.running_jobs_count),
            scheduler: Arc::clone(&self.inner),
        }
    }
}

impl SchedulerHandle {
    fn set_running_state(&self, state: bool) -> bool {
        let mut running = self.running.write();
        if *running == state {
            return false;
        }
        *running = state;
        true
    }

    fn run_scheduler_loop(context: SchedulerContext) {
        while *context.running.read() {
            let now = Utc::now();
            let jobs_to_schedule =
                Self::identify_jobs_to_schedule(&context, now, context.scheduler.clone());
            Self::schedule_identified_jobs(&context, jobs_to_schedule, now);
            Self::remove_completed_jobs(&context);
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn run_worker_loop(
        job_receiver: Receiver<JobExecution>,
        running: Arc<RwLock<bool>>,
        jobs: Arc<RwLock<HashMap<Uuid, Arc<RwLock<Job>>>>>,
        running_jobs_count: Arc<RwLock<usize>>,
        scheduler: Arc<SchedulerHandle>,
    ) {
        while *running.read() {
            if let Ok(job_exec) = job_receiver.recv_timeout(Duration::from_millis(100)) {
                trace!("Executing job with id {}", job_exec.id);

                Self::trigger_job_hooks(
                    scheduler.clone(),
                    jobs.clone(),
                    &job_exec.id,
                    JobEvent::Started(job_exec.id),
                );

                let task = job_exec.task.lock();
                // #[allow(clippy::redundant_closure)]
                // let result = std::panic::catch_unwind(|| task(job_exec.id, &scheduler));
                let result = task(job_exec.id, &scheduler);

                let jobs_lock = jobs.read();
                if let Some(job) = jobs_lock.get(&job_exec.id) {
                    let mut job = job.write();
                    job.executions += 1;
                    job.is_running = false;
                }
                drop(jobs_lock);

                match result {
                    Ok(_) => {
                        trace!("Job with id {} completed", job_exec.id);
                        Self::trigger_job_hooks(
                            scheduler.clone(),
                            jobs.clone(),
                            &job_exec.id,
                            JobEvent::Completed(job_exec.id),
                        );
                    }
                    Err(err) => {
                        trace!("Job with id {} failed", job_exec.id);
                        Self::trigger_job_hooks(
                            scheduler.clone(),
                            jobs.clone(),
                            &job_exec.id,
                            JobEvent::Failed(job_exec.id, err),
                        );
                    }
                }

                // match result {
                //     Ok(result) => match result {
                //         Ok(_) => {
                //             trace!("Job with id {} completed", job_exec.id);
                //             Self::trigger_job_hooks(
                //                 scheduler.clone(),
                //                 jobs.clone(),
                //                 &job_exec.id,
                //                 JobEvent::Completed(job_exec.id),
                //             );
                //         }
                //         Err(err) => {
                //             trace!("Job with id {} failed", job_exec.id);
                //             Self::trigger_job_hooks(
                //                 scheduler.clone(),
                //                 jobs.clone(),
                //                 &job_exec.id,
                //                 JobEvent::Failed(job_exec.id, err),
                //             );
                //         }
                //     },
                //     Err(err) => {
                //         trace!("Job with id {} failed with error {:?}", job_exec.id, err);
                //         Self::trigger_job_hooks(
                //             scheduler.clone(),
                //             jobs.clone(),
                //             &job_exec.id,
                //             JobEvent::Panicked(job_exec.id, err),
                //         );
                //     }
                // }

                let mut count = running_jobs_count.write();
                *count -= 1;
            }
        }
    }

    fn identify_jobs_to_schedule(
        context: &SchedulerContext,
        now: DateTime<Utc>,
        scheduler: Arc<SchedulerHandle>,
    ) -> Vec<Uuid> {
        let jobs_read = context.jobs.read();
        jobs_read
            .iter()
            .filter(|(_, job)| {
                let job = job.read();
                Self::should_schedule_job(&job, now, &jobs_read, scheduler.clone())
            })
            .map(|(id, _)| *id)
            .collect()
    }

    fn should_schedule_job(
        job: &Job,
        now: DateTime<Utc>,
        jobs: &HashMap<Uuid, Arc<RwLock<Job>>>,
        scheduler: Arc<SchedulerHandle>,
    ) -> bool {
        let is_start_time = now >= job.start_time.with_timezone(&now.timezone());
        let is_interval_passed = now.signed_duration_since(job.last_scheduled) >= job.interval;
        let is_first_execution = job.executions == 0;
        let is_completed = job.is_completed;
        let is_running = job.is_running;
        let are_dependencies_met = Self::are_dependencies_met(jobs, job);
        let are_conditions_met = job
            .conditions
            .iter()
            .all(|condition| condition(job.id, &scheduler));

        is_start_time
            && (is_interval_passed || is_first_execution)
            && !is_completed
            && !is_running
            && are_dependencies_met
            && are_conditions_met
    }

    fn schedule_identified_jobs(
        context: &SchedulerContext,
        jobs_to_schedule: Vec<Uuid>,
        now: DateTime<Utc>,
    ) {
        let running_count = *context.running_jobs_count.read();
        let available_slots = context
            .config
            .max_concurrent_jobs
            .saturating_sub(running_count);
        let jobs_read = context.jobs.read();

        for id in jobs_to_schedule.into_iter().take(available_slots) {
            if let Some(job) = jobs_read.get(&id) {
                let mut job = job.write();
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
            _ => false,
        }
    }

    fn remove_completed_jobs(context: &SchedulerContext) {
        let mut jobs_lock = context.jobs.write();
        jobs_lock.retain(|_, job| {
            let job = job.read();
            let retain = match job.mode {
                ScheduleMode::Once => !job.is_completed,
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
        now: DateTime<Utc>,
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

        let mut count = running_jobs_count.write();
        *count += 1;
        trace!("Job {} scheduled, Running jobs count: {}", job.id, *count);
    }

    fn trigger_job_hooks(
        scheduler: Arc<SchedulerHandle>,
        jobs: Arc<RwLock<HashMap<Uuid, Arc<RwLock<Job>>>>>,
        job_id: &Uuid,
        event: JobEvent,
    ) {
        trace!("Triggering job hooks for job with id {}", job_id);

        let jobs_read = jobs.read();
        if let Some(job) = jobs_read.get(job_id) {
            let mut job = job.write();
            match event {
                JobEvent::Started(uuid) => trigger_job_option!(job, on_start, (uuid, &scheduler)),
                JobEvent::Failed(uuid, err) => {
                    trigger_job_option!(job, on_fail, (uuid, &scheduler, err))
                }
                // JobEvent::Panicked(uuid, err) => {
                //     trigger_job_option!(job, on_panic, (uuid, &scheduler, err))
                // }
                JobEvent::Completed(uuid) => {
                    if job.mode.should_complete(job.executions) {
                        job.is_completed = true;
                    }
                    trigger_job_option!(job, on_complete, (uuid, &scheduler))
                }
            };
        }
    }

    fn are_dependencies_met(jobs: &HashMap<Uuid, Arc<RwLock<Job>>>, job: &Job) -> bool {
        job.dependencies.iter().all(|dep_id| {
            jobs.get(dep_id)
                .map(|dep_job| dep_job.read().is_completed)
                .unwrap_or(false)
        })
    }
}

impl SchedulerHandle {
    pub fn jobs(&self) -> parking_lot::RwLockReadGuard<HashMap<Uuid, Arc<RwLock<Job>>>> {
        self.jobs.read()
    }

    pub fn get_job(&self, id: Uuid) -> Option<Arc<RwLock<Job>>> {
        let jobs = self.jobs();
        let job = jobs.get(&id)?;
        Some(job.clone())
    }

    pub fn add_job(&self, job: Job) -> Result<Uuid, Error> {
        let job_id = job.id;
        trace!("Adding job with id {}", job_id);

        let mut jobs = self.jobs.write();
        if jobs.contains_key(&job.id) {
            return Err(Error::JobAlreadyExists(job.id));
        }
        jobs.insert(job.id, arc_rwlock!(job));
        drop(jobs);

        Ok(job_id)
    }

    pub fn remove_job(&self, job_id: Uuid) {
        trace!("Removing job with id {}", job_id);

        let mut jobs = self.jobs.write();
        jobs.remove(&job_id);
    }

    pub fn update_job(&self, id: Uuid, mut new_job: Job) -> Result<(), Error> {
        trace!("Updating job with id {}", id);

        // update the new job id to be the same as the old one
        new_job.id = id;

        let jobs = self.jobs.read();
        if let Some(job) = jobs.get(&id) {
            let mut job = job.write();
            *job = new_job;
            Ok(())
        } else {
            Err(Error::JobNotFound(id))
        }
    }

    pub fn stop(&self) {
        trace!("Stopping scheduler");

        let mut running = self.running.write();
        *running = false;
        drop(running);

        // wait for scheduler thread to finish
        if let Some(thread) = self.scheduler_thread.lock().take() {
            thread.join().unwrap();
        }

        // wait for worker threads to finish
        let mut worker_threads = self.worker_threads.lock();
        while let Some(thread) = worker_threads.pop() {
            thread.join().unwrap();
        }
    }

    pub fn next_execution_time(&self, id: Uuid) -> Option<DateTime<Utc>> {
        let jobs = self.jobs.read();
        let job = jobs.get(&id)?;
        let job = job.read();

        if job.is_completed {
            return None;
        }

        let now = Utc::now();
        let last_scheduled = job.last_scheduled;
        let interval = job.interval;
        let start_time = job.start_time;

        let next_time = if job.executions == 0 {
            start_time.max(now)
        } else {
            (last_scheduled + interval).max(now)
        };

        Some(next_time)
    }

    pub fn get_jobs_by_tag(&self, tag: &str) -> Vec<Uuid> {
        let jobs = self.jobs.read();

        jobs.values()
            .filter(|job| {
                let job = job.read();
                job.tags.contains(&tag.to_string())
            })
            .map(|job| {
                let job = job.read();
                job.id
            })
            .collect()
    }
}
