use std::{
    collections::HashSet,
    error::Error,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::scheduler::{ScheduleMode, SchedulerHandle};

pub(crate) type JobTask =
    Arc<Mutex<Box<dyn Fn(Uuid, &SchedulerHandle) -> Result<(), Box<dyn Error>> + Send + Sync>>>;
pub(crate) type JobCondition = Arc<dyn Fn(Uuid, &SchedulerHandle) -> bool + Send + Sync>;

pub struct Job {
    pub(crate) id: Uuid,
    pub(crate) _tags: Vec<String>,
    pub(crate) interval: Duration,
    pub(crate) task: JobTask,
    pub(crate) last_scheduled: DateTime<Utc>,
    pub(crate) mode: ScheduleMode,
    pub(crate) executions: usize,
    pub(crate) is_running: bool,
    pub(crate) start_time: DateTime<Utc>,
    pub(crate) hooks: JobHooks,
    pub(crate) dependencies: HashSet<Uuid>,
    pub(crate) completed: bool,
    pub(crate) conditions: Vec<JobCondition>,
}

impl Job {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    pub fn mode(&self) -> ScheduleMode {
        self.mode
    }

    pub fn start_time(&self) -> DateTime<Utc> {
        self.start_time
    }

    pub fn last_scheduled(&self) -> DateTime<Utc> {
        self.last_scheduled
    }

    pub fn executions(&self) -> usize {
        self.executions
    }

    pub fn is_running(&self) -> bool {
        self.is_running
    }

    pub fn completed(&self) -> bool {
        self.completed
    }

    pub fn dependencies(&self) -> &HashSet<Uuid> {
        &self.dependencies
    }
}

pub enum JobEvent {
    Started(Uuid),
    Completed(Uuid),
    Failed(Uuid, Box<dyn Error>),
    Panicked(Uuid, String),
    Scheduled(Uuid),
    Removed(Uuid),
}

type OptionArcMutex<T> = Option<Arc<Mutex<T>>>;

pub struct JobHooks {
    pub(crate) on_start: OptionArcMutex<dyn Fn(Uuid) + Send + Sync>,
    pub(crate) on_complete: OptionArcMutex<dyn Fn(Uuid) + Send + Sync>,
    pub(crate) on_fail: OptionArcMutex<dyn Fn(Uuid, Box<dyn Error>) + Send + Sync>,
    pub(crate) on_panic: OptionArcMutex<dyn Fn(Uuid, String) + Send + Sync>,
    pub(crate) on_schedule: OptionArcMutex<dyn Fn(Uuid) + Send + Sync>,
    pub(crate) on_remove: OptionArcMutex<dyn Fn(Uuid) + Send + Sync>,
}
