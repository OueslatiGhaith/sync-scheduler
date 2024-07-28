use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Duration};
use chrono_tz::Tz;
use uuid::Uuid;

use crate::scheduler::{ScheduleMode, SchedulerHandle};

pub(crate) type JobTask = Arc<Mutex<Box<dyn Fn(Uuid, &SchedulerHandle) + Send + Sync>>>;

pub struct Job {
    pub(crate) id: Uuid,
    pub(crate) _tags: Vec<String>,
    pub(crate) interval: Duration,
    pub(crate) task: JobTask,
    pub(crate) last_scheduled: DateTime<Tz>,
    pub(crate) mode: ScheduleMode,
    pub(crate) executions: usize,
    pub(crate) is_running: bool,
    pub(crate) start_time: DateTime<Tz>,
    pub(crate) hooks: JobHooks,
    pub(crate) dependencies: HashSet<Uuid>,
    pub(crate) completed: bool,
}

impl Job {
    pub fn id(&self) -> Uuid {
        self.id
    }
}

#[derive(Clone)]
pub enum JobEvent {
    Started(Uuid),
    Completed(Uuid),
    Failed(Uuid, String),
    Scheduled(Uuid),
    Removed(Uuid),
}

type OptionArcMutex<T> = Option<Arc<Mutex<T>>>;

pub struct JobHooks {
    pub(crate) on_start: OptionArcMutex<dyn Fn(Uuid) + Send + Sync>,
    pub(crate) on_complete: OptionArcMutex<dyn Fn(Uuid) + Send + Sync>,
    pub(crate) on_fail: OptionArcMutex<dyn Fn(Uuid, String) + Send + Sync>,
    pub(crate) on_schedule: OptionArcMutex<dyn Fn(Uuid) + Send + Sync>,
    pub(crate) on_remove: OptionArcMutex<dyn Fn(Uuid) + Send + Sync>,
}
