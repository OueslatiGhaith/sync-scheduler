use std::{any::Any, collections::HashSet, error::Error, sync::Arc};

use chrono::{DateTime, Duration, TimeZone, Utc};
use uuid::Uuid;

use crate::{
    arc_mutex_box,
    job::{Job, JobCondition, JobHooks},
    scheduler::{ScheduleMode, SchedulerHandle},
};

pub struct JobBuilder {
    id: Uuid,
    tags: Vec<String>,
    interval: Duration,
    mode: ScheduleMode,
    start_time: Option<DateTime<Utc>>,
    dependencies: HashSet<Uuid>,
    hooks: JobHooks,
    conditions: Vec<JobCondition>,
}

impl Default for JobBuilder {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            tags: Vec::new(),
            interval: Duration::seconds(10),
            mode: ScheduleMode::Once,
            start_time: None,
            dependencies: HashSet::new(),
            conditions: Vec::new(),
            hooks: JobHooks {
                on_start: None,
                on_complete: None,
                on_fail: None,
                on_panic: None,
            },
        }
    }
}

impl JobBuilder {
    pub fn with_tag<S: AsRef<str>>(mut self, tag: S) -> Self {
        self.tags.push(tag.as_ref().to_owned());
        self
    }

    pub fn repeating(mut self, interval: Duration) -> Self {
        self.mode = ScheduleMode::Repeating;
        self.interval = interval;
        self
    }

    pub fn once(mut self) -> Self {
        self.mode = ScheduleMode::Once;
        self
    }

    pub fn limited(mut self, limit: usize, interval: Duration) -> Self {
        self.mode = ScheduleMode::Limited(limit);
        self.interval = interval;
        self
    }

    pub fn depends_on(mut self, job_id: Uuid) -> Self {
        self.dependencies.insert(job_id);
        self
    }

    pub fn start_time<T: TimeZone>(mut self, start_time: DateTime<T>) -> Self {
        self.start_time = Some(start_time.to_utc());
        self
    }

    pub fn on_start(
        mut self,
        on_start: impl Fn(Uuid, &SchedulerHandle) + Send + Sync + 'static,
    ) -> Self {
        self.hooks.on_start = Some(arc_mutex_box!(on_start));
        self
    }

    pub fn on_complete(
        mut self,
        on_complete: impl Fn(Uuid, &SchedulerHandle) + Send + Sync + 'static,
    ) -> Self {
        self.hooks.on_complete = Some(arc_mutex_box!(on_complete));
        self
    }

    pub fn on_fail(
        mut self,
        on_fail: impl Fn(Uuid, &SchedulerHandle, Box<dyn Error>) + Send + Sync + 'static,
    ) -> Self {
        self.hooks.on_fail = Some(arc_mutex_box!(on_fail));
        self
    }

    pub fn on_panic(
        mut self,
        on_panic: impl Fn(Uuid, &SchedulerHandle, Box<dyn Any + Send>) + Send + Sync + 'static,
    ) -> Self {
        self.hooks.on_panic = Some(arc_mutex_box!(on_panic));
        self
    }

    pub fn add_condition(
        mut self,
        condition: impl Fn(Uuid, &SchedulerHandle) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.conditions.push(Arc::new(condition));
        self
    }

    pub fn build(
        self,
        task: impl Fn(Uuid, &SchedulerHandle) -> Result<(), Box<dyn Error>> + Send + Sync + 'static,
    ) -> Job {
        let start_time = self.start_time.unwrap_or_else(Utc::now);

        Job {
            id: self.id,
            _tags: self.tags,
            interval: self.interval,
            task: arc_mutex_box!(task),
            last_scheduled: Utc::now(),
            mode: self.mode,
            executions: 0,
            is_running: false,
            start_time,
            hooks: self.hooks,
            dependencies: self.dependencies,
            is_completed: false,
            conditions: self.conditions,
        }
    }
}
