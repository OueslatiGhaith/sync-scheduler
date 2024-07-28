use std::collections::HashSet;

use chrono::{DateTime, Duration, TimeZone, Utc};
use chrono_tz::Tz;
use uuid::Uuid;

use crate::{
    arc_mutex_box,
    job::{Job, JobHooks},
    scheduler::{ScheduleMode, SchedulerHandle},
};

pub struct JobBuilder {
    id: Uuid,
    tags: Vec<String>,
    interval: Duration,
    mode: ScheduleMode,
    start_time: Option<DateTime<Tz>>,
    timezone: Tz,
    dependencies: HashSet<Uuid>,
    hooks: JobHooks,
}

impl Default for JobBuilder {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            tags: vec![],
            interval: Duration::seconds(10),
            mode: ScheduleMode::Once,
            start_time: None,
            timezone: Tz::UTC,
            dependencies: HashSet::new(),
            hooks: JobHooks {
                on_start: None,
                on_complete: None,
                on_fail: None,
                on_schedule: None,
                on_remove: None,
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

    pub fn signleton(mut self, interval: Duration) -> Self {
        self.mode = ScheduleMode::Signleton;
        self.interval = interval;
        self
    }

    pub fn depends_on(mut self, job_id: Uuid) -> Self {
        self.dependencies.insert(job_id);
        self
    }

    pub fn start_time(mut self, start_time: DateTime<Tz>) -> Self {
        self.start_time = Some(start_time);
        self.timezone = start_time.timezone();
        self
    }

    pub fn on_start(mut self, on_start: impl Fn(Uuid) + Send + Sync + 'static) -> Self {
        self.hooks.on_start = Some(arc_mutex_box!(on_start));
        self
    }

    pub fn on_complete(mut self, on_complete: impl Fn(Uuid) + Send + Sync + 'static) -> Self {
        self.hooks.on_complete = Some(arc_mutex_box!(on_complete));
        self
    }

    pub fn on_fail(mut self, on_fail: impl Fn(Uuid, String) + Send + Sync + 'static) -> Self {
        self.hooks.on_fail = Some(arc_mutex_box!(on_fail));
        self
    }

    pub fn on_schedule(mut self, on_schedule: impl Fn(Uuid) + Send + Sync + 'static) -> Self {
        self.hooks.on_schedule = Some(arc_mutex_box!(on_schedule));
        self
    }

    pub fn on_remove(mut self, on_remove: impl Fn(Uuid) + Send + Sync + 'static) -> Self {
        self.hooks.on_remove = Some(arc_mutex_box!(on_remove));
        self
    }

    pub fn build(self, task: impl Fn(Uuid, &SchedulerHandle) + Send + Sync + 'static) -> Job {
        let start_time = self
            .start_time
            .unwrap_or_else(|| self.timezone.from_utc_datetime(&Utc::now().naive_utc()));

        Job {
            id: self.id,
            _tags: self.tags,
            interval: self.interval,
            task: arc_mutex_box!(task),
            last_scheduled: self.timezone.from_utc_datetime(&Utc::now().naive_utc()),
            mode: self.mode,
            executions: 0,
            is_running: false,
            start_time,
            hooks: self.hooks,
            dependencies: self.dependencies,
            completed: false,
        }
    }
}
