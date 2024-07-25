#[macro_export]
macro_rules! arc_mutex_box {
    ($value:expr) => {
        std::sync::Arc::new(std::sync::Mutex::new(Box::new($value)))
    };
}

#[macro_export]
macro_rules! arc_mutex {
    ($value:expr) => {
        std::sync::Arc::new(std::sync::Mutex::new($value))
    };
}

#[macro_export]
macro_rules! arc_rwlock {
    ($value:expr) => {
        std::sync::Arc::new(std::sync::RwLock::new($value))
    };
}

#[macro_export]
macro_rules! trigger_job_option {
    ($job:ident, $hook:ident, ($($args:expr),*)) => {
        $job.hooks
            .$hook
            .as_ref()
            .map(|h| h.lock().unwrap()($($args),*))
    };
}
