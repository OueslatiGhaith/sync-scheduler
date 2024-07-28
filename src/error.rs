use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Job with id {0} already exists")]
    JobAlreadyExists(Uuid),
    #[error("Job with id {0} not found")]
    JobNotFound(Uuid),
}
